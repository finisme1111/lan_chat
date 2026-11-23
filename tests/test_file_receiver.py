import socket
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import Mock, patch

import tcp_handler as tcp
from models import SharedState


class FileReceiverTest(unittest.TestCase):
    def test_concurrent_receives_never_overwrite_each_other(self):
        barrier = threading.Barrier(2)
        first = Mock(spec=socket.socket)
        second = Mock(spec=socket.socket)

        def receive(data):
            def read(_max_bytes):
                barrier.wait(timeout=2)
                return data
            return read

        first.recv.side_effect = receive(b"first")
        second.recv.side_effect = receive(b"second")

        with TemporaryDirectory() as directory, patch.object(tcp, "DOWNLOADS_DIR", directory):
            existing = Path(directory, "data.bin")
            existing.write_bytes(b"existing")
            with ThreadPoolExecutor(max_workers=2) as workers:
                results = [
                    workers.submit(tcp._receive_file_stream, first, "data.bin", 5, "A"),
                    workers.submit(tcp._receive_file_stream, second, "data.bin", 6, "B"),
                ]
                for result in results:
                    result.result(timeout=3)

            self.assertEqual(existing.read_bytes(), b"existing")
            paths = list(Path(directory).iterdir())
            self.assertEqual({path.name for path in paths}, {"data.bin", "data_1.bin", "data_2.bin"})
            self.assertEqual({path.read_bytes() for path in paths}, {b"existing", b"first", b"second"})
        first.sendall.assert_called_once_with(b"FILE_ACK|1|OK|")
        second.sendall.assert_called_once_with(b"FILE_ACK|1|OK|")

    def test_interrupted_receive_leaves_no_partial_file(self):
        conn = Mock(spec=socket.socket)
        conn.recv.side_effect = [b"abc", b""]
        with TemporaryDirectory() as directory, patch.object(tcp, "DOWNLOADS_DIR", directory):
            with self.assertLogs(tcp.logger, level="ERROR"):
                tcp._receive_file_stream(conn, "data.bin", 4, "A")
            self.assertEqual(list(Path(directory).iterdir()), [])
        conn.sendall.assert_called_once_with(b"FILE_ACK|1|ERROR|")
        conn.close.assert_called_once()

    def test_publish_failure_does_not_acknowledge_success(self):
        conn = Mock(spec=socket.socket)
        with (
            TemporaryDirectory() as directory,
            patch.object(tcp, "DOWNLOADS_DIR", directory),
            patch.object(tcp.os, "link", side_effect=OSError("unsupported")),
            self.assertLogs(tcp.logger, level="ERROR"),
        ):
            tcp._receive_file_stream(conn, "empty.bin", 0, "A")
            self.assertEqual(list(Path(directory).iterdir()), [])
        conn.sendall.assert_called_once_with(b"FILE_ACK|1|ERROR|")

    def test_oversized_file_is_rejected_before_reading_body(self):
        conn = Mock(spec=socket.socket)
        with (
            TemporaryDirectory() as directory,
            patch.object(tcp, "DOWNLOADS_DIR", directory),
            self.assertLogs(tcp.logger, level="ERROR"),
        ):
            tcp._receive_file_stream(conn, "large.bin", tcp.MAX_FILE_SIZE + 1, "A")
            self.assertEqual(list(Path(directory).iterdir()), [])
        conn.recv.assert_not_called()
        conn.sendall.assert_called_once_with(b"FILE_ACK|1|ERROR|")

    def test_quota_counts_existing_files_and_preserves_them(self):
        conn = Mock(spec=socket.socket)
        with (
            TemporaryDirectory() as directory,
            patch.object(tcp, "DOWNLOADS_DIR", directory),
            patch.object(tcp.state, "download_quota_bytes", 8),
            self.assertLogs(tcp.logger, level="ERROR") as logs,
        ):
            existing = Path(directory, "existing.bin")
            existing.write_bytes(b"123456")
            tcp._receive_file_stream(conn, "extra.bin", 3, "A")
            self.assertEqual(existing.read_bytes(), b"123456")
            self.assertEqual(list(Path(directory).iterdir()), [existing])
        self.assertIn("vượt quota", logs.output[0])
        conn.recv.assert_not_called()
        conn.sendall.assert_called_once_with(b"FILE_ACK|1|ERROR|")

    def test_overlapping_receives_reserve_capacity_until_cleanup(self):
        first = Mock(spec=socket.socket)
        refused = Mock(spec=socket.socket)
        retry = Mock(spec=socket.socket)
        receiving = threading.Event()
        abort = threading.Event()

        def wait_for_abort(_size):
            receiving.set()
            abort.wait(timeout=3)
            return b""

        first.recv.side_effect = wait_for_abort
        retry.recv.return_value = b"abc"
        with (
            TemporaryDirectory() as directory,
            patch.object(tcp, "DOWNLOADS_DIR", directory),
            patch.object(tcp.state, "download_quota_bytes", 10),
            self.assertLogs(tcp.logger, level="ERROR"),
            ThreadPoolExecutor(max_workers=1) as worker,
        ):
            active = worker.submit(tcp._receive_file_stream, first, "stalled.bin", 8, "A")
            try:
                self.assertTrue(receiving.wait(1))
                tcp._receive_file_stream(refused, "extra.bin", 3, "B")
                refused.recv.assert_not_called()
                refused.sendall.assert_called_once_with(b"FILE_ACK|1|ERROR|")
            finally:
                abort.set()
            active.result(timeout=2)
            tcp._receive_file_stream(retry, "extra.bin", 3, "B")
            self.assertEqual(Path(directory, "extra.bin").read_bytes(), b"abc")
            self.assertNotIn(directory, tcp._download_reservations)
        retry.sendall.assert_called_once_with(b"FILE_ACK|1|OK|")

    def test_insufficient_disk_space_is_rejected_before_temp_creation(self):
        conn = Mock(spec=socket.socket)
        with (
            TemporaryDirectory() as directory,
            patch.object(tcp, "DOWNLOADS_DIR", directory),
            patch.object(tcp.shutil, "disk_usage", return_value=SimpleNamespace(free=2)),
            self.assertLogs(tcp.logger, level="ERROR") as logs,
        ):
            tcp._receive_file_stream(conn, "large.bin", 3, "A")
            self.assertEqual(list(Path(directory).iterdir()), [])
        self.assertIn("Không đủ dung lượng trống", logs.output[0])
        conn.recv.assert_not_called()
        conn.sendall.assert_called_once_with(b"FILE_ACK|1|ERROR|")

    def test_shutdown_before_publish_discards_only_owned_part(self):
        state = SharedState()
        conn = Mock(spec=socket.socket)

        def last_chunk(_size):
            state.stop_event.set()
            return b"abc"

        conn.recv.side_effect = last_chunk
        with (
            TemporaryDirectory() as directory,
            patch.object(tcp, "state", state),
            patch.object(tcp, "DOWNLOADS_DIR", directory),
        ):
            existing = Path(directory, "keep.bin")
            existing.write_bytes(b"keep")
            tcp._receive_file_stream(conn, "new.bin", 3, "A")
            self.assertEqual(list(Path(directory).iterdir()), [existing])
            self.assertEqual(existing.read_bytes(), b"keep")
            self.assertNotIn(directory, tcp._download_reservations)
        conn.sendall.assert_called_once_with(b"FILE_ACK|1|ERROR|")
