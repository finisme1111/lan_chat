import os
import socket
import threading
import unittest
from contextlib import ExitStack
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, Mock, patch

from models import SharedState
import main
import tcp_handler as tcp


class FileSenderTest(unittest.TestCase):
    def setUp(self):
        self.state = SharedState()
        self.state.my_name = "A"
        self.state.my_tcp_port = 5001
        self.peer_key = self.state.get_peer_key("127.0.0.1", 5002)
        self.state.upsert_peer("B", "127.0.0.1", 5002, "available")
        self.state.current_partner = self.peer_key
        stack = ExitStack()
        self.addCleanup(stack.close)
        directory = stack.enter_context(TemporaryDirectory())
        self.path = Path(directory, "source.bin")
        self.path.write_bytes(b"abc")
        self.sock = MagicMock(spec=socket.socket)
        self.sock.__enter__.return_value = self.sock
        self.sock.connect_ex.return_value = 0
        self.sock.recv.return_value = b""
        stack.enter_context(patch.object(main, "state", self.state))
        stack.enter_context(patch.object(tcp, "state", self.state))
        stack.enter_context(patch.object(tcp, "_file_sender", None))
        stack.enter_context(patch.object(tcp, "_file_sender_socket", None))
        self.socket_factory = stack.enter_context(patch.object(tcp.socket, "socket", return_value=self.sock))
        self.addCleanup(self.stop_sender)

    def stop_sender(self):
        self.state.stop_event.set()
        tcp.stop_file_sender()

    def test_cli_keeps_accepting_commands_while_file_send_waits(self):
        sending = threading.Event()
        unblocked = threading.Event()

        def slow_send(data):
            if data == b"abc":
                sending.set()
                if not unblocked.wait(4):
                    raise TimeoutError
                raise ConnectionResetError

        self.sock.sendall.side_effect = slow_send
        self.sock.shutdown.side_effect = lambda _how: unblocked.set()
        with (
            patch.object(main, "print_info"),
            patch.object(main, "display_peer_list") as list_peers,
            patch.object(main, "print_warning") as warn,
            patch.object(main, "print_error") as report_error,
        ):
            self.assertFalse(main.handle_command(f"/file {self.path}"))
            self.assertTrue(sending.wait(1))
            self.assertFalse(main.handle_command("/list"))
            list_peers.assert_called_once()
            self.assertFalse(main.handle_command(f"/file {self.path}"))
            warn.assert_called_once()
            self.assertTrue(main.handle_command("/quit"))
            tcp.stop_file_sender()
            self.assertFalse(tcp._file_sender.is_alive())
            self.sock.shutdown.assert_called_once_with(socket.SHUT_RDWR)
            report_error.assert_not_called()

    def test_source_growth_does_not_add_bytes_to_frame(self):
        def grow_after_header(data):
            if data.startswith(b"FILE|"):
                with self.path.open("ab") as changed:
                    changed.write(b"more")

        self.sock.sendall.side_effect = grow_after_header
        self.assertIs(tcp.send_file(self.peer_key, str(self.path)), tcp.FileSendResult.UNCONFIRMED)
        self.assertEqual(self.sock.sendall.call_args.args, (b"abc",))
        self.assertEqual(self.sock.settimeout.call_args_list[0].args, (tcp.FILE_IO_TIMEOUT,))

    def test_source_truncation_fails(self):
        def truncate_after_header(data):
            if data.startswith(b"FILE|"):
                self.path.write_bytes(b"a")

        self.sock.sendall.side_effect = truncate_after_header
        progress = Mock()
        with self.assertLogs(tcp.logger, level="ERROR"):
            self.assertIs(tcp.send_file(self.peer_key, str(self.path), progress), tcp.FileSendResult.FAILED)
        progress.assert_called_once_with(1, 3)
        self.sock.__exit__.assert_called_once()

    @unittest.skipUnless(hasattr(os, "mkfifo"), "POSIX FIFO required")
    def test_fifo_source_is_rejected_without_waiting_for_writer(self):
        fifo = self.path.with_name("source.fifo")
        os.mkfifo(fifo)
        results = []
        worker = threading.Thread(
            target=lambda: results.append(tcp.send_file(self.peer_key, str(fifo))), daemon=True,
        )
        with self.assertLogs(tcp.logger, level="ERROR"):
            worker.start()
            try:
                worker.join(timeout=1)
                self.assertFalse(worker.is_alive())
            finally:
                if worker.is_alive():
                    fd = os.open(fifo, os.O_WRONLY | os.O_NONBLOCK)
                    os.close(fd)
                    worker.join(timeout=1)
        self.assertEqual(results, [tcp.FileSendResult.FAILED])
        self.sock.connect_ex.assert_not_called()

    @unittest.skipUnless(os.name == "posix", "POSIX byte filenames required")
    def test_unencodable_filename_fails_before_opening_a_socket(self):
        name = os.fsdecode(b"nguon-\xff.bin")
        if not any(0xD800 <= ord(character) <= 0xDFFF for character in name):
            self.skipTest("Filesystem decoding does not produce a surrogate for this byte fixture")
        path = self.path.with_name(name)
        path.write_bytes(b"abc")
        done, progress = Mock(), Mock()
        with (
            path.open("rb") as source,
            patch.object(tcp, "open", return_value=source, create=True),
            patch.object(threading, "excepthook") as worker_error,
            self.assertLogs(tcp.logger, level="ERROR") as logs,
        ):
            self.assertTrue(tcp.start_file_send(self.peer_key, str(path), progress, done))
            tcp._file_sender.join(timeout=2)
            self.assertFalse(tcp._file_sender.is_alive())
            self.assertTrue(source.closed)

        done.assert_called_once_with(tcp.FileSendResult.FAILED)
        self.assertIn("UTF-8", logs.output[0])
        progress.assert_not_called()
        self.socket_factory.assert_not_called()
        worker_error.assert_not_called()
        self.assertIsNone(tcp._file_sender_socket)

    def test_unicode_filename_is_transmitted_as_utf8(self):
        path = self.path.with_name("tệp tiếng Việt.bin")
        path.write_bytes(b"abc")
        self.sock.recv.side_effect = [bytes([value]) for value in b"FILE_ACK|1|OK|"]

        self.assertIs(tcp.send_file(self.peer_key, str(path)), tcp.FileSendResult.SAVED)
        self.assertEqual(
            [call.args[0] for call in self.sock.sendall.call_args_list],
            [b"HELLO|A|5001|FILE|", "FILE|tệp tiếng Việt.bin|3|".encode("utf-8"), b"abc"],
        )

    def test_only_supported_success_ack_confirms_save(self):
        for response, expected in (
            (b"FILE_ACK|1|OK|", tcp.FileSendResult.SAVED),
            (b"FILE_ACK|1|ERROR|", tcp.FileSendResult.FAILED),
            (b"FILE_ACK|2|OK|", tcp.FileSendResult.UNCONFIRMED),
        ):
            with self.subTest(response=response):
                self.sock.recv.side_effect = [bytes([value]) for value in response]
                self.assertIs(tcp.send_file(self.peer_key, str(self.path)), expected)
