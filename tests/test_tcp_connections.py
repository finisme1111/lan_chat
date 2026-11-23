import errno
import socket
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock, patch

from models import SharedState
import tcp_handler as tcp


class ChatConnectionsTest(unittest.TestCase):
    def setUp(self):
        self.state = SharedState()
        self.state.my_name = "A"
        self.state.my_tcp_port = 5001
        self.peer_key = self.state.get_peer_key("127.0.0.1", 5002)
        self.state.upsert_peer("B", "127.0.0.1", 5002, "available")
        state_patch = patch.object(tcp, "state", self.state)
        state_patch.start()
        self.addCleanup(state_patch.stop)

    @staticmethod
    def make_socket(local_port, remote_port):
        sock = Mock(spec=socket.socket)
        sock.getsockname.return_value = ("127.0.0.1", local_port)
        sock.getpeername.return_value = ("127.0.0.1", remote_port)
        sock.connect_ex.return_value = 0
        return sock

    def accept_chat(self, conn, remote_port, name="B"):
        with (
            patch.object(tcp, "_recv_field", side_effect=["HELLO", name, str(remote_port), "CHAT"]),
            patch.object(tcp, "_receiver_loop"),
        ):
            tcp._handle_incoming(conn, conn.getpeername())

    def connect_chat(self, peer_key, outgoing):
        with patch.object(tcp.socket, "socket", return_value=outgoing), patch.object(tcp.threading, "Thread"):
            return tcp.get_or_connect(peer_key)

    def test_retired_receiver_does_not_close_replacement(self):
        original = self.make_socket(5001, 61000)
        replacement = self.make_socket(5001, 61001)
        self.accept_chat(original, 5002)
        self.state.current_partner = self.peer_key
        self.accept_chat(replacement, 5002)

        original.recv.return_value = b""
        with patch.object(tcp, "_system_cb") as notify:
            tcp._receiver_loop(original, self.peer_key, "B")
        notify.assert_not_called()

        self.assertIs(tcp.get_or_connect(self.peer_key), replacement)
        replacement.close.assert_not_called()
        self.assertEqual(self.state.current_partner, self.peer_key)

    def test_simultaneous_dials_keep_same_physical_connection(self):
        a_outgoing = self.make_socket(61000, 5002)
        a_incoming = self.make_socket(5001, 62000)
        b_outgoing = self.make_socket(62000, 5001)
        b_incoming = self.make_socket(5002, 61000)

        # Both sides dial before processing the incoming HELLO.
        self.assertIs(self.connect_chat(self.peer_key, a_outgoing), a_outgoing)
        self.accept_chat(a_incoming, 5002)
        b = SharedState()
        b.my_name = "B"
        b.my_tcp_port = 5002
        a_key = b.get_peer_key("127.0.0.1", 5001)
        b.upsert_peer("A", "127.0.0.1", 5001, "available")
        with patch.object(tcp, "state", b):
            self.assertIs(self.connect_chat(a_key, b_outgoing), b_outgoing)
            self.accept_chat(b_incoming, 5001, "A")
            self.assertIs(tcp.get_or_connect(a_key), b_incoming)

        self.assertIs(tcp.get_or_connect(self.peer_key), a_outgoing)
        self.assertEqual(a_outgoing.getsockname(), b_incoming.getpeername())
        a_incoming.close.assert_called_once()
        b_outgoing.close.assert_called_once()

    def test_incoming_can_win_before_outgoing_registers(self):
        self.state.my_tcp_port = 5003
        incoming = self.make_socket(5003, 61000)
        outgoing = self.make_socket(62000, 5002)
        outgoing.sendall.side_effect = lambda _data: self.accept_chat(incoming, 5002)

        self.assertIs(self.connect_chat(self.peer_key, outgoing), incoming)
        self.assertIs(tcp.get_or_connect(self.peer_key), incoming)
        outgoing.close.assert_called_once()
        incoming.close.assert_not_called()

    def test_handshake_timeout_is_not_reset_by_partial_reads(self):
        conn = self.make_socket(5001, 61000)
        conn.recv.return_value = b"H"
        with patch.object(tcp.time, "monotonic", side_effect=[0, 0, 4, 10]):
            tcp._handle_incoming(conn, conn.getpeername())
        self.assertEqual([call.args for call in conn.settimeout.call_args_list], [(10.0,), (6.0,)])
        self.assertEqual(conn.recv.call_count, 2)
        conn.close.assert_called_once()

    def test_worker_limit_and_shutdown_release_blocked_worker(self):
        started = threading.Event()
        stopped = threading.Event()
        first = self.make_socket(5001, 61000)
        second = self.make_socket(5001, 61001)

        def wait_for_shutdown(_conn):
            started.set()
            stopped.wait(timeout=4)

        first.shutdown.side_effect = lambda _how: stopped.set()
        with patch.object(tcp, "MAX_INCOMING_WORKERS", 1):
            self.assertTrue(tcp._start_tcp_worker(first, wait_for_shutdown, (), incoming=True))
            try:
                self.assertTrue(started.wait(1))
                self.assertFalse(tcp._start_tcp_worker(second, wait_for_shutdown, (), incoming=True))
            finally:
                self.state.stop_event.set()
                self.assertTrue(tcp.drain_tcp_workers())
        self.assertEqual(self.state.tcp_workers, {})
        first.close.assert_called_once()

    def test_bind_failure_is_reported_to_startup_caller(self):
        listener = self.make_socket(5001, 61000)
        listener.bind.side_effect = OSError("address already in use")
        with patch.object(tcp.socket, "socket", return_value=listener):
            with self.assertRaisesRegex(OSError, "address already in use"):
                tcp.start_tcp_threads()
        listener.close.assert_called_once()

    def test_reconnect_start_failure_rolls_back_tcp_acceptor(self):
        listener = self.make_socket(5001, 61000)
        acceptor, reconnect = Mock(), Mock()
        reconnect.start.side_effect = RuntimeError("cannot start thread")
        with (
            patch.object(tcp.socket, "socket", return_value=listener),
            patch.object(tcp.threading, "Thread", side_effect=[acceptor, reconnect]),
        ):
            with self.assertRaisesRegex(RuntimeError, "cannot start thread"):
                tcp.start_tcp_threads()
        self.assertTrue(self.state.stop_event.is_set())
        listener.close.assert_called_once()
        acceptor.join.assert_called_once()

    def test_shutdown_wakes_pending_dial_waiters_and_prevents_hello(self):
        outgoing = self.make_socket(61000, 5002)
        outgoing.connect_ex.return_value = errno.EINPROGRESS
        connecting = threading.Event()
        release = threading.Event()
        waiting = threading.Event()

        def first_poll(*_args):
            connecting.set()
            release.wait(timeout=3)
            return [], [], []

        with (
            patch.object(tcp.socket, "socket", return_value=outgoing),
            patch.object(tcp.select, "select", side_effect=first_poll),
            ThreadPoolExecutor(max_workers=2) as workers,
        ):
            owner = workers.submit(tcp.get_or_connect, self.peer_key)
            try:
                self.assertTrue(connecting.wait(1))
                ready = self.state.pending_chat_dials[self.peer_key]
                original_wait = ready.wait

                def wait_for_owner(timeout):
                    waiting.set()
                    return original_wait(timeout)

                with patch.object(ready, "wait", side_effect=wait_for_owner):
                    waiter = workers.submit(tcp.get_or_connect, self.peer_key)
                    self.assertTrue(waiting.wait(1))
                    self.state.stop_event.set()
                    tcp.close_chat_connections()
                    self.assertIsNone(waiter.result(timeout=1))
            finally:
                release.set()
            self.assertIsNone(owner.result(timeout=1))
        outgoing.sendall.assert_not_called()
        outgoing.close.assert_called_once()
        self.assertFalse(self.state.dialing_chat_sockets)

    def test_concurrent_outgoing_callers_share_one_dial(self):
        outgoing = self.make_socket(61000, 5002)
        dialing = threading.Event()
        second_waiting = threading.Event()
        release_dial = threading.Event()

        def send_hello(_frame):
            dialing.set()
            release_dial.wait(timeout=3)

        outgoing.sendall.side_effect = send_hello
        with (
            patch.object(tcp.socket, "socket", return_value=outgoing) as new_socket,
            patch.object(tcp, "_start_tcp_worker", return_value=True) as start_receiver,
            ThreadPoolExecutor(max_workers=2) as workers,
        ):
            first = workers.submit(tcp.get_or_connect, self.peer_key)
            try:
                self.assertTrue(dialing.wait(1))
                ready = self.state.pending_chat_dials[self.peer_key]
                original_wait = ready.wait

                def wait_for_first(timeout):
                    second_waiting.set()
                    return original_wait(timeout)

                with patch.object(ready, "wait", side_effect=wait_for_first):
                    second = workers.submit(tcp.get_or_connect, self.peer_key)
                    self.assertTrue(second_waiting.wait(1))
                    release_dial.set()
                    self.assertIs(second.result(timeout=2), outgoing)
            finally:
                release_dial.set()
            self.assertIs(first.result(timeout=2), outgoing)

        new_socket.assert_called_once()
        start_receiver.assert_called_once()
        self.assertFalse(self.state.pending_chat_dials)

    def test_invalid_chat_handshake_is_rejected_before_registering(self):
        for fields in (["HELLO", "B", "70000", "CHAT"], ["HELLO", "B", "5002", "OTHER"]):
            conn = self.make_socket(5001, 61000)
            with (
                patch.object(tcp, "_recv_field", side_effect=fields),
                self.assertLogs(tcp.logger, level="WARNING"),
            ):
                tcp._handle_incoming(conn, conn.getpeername())
            conn.close.assert_called_once()
            self.assertFalse(self.state.connections)

    def test_outbound_message_limit_is_enforced_before_connecting(self):
        with patch.object(tcp, "get_or_connect") as connect:
            with self.assertRaisesRegex(ValueError, "UTF-8"):
                tcp.send_chat_message(self.peer_key, "a" * (tcp.MAX_MSG_LEN + 1))
        connect.assert_not_called()

    def test_partial_writes_share_one_deadline(self):
        conn = self.make_socket(5001, 61000)
        chunks = []

        def send(data):
            chunks.append(bytes(data))
            return 2

        conn.send.side_effect = send
        with patch.object(tcp.time, "monotonic", side_effect=[1.0, 2.0, 4.0]):
            with self.assertRaises(TimeoutError):
                tcp._send_chat_frame(conn, b"abcdef", deadline=4.0)
        self.assertEqual(chunks, [b"abcdef", b"cdef"])
        conn.settimeout.assert_not_called()

    def test_stalled_write_releases_worker_and_leaves_next_connection_usable(self):
        blocked = self.make_socket(5001, 61000)
        ready = self.make_socket(5001, 61001)
        blocked.send.side_effect = [2, BlockingIOError()]
        ready.send.side_effect = len
        next_key = "127.0.0.1:5003"
        self.state.connections[self.peer_key] = tcp.ChatConnection(blocked, outgoing=True)
        self.state.connections[next_key] = tcp.ChatConnection(ready, outgoing=True)
        with self.assertLogs(tcp.logger, level="ERROR"):
            self.assertFalse(tcp.send_chat_message(self.peer_key, "chậm"))

        self.assertNotIn(self.peer_key, self.state.connections)
        blocked.close.assert_called_once()
        self.assertTrue(tcp.send_chat_message(next_key, "tiếp theo"))
        self.assertEqual(bytes(ready.send.call_args.args[0]), tcp.encode_msg("tiếp theo"))
        ready.settimeout.assert_not_called()

    def test_reconnect_restores_selected_peer_while_presence_is_fresh(self):
        original = self.make_socket(5001, 61000)
        replacement = self.make_socket(61001, 5002)
        self.accept_chat(original, 5002)
        self.state.current_partner = self.peer_key
        self.state.reconnect_partner = self.peer_key
        tcp._cleanup_connection(self.peer_key, original)
        self.assertIsNone(self.state.current_partner)

        def connect(key):
            self.state.connections[key] = tcp.ChatConnection(replacement, outgoing=True)
            return replacement

        with patch.object(tcp, "get_or_connect", side_effect=connect) as dial:
            tcp._reconnect_selected_peer()
            dial.assert_called_once_with(self.peer_key)
        self.assertEqual(self.state.current_partner, self.peer_key)

    def test_reconnect_does_not_restore_peer_after_selection_is_cancelled(self):
        self.state.reconnect_partner = self.peer_key
        replacement = self.make_socket(61000, 5002)

        def connect(key):
            self.state.reconnect_partner = None
            self.state.connections[key] = tcp.ChatConnection(replacement, outgoing=True)
            return replacement

        with patch.object(tcp, "get_or_connect", side_effect=connect) as dial:
            tcp._reconnect_selected_peer()
            tcp._reconnect_selected_peer()
            dial.assert_called_once_with(self.peer_key)
        self.assertIsNone(self.state.current_partner)
        replacement.close.assert_not_called()
