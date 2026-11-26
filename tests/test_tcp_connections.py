import errno
import socket
import threading
import unittest
from collections import deque
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

    def receive_scheduled(self, arrivals):
        """Drive CHAT reads with arrival times on a fake monotonic clock."""
        pending = deque(arrivals)
        now = 0.0
        conn = self.make_socket(5001, 61000)
        self.state.connections[self.peer_key] = tcp.ChatConnection(conn, outgoing=False)

        def recv(size):
            nonlocal now
            arrival, chunk = pending.popleft()
            now = max(now, arrival)
            self.assertLessEqual(len(chunk), size)
            return chunk

        def select_read(readers, writers, errors, timeout):
            nonlocal now
            self.assertEqual((readers, writers, errors), ([conn], [], []))
            if pending and pending[0][0] < now + timeout:
                now = max(now, pending[0][0])
                return readers, [], []
            now += timeout
            return [], [], []

        conn.recv.side_effect = recv
        with (
            patch.object(tcp.time, "monotonic", side_effect=lambda: now),
            patch.object(tcp.select, "select", side_effect=select_read),
            patch.object(tcp, "_msg_cb") as message,
        ):
            tcp._receiver_loop(conn, self.peer_key, "B")
        conn.settimeout.assert_not_called()
        conn.close.assert_called_once()
        self.assertNotIn(self.peer_key, self.state.connections)
        return now, message

    @staticmethod
    def header_at(start, header):
        return [(start, bytes([byte])) for byte in header]

    def assert_receive_preserves_history(self, arrivals, deadline):
        self.state.chat_history[self.peer_key] = ["previous unread message"]
        self.state.unread_counts[self.peer_key] = 1
        self.state.first_unread[self.peer_key] = 0
        elapsed, message = self.receive_scheduled(arrivals)
        self.assertEqual(elapsed, deadline)
        message.assert_not_called()
        self.assertEqual(self.state.chat_history[self.peer_key], ["previous unread message"])
        self.assertEqual(self.state.unread_counts[self.peer_key], 1)
        self.assertEqual(self.state.first_unread[self.peer_key], 0)

    def test_partial_frame_type_expires_from_first_byte_after_idle(self):
        self.assert_receive_preserves_history([(100, b"M"), (111, b"S")], 110)

    def test_partial_length_shares_the_frame_type_deadline(self):
        self.assert_receive_preserves_history([(0, b"M"), *self.header_at(8, b"SG|1"), (11, b"|")], 10)

    def test_payload_inactivity_deadline_resets_on_progress(self):
        arrivals = [*self.header_at(0, b"MSG|2|"), (9, b"a"), (20, b"b")]
        self.assert_receive_preserves_history(arrivals, 19)

    def test_payload_total_deadline_expires_despite_regular_progress(self):
        arrivals = self.header_at(0, b"MSG|7|") + [(t, b"a") for t in range(9, 64, 9)]
        self.assert_receive_preserves_history(arrivals, 60)

    def test_short_select_waits_preserve_chat_frame_deadlines(self):
        cases = (
            ([(0, b"M"), (11, b"S")], 10),
            ([*self.header_at(0, b"MSG|2|"), (9, b"a"), (20, b"b")], 19),
            (self.header_at(0, b"MSG|7|") + [(t, b"a") for t in range(9, 64, 9)], 60),
        )
        with patch.object(tcp, "_CHAT_READ_SELECT_INTERVAL", 0.1):
            for arrivals, deadline in cases:
                with self.subTest(deadline=deadline):
                    self.assert_receive_preserves_history(arrivals, deadline)

    def test_cancel_pending_chat_read_without_socket_readiness(self):
        conn = self.make_socket(5001, 61000)

        def select_without_wakeup(readers, writers, errors, timeout):
            self.assertEqual((readers, writers, errors), ([conn], [], []))
            self.assertEqual(timeout, 0.1)
            self.state.stop_event.set()
            return [], [], []

        with (
            patch.object(tcp, "_CHAT_READ_SELECT_INTERVAL", 0.1),
            patch.object(tcp.time, "monotonic", return_value=0),
            patch.object(tcp.select, "select", side_effect=select_without_wakeup) as wait,
        ):
            with self.assertRaises(ConnectionAbortedError):
                tcp._recv_chat_before(conn, 1, deadline=10)
        wait.assert_called_once()
        conn.recv.assert_not_called()
        conn.settimeout.assert_not_called()

    def test_slow_fragmented_utf8_and_back_to_back_frames_are_received(self):
        now, message = self.receive_scheduled([
            (80, b"M"), *self.header_at(89, b"SG|2|"), (98, b"\xc3"), (107, b"\xa9"),
            *self.header_at(107, b"MSG|1|"), (107, b"x"), *self.header_at(107, b"BYE|"),
        ])
        self.assertEqual(now, 107)
        self.assertEqual([call.args[2] for call in message.call_args_list], ["é", "x"])
        self.assertEqual(self.state.unread_counts[self.peer_key], 2)

    def test_eof_discards_partial_frame_without_waiting_for_deadline(self):
        for header in (b"MS", b"MSG|2|a"):
            with self.subTest(header=header):
                self.assert_receive_preserves_history([*self.header_at(0, header), (2, b"")], 2)

    def test_stalled_reader_releases_inbound_slot_and_allows_reconnect(self):
        waiting, sender = socket.socketpair()
        replacement, replacement_peer = socket.socketpair()
        with (
            waiting, sender, replacement, replacement_peer,
            patch.object(tcp, "CHAT_HEADER_TIMEOUT", 0.1),
            patch.object(tcp, "MAX_INCOMING_WORKERS", 1),
        ):
            self.state.current_partner = self.peer_key
            self.state.reconnect_partner = self.peer_key
            self.state.connections[self.peer_key] = tcp.ChatConnection(waiting, outgoing=False)
            self.assertTrue(tcp._start_tcp_worker(waiting, tcp._receiver_loop, (self.peer_key,), incoming=True))
            worker, _ = self.state.tcp_workers[waiting]
            sender.sendall(b"MSG|")
            worker.join(timeout=3)
            self.assertFalse(worker.is_alive())
            self.assertEqual(waiting.fileno(), -1)
            self.assertNotIn(waiting, self.state.tcp_workers)
            self.assertIsNone(self.state.current_partner)

            def connect(peer_key):
                self.state.connections[peer_key] = tcp.ChatConnection(replacement, outgoing=True)
                return replacement

            with patch.object(tcp, "get_or_connect", side_effect=connect) as dial:
                tcp._reconnect_selected_peer()
            dial.assert_called_once_with(self.peer_key)
            self.assertEqual(self.state.current_partner, self.peer_key)
            self.assertTrue(tcp._start_tcp_worker(replacement, tcp._receiver_loop, (self.peer_key,), incoming=True))
            replacement_peer.sendall(b"BYE|")
            self.assertTrue(tcp.drain_tcp_workers())

    def test_shutdown_wakes_receiver_waiting_for_partial_payload(self):
        conn, sender = socket.socketpair()
        waiting = threading.Event()
        select_read = tcp.select.select

        def ready(readers, writers, errors, timeout):
            result = select_read(readers, writers, errors, 0)
            if not result[0]:
                waiting.set()
                return select_read(readers, writers, errors, timeout)
            return result

        with conn, sender, patch.object(tcp.select, "select", side_effect=ready):
            sender.sendall(b"MSG|2|a")
            self.state.connections[self.peer_key] = tcp.ChatConnection(conn, outgoing=False)
            self.assertTrue(tcp._start_tcp_worker(conn, tcp._receiver_loop, (self.peer_key,), incoming=True))
            try:
                self.assertTrue(waiting.wait(2))
            finally:
                self.state.stop_event.set()
                self.assertTrue(tcp.drain_tcp_workers(timeout=2))
        self.assertFalse(self.state.chat_history.get(self.peer_key))
        self.assertFalse(self.state.unread_counts.get(self.peer_key))
        self.assertFalse(self.state.tcp_workers)

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

    def test_winning_incoming_restores_selection_after_losing_socket_closes(self):
        outgoing = self.make_socket(61000, 5002)
        incoming = self.make_socket(5001, 61001)
        self.connect_chat(self.peer_key, outgoing)
        self.state.current_partner = self.peer_key
        self.state.reconnect_partner = self.peer_key
        tcp._cleanup_connection(self.peer_key, outgoing)
        self.assertIsNone(self.state.current_partner)

        self.accept_chat(incoming, 5002)
        self.assertEqual(self.state.current_partner, self.peer_key)
        self.assertIs(tcp.get_or_connect(self.peer_key), incoming)

    def test_incoming_connection_after_leave_does_not_restore_selection(self):
        incoming = self.make_socket(5001, 61001)
        self.accept_chat(incoming, 5002)
        self.assertIsNone(self.state.current_partner)
        self.assertIs(tcp.get_or_connect(self.peer_key), incoming)
