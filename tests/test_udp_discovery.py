import socket
import unittest
from unittest.mock import Mock, patch
from uuid import uuid4

from models import SharedState
from protocol import PEER_TIMEOUT, decode_hello, encode_hello
import udp_handler as udp


class DiscoveryTest(unittest.TestCase):
    def test_invalid_utf8_does_not_become_a_valid_peer(self):
        self.assertIsNone(decode_hello(b"HELLO|A\xff|5001|available"))

    def test_peer_expires_even_when_every_datagram_is_ignored(self):
        state = SharedState()
        state.my_name = "A"
        state.my_tcp_port = 5001
        conn = Mock(spec=socket.socket)
        notify = Mock()
        now = 100.0
        packets = iter([b"invalid", b"HELLO|A|5001|available"])

        def receive(_size):
            nonlocal now
            now += PEER_TIMEOUT / 2
            packet = next(packets)
            if now == 100.0 + PEER_TIMEOUT:
                state.stop_event.set()
            return packet, ("127.0.0.1", 5001)

        conn.recvfrom.side_effect = receive
        with patch.object(udp, "state", state), patch.object(udp.time, "monotonic", side_effect=lambda: now):
            state.upsert_peer("B", "127.0.0.1", 5002, "available")
            udp.listener_thread(conn, 5001, notify)

        self.assertEqual(state.peer_list, {})
        notify.assert_called_once_with("[*] B đã offline (hết thời gian chờ).")
        conn.close.assert_called_once()

    def test_bind_failure_is_reported_before_starting_discovery_threads(self):
        conn = Mock(spec=socket.socket)
        conn.bind.side_effect = OSError("address already in use")
        with patch.object(udp.socket, "socket", return_value=conn), patch.object(udp.threading, "Thread") as worker:
            with self.assertRaisesRegex(OSError, "address already in use"):
                udp.start_udp_threads()
        conn.close.assert_called_once()
        worker.assert_not_called()

    def test_broadcaster_start_failure_rolls_back_udp_listener(self):
        listener, broadcaster = Mock(spec=socket.socket), Mock(spec=socket.socket)
        broadcaster.getsockname.return_value = ("0.0.0.0", 63000)
        sender_thread, listener_thread = Mock(), Mock()
        sender_thread.start.side_effect = RuntimeError("cannot start thread")

        with (
            patch.object(udp, "_open_listener", return_value=listener),
            patch.object(udp.socket, "socket", return_value=broadcaster),
            patch.object(udp.threading, "Thread", side_effect=[sender_thread, listener_thread]),
        ):
            with self.assertRaisesRegex(RuntimeError, "cannot start thread"):
                udp.start_udp_threads()
        listener.close.assert_called_once()
        broadcaster.close.assert_called_once()
        listener_thread.join.assert_called_once()

    def test_versioned_announcements_preserve_legacy_wire_format(self):
        instance = uuid4().hex
        self.assertEqual(encode_hello("A", 5001, "away"), b"HELLO|A|5001|away")
        self.assertEqual(decode_hello(encode_hello("A", 5001, "away", instance))["instance_id"], instance)
        self.assertIsNone(decode_hello(b"HELLO2|A|5001|available|bad-id"))

    def test_listener_keeps_remote_peer_with_same_name_and_port(self):
        state = SharedState()
        state.my_name = "A"
        state.my_tcp_port = 5001
        remote = uuid4().hex
        remote2 = uuid4().hex
        packets = iter([
            (encode_hello("A", 5001, "available", state.instance_id), ("127.0.0.1", 61001)),
            (encode_hello("A", 5001, "available"), ("127.0.0.1", 61001)),
            (encode_hello("A", 5001, "away", remote), ("192.0.2.1", 61002)),
            (encode_hello("A", 5001, "away"), ("192.0.2.1", 61002)),
            (encode_hello("A", 5001, "away", remote), ("192.0.2.2", 61002)),
            (encode_hello("A", 5001, "available", remote2), ("192.0.2.3", 61003)),
        ])
        conn = Mock(spec=socket.socket)

        def receive(_size):
            packet = next(packets, None)
            if packet is None:
                state.stop_event.set()
                raise socket.timeout
            return packet

        conn.recvfrom.side_effect = receive
        notify = Mock()
        with patch.object(udp, "state", state):
            udp.listener_thread(conn, 61001, notify)

        self.assertEqual(set(state.peer_list), {"192.0.2.1:5001", "192.0.2.3:5001"})
        self.assertEqual(state.resolve_peer_key("192.0.2.2", 5001), "192.0.2.1:5001")
        self.assertEqual(notify.call_count, 2)

    def test_live_alternate_endpoint_replaces_stale_preferred_endpoint(self):
        state = SharedState()
        instance = uuid4().hex
        key = "192.0.2.1:5001"
        with patch.object(udp.time, "monotonic", return_value=100.0) as clock:
            state.upsert_peer("A", "192.0.2.1", 5001, "available", instance)
            state.upsert_peer("A", "192.0.2.2", 5001, "available", instance)
            state.chat_history[key] = ["tin cũ"]
            clock.return_value = 100.0 + PEER_TIMEOUT
            state.upsert_peer("A", "192.0.2.2", 5001, "available", instance)
            state.remove_timed_out_peers()
        self.assertEqual(state.get_peer_list_snapshot()[key].ip, "192.0.2.2")
        self.assertEqual(state.chat_history[key], ["tin cũ"])

    def test_discovery_alone_does_not_allocate_persistent_chat_state(self):
        state = SharedState()
        with patch.object(udp.time, "monotonic", return_value=100.0) as clock:
            state.upsert_peer("A", "192.0.2.1", 5001, "available", uuid4().hex)
            clock.return_value = 100.0 + PEER_TIMEOUT
            self.assertEqual(state.remove_timed_out_peers(), ["A"])
        self.assertEqual(state.chat_history, {})
        self.assertEqual(state.unread_counts, {})
