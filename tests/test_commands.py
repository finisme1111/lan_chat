import threading
import unittest
from contextlib import ExitStack
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

import main
import tcp_handler as tcp
import ui_handler as ui
from models import ChatConnection, SharedState


class CommandsTest(unittest.TestCase):
    def setUp(self):
        self.state = SharedState()
        self.state.upsert_peer("B", "127.0.0.1", 5002, "available")
        self.peer_key = "127.0.0.1:5002"
        stack = ExitStack()
        self.addCleanup(stack.close)
        stack.enter_context(patch.object(main, "state", self.state))
        for name in ("print_chat", "print_info", "print_success", "display_chat_history", "display_peer_list"):
            stack.enter_context(patch.object(main, name))
        self.warning = stack.enter_context(patch.object(main, "print_warning"))

    def test_chat_number_selects_the_peer_from_last_list(self):
        self.state.upsert_peer("C", "127.0.0.1", 5003, "available")
        main.handle_command("/list")
        self.state.peer_list.pop(self.peer_key)
        conn = Mock()
        self.state.connections["127.0.0.1:5003"] = ChatConnection(conn, outgoing=False)
        with patch.object(main, "get_or_connect", return_value=conn) as connect:
            main.handle_command("/chat 1")
            connect.assert_not_called()
            main.handle_command("/chat 2")
            connect.assert_called_once_with("127.0.0.1:5003")
        self.assertEqual(self.state.current_partner, "127.0.0.1:5003")

    def test_chat_by_name_is_case_insensitive(self):
        conn = Mock()
        self.state.connections[self.peer_key] = ChatConnection(conn, outgoing=False)
        with patch.object(main, "get_or_connect", return_value=conn) as connect:
            main.handle_command("/chat b")
            connect.assert_called_once_with(self.peer_key)
        self.assertEqual(self.state.current_partner, self.peer_key)

    def test_status_online_preserves_wire_value(self):
        main.handle_command("/status away")
        self.assertEqual(self.state.my_status, "away")
        main.handle_command("/status online")
        self.assertEqual(self.state.my_status, "available")

    def test_leave_cancels_retry_without_closing_chat_socket(self):
        self.state.reconnect_partner = self.peer_key
        conn = Mock()
        self.state.connections[self.peer_key] = ChatConnection(conn, outgoing=False)
        main.handle_command("/leave")
        self.assertIsNone(self.state.reconnect_partner)
        conn.close.assert_not_called()

    def test_only_explicit_commands_send_files(self):
        self.state.current_partner = self.peer_key
        with TemporaryDirectory() as directory:
            source = Path(directory, "hello world.txt")
            source.write_text("abc", encoding="utf-8")
            with (
                patch.object(main, "send_chat_message", return_value=True) as send_text,
                patch.object(main, "start_file_send", return_value=True) as start_file,
            ):
                main.handle_command(" README.md ")
                send_text.assert_called_once_with(self.peer_key, " README.md ")
                start_file.assert_not_called()
                main.handle_command(f"/send {source}.missing")
                self.warning.assert_called_once()
                self.assertEqual(send_text.call_count, 1)
                start_file.assert_not_called()
                main.handle_command(f'/send "{source}"')
                self.assertEqual(start_file.call_args.args[:2], (self.peer_key, str(source)))
                main.handle_command(f'/file "{source}"')
                self.assertEqual(start_file.call_count, 2)

    def test_all_unread_messages_are_shown_before_reset(self):
        messages = [f"tin {i}" for i in range(25)]
        self.state.chat_history[self.peer_key] = messages
        self.state.unread_counts[self.peer_key] = len(messages)
        with patch.object(ui, "state", self.state), patch.object(ui, "_print_fragments") as output:
            ui.display_chat_history(self.peer_key)
        self.assertEqual(self.state.unread_counts[self.peer_key], 0)
        shown = [call.args[0][1] for call in output.call_args_list[1:-1]]
        self.assertEqual(shown, messages)

    def test_outgoing_history_does_not_hide_older_unread_message(self):
        conn = Mock()
        conn.recv.return_value = b"unread"
        with patch.object(tcp, "state", self.state), patch.object(tcp, "_recv_field", side_effect=["MSG", "6", "BYE"]):
            tcp._receiver_loop(conn, self.peer_key, "B")
        self.state.chat_history[self.peer_key].extend(["outgoing"] * 30)
        with patch.object(ui, "state", self.state), patch.object(ui, "_print_fragments") as output:
            ui.display_chat_history(self.peer_key)
        shown = [call.args[0][1] for call in output.call_args_list[1:-1]]
        self.assertEqual(len(shown), 31)
        self.assertTrue(shown[0].endswith("B: unread"))
        self.assertEqual(self.state.unread_counts[self.peer_key], 0)

    def test_commands_remain_responsive_while_chat_connects(self):
        connecting = threading.Event()
        connected = threading.Event()
        sent = threading.Event()
        conn = Mock()

        def connect(key):
            connecting.set()
            if not connected.wait(2):
                return None
            self.state.connections[key] = ChatConnection(conn, outgoing=True)
            return conn

        def send(_key, _message):
            sent.set()
            return True

        with (
            patch.object(main, "get_or_connect", side_effect=connect),
            patch.object(main, "send_chat_message", side_effect=send) as send_text,
            patch.object(main, "print_info"),
        ):
            worker, submit = main._start_chat_worker()
            try:
                main.handle_command("/chat B", submit)
                self.assertTrue(connecting.wait(1))
                main.handle_command("tin nhắn kế tiếp", submit)
                main.handle_command("/status away", submit)
                self.assertEqual(self.state.my_status, "away")
                send_text.assert_not_called()
                connected.set()
                self.assertTrue(sent.wait(1))
                send_text.assert_called_once_with(self.peer_key, "tin nhắn kế tiếp")
                self.assertTrue(main.handle_command("/quit", submit))
            finally:
                self.state.stop_event.set()
                connected.set()
                worker.join(timeout=2)
            self.assertFalse(worker.is_alive())

    def test_failed_pending_selection_does_not_send_to_previous_peer(self):
        connecting = threading.Event()
        let_connect_fail = threading.Event()
        skipped = threading.Event()
        self.state.current_partner = "127.0.0.1:5003"

        def fail_connect(_key):
            connecting.set()
            let_connect_fail.wait(timeout=2)
            return None

        self.warning.side_effect = lambda _message: skipped.set()
        with (
            patch.object(main, "get_or_connect", side_effect=fail_connect),
            patch.object(main, "send_chat_message") as send_text,
            patch.object(main, "print_error"),
        ):
            worker, submit = main._start_chat_worker()
            try:
                main.handle_command("/chat B", submit)
                self.assertTrue(connecting.wait(1))
                main.handle_command("tin nhắn dành cho B", submit)
                let_connect_fail.set()
                self.assertTrue(skipped.wait(1))
                send_text.assert_not_called()
                self.assertEqual(self.state.current_partner, "127.0.0.1:5003")
            finally:
                self.state.stop_event.set()
                let_connect_fail.set()
                worker.join(timeout=2)

    def test_slow_broadcast_does_not_hold_direct_chat_queue(self):
        second_key = "127.0.0.1:5003"
        self.state.upsert_peer("C", "127.0.0.1", 5003, "available")
        self.state.current_partner = second_key
        broadcasting = threading.Event()
        release = threading.Event()
        direct_sent = threading.Event()

        def send(key, message):
            if (key, message) == (self.peer_key, "[Broadcast] thông báo"):
                broadcasting.set()
                release.wait(timeout=2)
            elif (key, message) == (second_key, "chat riêng"):
                direct_sent.set()
            return True

        with patch.object(main, "send_chat_message", side_effect=send):
            chat, submit_chat = main._start_chat_worker()
            broadcast, submit_broadcast = main._start_command_worker("Test-Broadcast", 8)
            try:
                main.handle_command("/broadcast thông báo", submit_chat, submit_broadcast)
                self.assertTrue(broadcasting.wait(1))
                main.handle_command("chat riêng", submit_chat, submit_broadcast)
                self.assertTrue(direct_sent.wait(1))
            finally:
                self.state.stop_event.set()
                release.set()
                chat.join(timeout=2)
                broadcast.join(timeout=2)
        self.assertFalse(chat.is_alive())
        self.assertFalse(broadcast.is_alive())

    def test_shutdown_joins_workers_before_requesting_final_history(self):
        worker = Mock()
        worker.is_alive.return_value = False

        def finish(timeout):
            self.assertTrue(self.state.stop_event.is_set())
            worker.join.assert_called_once()
            self.assertEqual(timeout, 2.0)
            return None

        with (
            patch.object(main, "close_chat_connections"),
            patch.object(main, "stop_file_sender", return_value=True),
            patch.object(main, "drain_tcp_workers", return_value=True),
        ):
            main._shutdown_workers([worker], finish)
        self.warning.assert_called_once_with("Hết thời gian chờ lưu lịch sử chat.")

    def test_oversized_messages_do_not_enter_either_queue(self):
        chat, broadcast = Mock(), Mock()
        with patch.object(main, "MAX_MSG_LEN", 16):
            main.handle_command("ê" * 9, chat, broadcast)
            main.handle_command("/broadcast abcde", chat, broadcast)
        chat.assert_not_called()
        broadcast.assert_not_called()
        self.assertEqual(self.warning.call_count, 2)

    def test_connect_registers_static_peer_and_opens_chat(self):
        conn = Mock()

        def connect(key):
            self.state.connections[key] = ChatConnection(conn, outgoing=True)
            return conn

        with patch.object(main, "get_or_connect", side_effect=connect) as dial:
            main.handle_command("/connect 100.86.10.2:5002 LinuxBox")
            dial.assert_called_once_with("100.86.10.2:5002")
        peer = self.state.peer_list["100.86.10.2:5002"]
        self.assertTrue(peer.is_static)
        self.assertEqual(peer.name, "LinuxBox")
        self.assertEqual(self.state.current_partner, "100.86.10.2:5002")

    def test_peer_add_and_remove_manage_static_peer(self):
        main.handle_command("/peer add 10.232.1.10 5005 Ubuntu")
        self.assertIn("10.232.1.10:5005", self.state.peer_list)
        conn = Mock()
        self.state.connections["10.232.1.10:5005"] = ChatConnection(conn, outgoing=True)
        with patch.object(main, "close_peer_connection") as close_conn:
            main.handle_command("/peer remove Ubuntu")
            close_conn.assert_called_once_with("10.232.1.10:5005")
        self.assertNotIn("10.232.1.10:5005", self.state.peer_list)

    def test_load_peers_file_supports_text_and_json(self):
        with TemporaryDirectory() as directory:
            txt = Path(directory, "peers.txt")
            txt.write_text("# comment\n100.86.1.2:5002 Linux # inline\n\n10.0.0.3 5003\n", encoding="utf-8")
            self.assertEqual(main._load_peers_file(txt), ["100.86.1.2:5002 Linux", "10.0.0.3 5003"])

            js = Path(directory, "peers.json")
            js.write_text('[{"host": "100.86.1.2", "port": 5002, "name": "Linux"}]', encoding="utf-8")
            self.assertEqual(main._load_peers_file(js), ["100.86.1.2:5002 Linux"])
