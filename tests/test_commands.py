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
            worker, submit, _ = main._start_chat_worker()
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
            worker, submit, _ = main._start_chat_worker()
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
            chat, submit_chat, _ = main._start_chat_worker()
            payload_budget = len(f"{main.BROADCAST_PREFIX}thông báo".encode("utf-8"))
            broadcast, submit_broadcast, _ = main._start_command_worker("Test-Broadcast", 8, payload_budget)
            try:
                main.handle_command("/broadcast thông báo", submit_chat, submit_broadcast)
                self.assertTrue(broadcasting.wait(1))
                main.handle_command("/broadcast a", submit_chat, submit_broadcast)
                self.warning.assert_called_once_with("Hàng đợi broadcast đang đầy. Vui lòng thử lại.")
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
        cancel = Mock()

        def finish(timeout):
            self.assertTrue(self.state.stop_event.is_set())
            cancel.assert_called_once()
            worker.join.assert_called_once()
            self.assertEqual(timeout, 2.0)
            return None

        with (
            patch.object(main, "close_chat_connections"),
            patch.object(main, "stop_file_sender", return_value=True),
            patch.object(main, "drain_tcp_workers", return_value=True),
        ):
            main._shutdown_workers([worker], finish, [cancel])
        self.warning.assert_called_once_with("Hết thời gian chờ lưu lịch sử chat.")

    def test_oversized_messages_do_not_enter_either_queue(self):
        chat, broadcast = Mock(), Mock()
        with patch.object(main, "MAX_MSG_LEN", 16):
            main.handle_command("ê" * 9, chat, broadcast)
            main.handle_command("/broadcast abcde", chat, broadcast)
        chat.assert_not_called()
        broadcast.assert_not_called()
        self.assertEqual(self.warning.call_count, 2)

    def test_queue_accounts_for_utf8_payload_at_the_message_limit(self):
        chat, broadcast = Mock(return_value=True), Mock(return_value=True)
        chat_text = "  " + "🙂" * 3 + "á"
        broadcast_text = "éé"
        with patch.object(main, "MAX_MSG_LEN", 16):
            main.handle_command(chat_text, chat, broadcast)
            main.handle_command(f"/broadcast {broadcast_text}", chat, broadcast)
        self.assertEqual(chat.call_args.args[2], len(chat_text.encode("utf-8")))
        self.assertEqual(broadcast.call_args.args[1], len((main.BROADCAST_PREFIX + broadcast_text).encode("utf-8")))
        self.warning.assert_not_called()

    def test_broadcast_reports_partial_delivery_on_shutdown(self):
        self.state.upsert_peer("C", "127.0.0.1", 5003, "available")

        def send(_key, _message):
            self.state.stop_event.set()
            return True

        with patch.object(main, "send_chat_message", side_effect=send) as send_text:
            main.handle_command("/broadcast thông báo")
        send_text.assert_called_once_with(self.peer_key, "[Broadcast] thông báo")
        self.warning.assert_called_once_with(
            "Đã dừng broadcast khi đóng ứng dụng. Đã gửi tới 1/2 người dùng."
        )


class QueueBudgetTest(unittest.TestCase):
    def setUp(self):
        self.stop = threading.Event()
        self.commands = main._CommandQueue("Test-Queue", 8, 5, self.stop)

    def test_pending_and_in_flight_share_one_budget_and_keep_order(self):
        first_started, second_started, third_completed = (threading.Event() for _ in range(3))
        finish_first, finish_second = threading.Event(), threading.Event()
        order = []

        def first():
            order.append(1)
            first_started.set()
            finish_first.wait(3)

        def second():
            order.append(2)
            second_started.set()
            finish_second.wait(3)

        def third():
            order.append(3)
            third_completed.set()

        self.assertTrue(self.commands.enqueue(first, 2))
        worker = threading.Thread(target=self.commands.run, daemon=True)
        worker.start()
        try:
            self.assertTrue(first_started.wait(1))
            self.assertTrue(self.commands.enqueue(second, 3))
            self.assertFalse(self.commands.enqueue(third, 1))
            finish_first.set()
            self.assertTrue(second_started.wait(1))
            self.assertTrue(self.commands.enqueue(third, 2))
            finish_second.set()
            self.assertTrue(third_completed.wait(1))
            self.assertEqual(order, [1, 2, 3])
        finally:
            self.stop.set()
            finish_first.set()
            finish_second.set()
            worker.join(2)
        self.assertFalse(worker.is_alive())
        self.assertEqual(self.commands._reserved_bytes, 0)

    def test_worker_failure_reports_error_and_releases_reservation(self):
        first_checked, second_completed = threading.Event(), threading.Event()

        def fail():
            raise RuntimeError("test failure")

        self.assertTrue(self.commands.enqueue(fail, 5))
        self.assertTrue(self.commands.enqueue(first_checked.set, 0))
        with self.assertLogs(main.logger, level="ERROR") as logs:
            worker = threading.Thread(target=self.commands.run, daemon=True)
            worker.start()
            try:
                self.assertTrue(first_checked.wait(1))
                self.assertTrue(self.commands.enqueue(second_completed.set, 5))
                self.assertTrue(second_completed.wait(1))
            finally:
                self.stop.set()
                worker.join(2)
        self.assertIn("test failure", logs.output[0])
        self.assertEqual(self.commands._reserved_bytes, 0)

    def test_shutdown_releases_pending_once_while_running_job_keeps_its_reservation(self):
        started, release = threading.Event(), threading.Event()
        pending = Mock()

        def running():
            started.set()
            release.wait(3)

        self.assertTrue(self.commands.enqueue(running, 2))
        self.assertTrue(self.commands.enqueue(pending, 3))
        with patch.object(main, "print_warning") as warn:
            worker = threading.Thread(target=self.commands.run, daemon=True)
            worker.start()
            try:
                self.assertTrue(started.wait(1))
                self.stop.set()
                self.commands.cancel_pending()
                self.commands.cancel_pending()
                self.assertEqual(self.commands._reserved_bytes, 2)
                self.assertFalse(self.commands.enqueue(pending, 1))
                pending.assert_not_called()
            finally:
                self.stop.set()
                release.set()
                worker.join(2)
        self.assertFalse(worker.is_alive())
        self.assertEqual(self.commands._reserved_bytes, 0)
        warn.assert_called_once_with("Đã hủy 1 lệnh chưa chạy trên Test-Queue khi đóng ứng dụng.")

    def test_zero_payload_commands_still_use_queue_slots(self):
        commands = main._CommandQueue("Test-Queue", 1, 5, self.stop)
        self.assertTrue(commands.enqueue(Mock(), 0))
        self.assertFalse(commands.enqueue(Mock(), 0))
