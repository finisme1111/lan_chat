import asyncio
import io
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import redirect_stdout
from contextvars import copy_context
from unittest.mock import patch

from prompt_toolkit.application import create_app_session
from prompt_toolkit.input.defaults import create_pipe_input
from prompt_toolkit.output import DummyOutput, defaults

import main
import run_auto_test
import ui_handler as ui
from models import SharedState


class UiOutputTest(unittest.TestCase):
    def test_chat_renders_untrusted_text_as_literal_fragments(self):
        with patch.object(ui, "print_formatted_text") as output:
            ui.print_chat("B\x00", "Chào <peer> & bạn\x1b")
        fragments = output.call_args.args[0]
        self.assertEqual(
            list(fragments),
            [("class:peer", "[B\uFFFD]"), ("", " Chào <peer> & bạn\uFFFD")],
        )

    def test_render_failure_does_not_propagate_to_network_callback(self):
        with (
            patch.object(main, "print_chat", side_effect=RuntimeError("output unavailable")),
            self.assertLogs(main.logger, level="ERROR"),
        ):
            main.on_tcp_message("127.0.0.1:5002", "B", "Chào", True)

    def test_integration_peer_output_handles_a_pipe_in_startup_and_worker(self):
        output = io.StringIO()

        def notify_from_worker():
            with create_app_session():
                ui.print_info("Thông báo từ worker")

        with (
            create_app_session(),
            redirect_stdout(output),
            patch.object(defaults, "create_output", side_effect=RuntimeError("console output unavailable")),
            patch.object(ui, "print_formatted_text", ui.print_formatted_text),
        ):
            with self.assertRaisesRegex(RuntimeError, "console output unavailable"):
                ui.print_banner()

            run_auto_test._configure_peer_output()
            ui.print_banner()
            with ThreadPoolExecutor(max_workers=1) as workers:
                workers.submit(notify_from_worker).result(timeout=2)

        self.assertIn("LAN Chat & File Sharing P2P", output.getvalue())
        self.assertIn("Thông báo từ worker", output.getvalue())


class HistoryOutputTest(unittest.TestCase):
    def setUp(self):
        self.state = SharedState()
        self.key = "127.0.0.1:5002"
        self.state.upsert_peer("Minh", "127.0.0.1", 5002, "available")
        state_patch = patch.object(ui, "state", self.state)
        state_patch.start()
        self.addCleanup(state_patch.stop)

    def test_long_history_copies_only_the_window_needed_for_rendering(self):
        class SliceOnlyHistory(list):
            def __iter__(self):
                raise AssertionError("The renderer must not copy the whole history")

        messages = [f"tin {index}" for index in range(5000)]
        self.state.chat_history[self.key] = SliceOnlyHistory(messages)
        for unread_from, unread in ((5000, 0), (4930, 1)):
            with self.subTest(unread_from=unread_from):
                self.state.first_unread[self.key] = unread_from
                self.state.unread_counts[self.key] = unread
                first_shown = min(4980, unread_from)
                with patch.object(ui, "_print_fragments") as output:
                    ui.display_chat_history(self.key)

                shown = [call.args[0][1] for call in output.call_args_list[2:-1]]
                self.assertEqual(shown, messages[first_shown:])
                self.assertIn(f"({5000 - first_shown}/5000)", output.call_args_list[0].args[0][1])
                self.assertIn(f"{first_shown} tin nhắn cũ hơn", output.call_args_list[1].args[0][1])
                self.assertEqual(self.state.unread_counts[self.key], 0)
                self.assertNotIn(self.key, self.state.first_unread)

    def test_messages_arriving_during_render_remain_unread(self):
        self.state.chat_history[self.key] = ["tin cũ", "tin chưa đọc"]
        self.state.unread_counts[self.key] = 1
        self.state.first_unread[self.key] = 1
        rendering = threading.Event()
        resume = threading.Event()
        rendered = []

        def output(fragment):
            if not rendering.is_set():
                rendering.set()
                if not resume.wait(3):
                    raise AssertionError("History rendering was not resumed")
            rendered.append(fragment[1])

        with (
            patch.object(ui, "_print_fragments", side_effect=output),
            ThreadPoolExecutor(max_workers=1) as workers,
        ):
            result = workers.submit(ui.display_chat_history, self.key)
            try:
                self.assertTrue(rendering.wait(1))
                self.assertTrue(self.state.lock.acquire(timeout=1), "The UI held the state lock while rendering")
                try:
                    self.state.chat_history[self.key].append("tin đến trong lúc render")
                    self.state.unread_counts[self.key] += 1
                finally:
                    self.state.lock.release()
            finally:
                resume.set()
            result.result(timeout=1)

        self.assertIn("tin chưa đọc", rendered)
        self.assertNotIn("tin đến trong lúc render", rendered)
        self.assertEqual(self.state.chat_history[self.key][-1], "tin đến trong lúc render")
        self.assertEqual(self.state.unread_counts[self.key], 1)
        self.assertEqual(self.state.first_unread[self.key], 2)


class PromptRedrawTest(unittest.IsolatedAsyncioTestCase):
    async def test_background_output_preserves_draft_and_cursor(self):
        rendered = asyncio.Event()
        draft_ready = asyncio.Event()

        class RecordingOutput(DummyOutput):
            def write(self, data):
                if "tin mới" in data:
                    rendered.set()

        def on_cursor_change(buffer):
            if buffer.text == "abcd" and buffer.cursor_position == 2:
                draft_ready.set()

        with create_pipe_input() as terminal:
            with create_app_session(input=terminal, output=RecordingOutput()):
                session = ui.create_prompt_session()
                session.default_buffer.on_cursor_position_changed += on_cursor_change
                prompt = asyncio.create_task(session.prompt_async(ui.get_prompt_text))
                try:
                    terminal.send_text("abcd\x1b[D\x1b[D")
                    await asyncio.wait_for(draft_ready.wait(), 2)
                    worker = threading.Thread(target=copy_context().run, args=(ui.print_chat, "B", "tin mới"))
                    worker.start()
                    worker.join(timeout=1)
                    self.assertFalse(worker.is_alive())
                    await asyncio.wait_for(rendered.wait(), 2)
                    self.assertEqual(session.default_buffer.text, "abcd")
                    self.assertEqual(session.default_buffer.cursor_position, 2)
                    terminal.send_text("X\n")
                    self.assertEqual(await asyncio.wait_for(prompt, 2), "abXcd")
                finally:
                    if not prompt.done():
                        prompt.cancel()
                        await asyncio.gather(prompt, return_exceptions=True)
