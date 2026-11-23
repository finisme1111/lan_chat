import asyncio
import threading
import unittest
from contextvars import copy_context
from unittest.mock import patch

from prompt_toolkit.application import create_app_session
from prompt_toolkit.input.defaults import create_pipe_input
from prompt_toolkit.output import DummyOutput

import main
import ui_handler as ui


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
