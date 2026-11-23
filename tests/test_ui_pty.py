import errno
import os
import select
import subprocess
import sys
import time
import unittest
from pathlib import Path


_DRIVER = """
import threading

from prompt_toolkit.patch_stdout import patch_stdout

import main
from ui_handler import create_prompt_session

draft_ready = threading.Event()
session = create_prompt_session()

def on_cursor_change(buffer):
    if buffer.text == "abcd" and buffer.cursor_position == 2:
        draft_ready.set()

def receive():
    if draft_ready.wait(5):
        main.on_tcp_message("minh", "Minh", "PTY active message", True)
        main.on_tcp_message("an", "An", "PTY unread message", False)

session.default_buffer.on_cursor_position_changed += on_cursor_change
with patch_stdout():
    worker = threading.Thread(target=receive, daemon=True)
    worker.start()
    text = session.prompt("QA_PROMPT> ")
    worker.join(timeout=1)
print("QA_RESULT", text, flush=True)
"""


@unittest.skipUnless(os.name == "posix", "POSIX PTY required")
class TerminalRedrawTest(unittest.TestCase):
    def test_incoming_chat_and_unread_preserve_draft_in_ansi_terminal(self):
        import fcntl
        import pty
        import struct
        import termios

        master, slave = pty.openpty()
        with os.fdopen(master, "rb", buffering=0) as terminal, os.fdopen(slave, "rb", buffering=0) as peer:
            fcntl.ioctl(peer, termios.TIOCSWINSZ, struct.pack("HHHH", 24, 100, 0, 0))
            env = {
                **os.environ, "TERM": "xterm-256color", "PROMPT_TOOLKIT_NO_CPR": "1",
                "PYTHONIOENCODING": "utf-8",
            }
            with subprocess.Popen(
                [sys.executable, "-B", "-c", _DRIVER],
                cwd=Path(__file__).resolve().parents[1], env=env,
                stdin=peer, stdout=peer, stderr=peer,
            ) as process:
                peer.close()
                output = bytearray()

                def expect(text):
                    deadline = time.monotonic() + 4
                    expected = text.encode("utf-8")
                    while expected not in output:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0 or not select.select([terminal], [], [], remaining)[0]:
                            break
                        try:
                            chunk = terminal.read(65536)
                        except OSError as error:
                            if error.errno != errno.EIO:
                                raise
                            break
                        if not chunk:
                            break
                        output.extend(chunk)
                    self.assertIn(expected, output, bytes(output[-2000:]))

                try:
                    expect("QA_PROMPT>")
                    os.write(master, b"abcd\x1b[D\x1b[D")
                    expect("PTY active message")
                    expect("Tin nhắn mới từ An: PTY unread message")
                    os.write(master, b"X\r")
                    expect("QA_RESULT abXcd")
                    self.assertIn(b"\x1b[", output, "Expected ANSI terminal output")
                    self.assertEqual(process.wait(timeout=2), 0)
                finally:
                    if process.poll() is None:
                        process.kill()
