import os
import subprocess
import sys
import threading
import unittest
from pathlib import Path
from queue import Queue
from tempfile import TemporaryDirectory

from data_dir_lock import LOCK_FILENAME


_CLI_DRIVER = """
import sys
import threading
from contextlib import nullcontext
from unittest.mock import Mock

from prompt_toolkit.application import create_app_session
from prompt_toolkit.output.plain_text import PlainTextOutput

import history
import main

mode = sys.argv.pop(1)
main._interactive_console = lambda _: nullcontext(Mock(prompt=lambda _: "/quit"))
main.get_local_ip = lambda: "127.0.0.1"

if mode == "interrupt":
    main._interactive_console = lambda _: nullcontext(Mock(prompt=Mock(side_effect=KeyboardInterrupt)))

if mode == "reject":
    main.load_history = Mock(side_effect=AssertionError("history read before lock"))
    main.get_local_ip = Mock(side_effect=AssertionError("network initialized before lock"))
elif mode == "exception":
    main.load_history = Mock(side_effect=RuntimeError("simulated startup exception"))

def start_tcp(**_):
    if mode == "hold":
        print("OWNER_READY", flush=True)
        input()
    if mode not in ("interrupt", "writer"):
        raise OSError("simulated startup failure")
    threads = [threading.Thread(target=main.state.stop_event.wait) for _ in range(2)]
    for thread in threads:
        thread.start()
    return threads

main.start_tcp_threads = start_tcp
main.start_udp_threads = lambda **_: ()
if mode == "writer":
    started, release = threading.Event(), threading.Event()
    def save_history():
        started.set()
        return release.wait(10)
    history.save_history = save_history
    def start_autosave():
        worker, finish = history.start_autosave_thread()
        return worker, lambda _: finish(0)
    main.start_autosave_thread = start_autosave

with create_app_session(output=PlainTextOutput(sys.stdout)):
    main.main()
if mode == "writer":
    assert started.wait(3), "final save did not start"
    print("OWNER_READY", flush=True)
    input()
    release.set()
"""


_BIND_FAILURE_DRIVER = """
import errno
import runpy
import socket
import sys
from contextlib import nullcontext
from unittest.mock import Mock, patch

import prompt_toolkit.patch_stdout as prompt_stdout
import udp_handler
import ui_handler
from prompt_toolkit.application import create_app_session
from prompt_toolkit.output.plain_text import PlainTextOutput

udp_probe = Mock()
udp_probe.getsockname.return_value = ("127.0.0.1", 0)
listener = Mock()
listener.bind.side_effect = OSError(errno.EADDRINUSE, "simulated TCP bind failure")
start_udp = Mock(side_effect=AssertionError("UDP started after TCP startup failure"))

def open_socket(family, kind):
    assert family == socket.AF_INET
    return udp_probe if kind == socket.SOCK_DGRAM else listener

with (
    patch.object(sys, "stdin", Mock(spec=["isatty"], isatty=lambda: True)),
    patch.object(sys.stdout, "isatty", return_value=True),
    create_app_session(output=PlainTextOutput(sys.stdout)),
    patch.object(ui_handler, "create_prompt_session"),
    patch.object(prompt_stdout, "patch_stdout", nullcontext),
    patch.object(socket, "socket", side_effect=open_socket),
    patch.object(udp_handler, "start_udp_threads", start_udp),
):
    try:
        runpy.run_module("main", run_name="__main__", alter_sys=True)
    except SystemExit as exit_error:
        if exit_error.code == 1:
            listener.bind.assert_called_once()
            listener.close.assert_called_once_with()
            start_udp.assert_not_called()
        raise
"""


class DataDirectoryLockTest(unittest.TestCase):
    def setUp(self):
        temp = TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.directory = Path(temp.name)
        self.cwd = Path(__file__).resolve().parents[1]
        self.env = {**os.environ, "PYTHONIOENCODING": "utf-8"}

    def command(self, mode, folder=None):
        return [
            sys.executable, "-B", "-u", "-c", _CLI_DRIVER, mode,
            "--name", "QA", "--port", "5001", "--data-dir", str(folder or self.directory),
        ]

    def run_cli(self, mode, folder=None):
        return subprocess.run(
            self.command(mode, folder), cwd=self.cwd, env=self.env,
            capture_output=True, encoding="utf-8", timeout=6,
        )

    def start_owner(self, mode="hold"):
        owner = subprocess.Popen(
            self.command(mode), cwd=self.cwd, env=self.env,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, encoding="utf-8",
        )

        def close_owner():
            if owner.poll() is None:
                owner.kill()
            owner.wait(timeout=5)
            owner.stdin.close()
            owner.stdout.close()

        self.addCleanup(close_owner)
        output = Queue()

        def read_ready():
            lines = []
            for line in owner.stdout:
                if line.strip() == "OWNER_READY":
                    output.put((True, ""))
                    return
                lines.append(line)
            output.put((False, "".join(lines)))

        threading.Thread(target=read_ready, daemon=True).start()
        ready, diagnostic = output.get(timeout=6)
        self.assertTrue(ready, diagnostic)
        return owner

    def assert_locked(self, result):
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertIn("Hãy chọn --data-dir riêng", result.stderr)

    def assert_startup_failure_reported(self, folder=None):
        result = self.run_cli("startup-failure", folder)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("simulated startup failure", result.stdout)

    def test_tcp_bind_failure_has_nonzero_exit_and_releases_data_directory(self):
        command = [
            sys.executable, "-B", "-u", "-c", _BIND_FAILURE_DRIVER,
            "--name", "QA", "--port", "5001", "--data-dir", str(self.directory),
        ]
        result = subprocess.run(
            command, cwd=self.cwd, env=self.env, capture_output=True, encoding="utf-8", timeout=6,
        )
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("Không thể mở TCP port 5001", result.stdout)
        self.assertIn("simulated TCP bind failure", result.stdout)
        self.assertEqual(result.stderr, "")
        self.assert_startup_failure_reported()

    def test_ctrl_c_at_prompt_has_zero_exit(self):
        result = self.run_cli("interrupt")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("Ctrl+C", result.stdout)
        self.assert_startup_failure_reported()

    def test_rejects_resolved_alias_before_touching_history_or_network(self):
        history_file = self.directory / "chat_history.json"
        history_file.write_bytes(b'{"peer":["keep me"]}')
        downloads = self.directory / "downloads"
        downloads.mkdir()
        kept_file = downloads / "keep.bin"
        kept_file.write_bytes(b"data")
        owner = self.start_owner()
        self.assert_locked(self.run_cli("reject", downloads / ".."))
        self.assertEqual(history_file.read_bytes(), b'{"peer":["keep me"]}')
        self.assertEqual(kept_file.read_bytes(), b"data")
        other = self.directory / "other"
        self.assert_startup_failure_reported(other)
        self.assertTrue((other / LOCK_FILENAME).is_file())
        owner.communicate("\n", timeout=6)
        self.assertEqual(owner.returncode, 1)
        self.assert_startup_failure_reported()
        self.assertTrue((self.directory / LOCK_FILENAME).is_file())

    def test_os_releases_lock_after_forced_termination(self):
        owner = self.start_owner()
        self.assert_locked(self.run_cli("reject"))
        owner.kill()
        owner.wait(timeout=5)
        self.assert_startup_failure_reported()

    def test_os_releases_lock_after_unexpected_startup_exception(self):
        result = self.run_cli("exception")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("simulated startup exception", result.stderr)
        self.assert_startup_failure_reported()

    def test_data_directory_is_still_owned_after_bounded_shutdown(self):
        owner = self.start_owner("writer")
        self.assert_locked(self.run_cli("reject"))
        owner.communicate("\n", timeout=6)
        self.assertEqual(owner.returncode, 0)
        self.assert_startup_failure_reported()

    def test_clear_error_when_lock_cannot_be_opened(self):
        (self.directory / LOCK_FILENAME).mkdir()
        result = self.run_cli("reject")
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertIn("Không thể khóa --data-dir", result.stderr)
