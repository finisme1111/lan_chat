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

import history
import main

mode = sys.argv.pop(1)
main.patch_stdout = nullcontext
main.create_prompt_session = lambda: Mock(prompt=lambda _: "/quit")
main.get_local_ip = lambda: "127.0.0.1"

if mode == "reject":
    main.load_history = Mock(side_effect=AssertionError("history read before lock"))
    main.get_local_ip = Mock(side_effect=AssertionError("network initialized before lock"))
elif mode == "exception":
    main.load_history = Mock(side_effect=RuntimeError("simulated startup exception"))

def start_tcp(**_):
    if mode == "hold":
        print("OWNER_READY", flush=True)
        input()
    if mode != "writer":
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

main.main()
if mode == "writer":
    assert started.wait(3), "final save did not start"
    print("OWNER_READY", flush=True)
    input()
    release.set()
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
        self.assertEqual(self.run_cli("startup-failure", other).returncode, 0)
        self.assertTrue((other / LOCK_FILENAME).is_file())
        owner.communicate("\n", timeout=6)
        self.assertEqual(owner.returncode, 0)
        self.assertEqual(self.run_cli("startup-failure").returncode, 0)
        self.assertTrue((self.directory / LOCK_FILENAME).is_file())

    def test_os_releases_lock_after_forced_termination(self):
        owner = self.start_owner()
        self.assert_locked(self.run_cli("reject"))
        owner.kill()
        owner.wait(timeout=5)
        self.assertEqual(self.run_cli("startup-failure").returncode, 0)

    def test_os_releases_lock_after_unexpected_startup_exception(self):
        result = self.run_cli("exception")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("simulated startup exception", result.stderr)
        self.assertEqual(self.run_cli("startup-failure").returncode, 0)

    def test_data_directory_is_still_owned_after_bounded_shutdown(self):
        owner = self.start_owner("writer")
        self.assert_locked(self.run_cli("reject"))
        owner.communicate("\n", timeout=6)
        self.assertEqual(owner.returncode, 0)
        self.assertEqual(self.run_cli("startup-failure").returncode, 0)

    def test_clear_error_when_lock_cannot_be_opened(self):
        (self.directory / LOCK_FILENAME).mkdir()
        result = self.run_cli("reject")
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertIn("Không thể khóa --data-dir", result.stderr)
