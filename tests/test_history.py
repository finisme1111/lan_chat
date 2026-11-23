import json
import threading
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import history
from models import SharedState


class HistoryTest(unittest.TestCase):
    def test_shutdown_flushes_current_snapshot_on_existing_autosave_worker(self):
        state = SharedState()
        with TemporaryDirectory() as directory, patch.object(history, "state", state):
            state.data_dir = Path(directory)
            worker, finish = history.start_autosave_thread()
            try:
                state.stop_event.set()
                state.chat_history["peer"] = ["final message"]
                self.assertIs(finish(1), True)
                path = state.data_dir / history.HISTORY_FILENAME
                self.assertEqual(json.loads(path.read_text(encoding="utf-8")), {"peer": ["final message"]})
            finally:
                state.stop_event.set()
                finish(1)
                worker.join(timeout=1)

    def test_stalled_final_save_has_bounded_wait(self):
        state = SharedState()
        started = threading.Event()
        release = threading.Event()

        def stalled_save():
            started.set()
            release.wait(timeout=3)
            return True

        with patch.object(history, "state", state), patch.object(history, "save_history", side_effect=stalled_save):
            worker, finish = history.start_autosave_thread()
            try:
                state.stop_event.set()
                self.assertIsNone(finish(0.01))
                self.assertTrue(started.wait(1))
                self.assertTrue(worker.is_alive())
            finally:
                release.set()
                worker.join(timeout=2)
            self.assertIs(finish(0), True)

    def test_failed_publish_preserves_previous_history(self):
        state = SharedState()
        with TemporaryDirectory() as directory:
            state.data_dir = Path(directory)
            path = state.data_dir / history.HISTORY_FILENAME
            path.write_text('{"peer":["old"]}', encoding="utf-8")
            state.chat_history["peer"] = ["new"]
            with patch.object(history, "state", state):
                with (
                    patch.object(history.os, "replace", side_effect=OSError("disk error")),
                    self.assertLogs(history.logger, level="ERROR"),
                ):
                    self.assertFalse(history.save_history())
                self.assertEqual(path.read_text(encoding="utf-8"), '{"peer":["old"]}')
                self.assertEqual(list(state.data_dir.iterdir()), [path])
                self.assertTrue(history.save_history())
            self.assertEqual(json.loads(path.read_text(encoding="utf-8")), {"peer": ["new"]})

    def test_invalid_history_is_preserved(self):
        state = SharedState()
        with TemporaryDirectory() as directory:
            state.data_dir = Path(directory)
            path = state.data_dir / history.HISTORY_FILENAME
            for raw in (b"[]", b'{"peer":[123]}', b"\xff"):
                with self.subTest(raw=raw):
                    path.write_bytes(raw)
                    with patch.object(history, "state", state), self.assertLogs(history.logger, level="ERROR"):
                        self.assertEqual(history.load_history(), 0)
                        self.assertFalse(history.save_history())
                    self.assertEqual(path.read_bytes(), raw)

    def test_instances_save_history_in_their_data_directories(self):
        with TemporaryDirectory() as directory:
            for name in ("alice", "bob"):
                state = SharedState()
                state.data_dir = Path(directory, name)
                state.data_dir.mkdir()
                state.chat_history["peer"] = [name]
                with patch.object(history, "state", state):
                    self.assertTrue(history.save_history())
            for name in ("alice", "bob"):
                path = Path(directory, name, history.HISTORY_FILENAME)
                self.assertEqual(json.loads(path.read_text(encoding="utf-8")), {"peer": [name]})
