"""
history.py - Lưu và tải lịch sử chat (Persistent Chat History)

Tại sao cần module này?
- Mỗi lần tắt app, chat_history trong RAM bị mất.
- Module này serialize dict sang JSON và ghi ra disk, đọc lại khi khởi động.

Format file JSON:
  {
    "192.168.1.2:5002": [
      "[10:30:01] Minh: Chào bạn!",
      "[10:30:05] Quan (bạn): Chào Minh!"
    ],
    ...
  }

Chiến lược lưu:
  - Tự động lưu mỗi 60 giây (autosave thread).
  - Lưu ngay khi /quit (đảm bảo không mất tin nhắn cuối).
"""

import json
import logging
import os
import tempfile
import threading
from concurrent.futures import Future, TimeoutError as FutureTimeout
from pathlib import Path
from typing import Callable

from models import state

logger = logging.getLogger(__name__)

HISTORY_FILENAME = "chat_history.json"
_save_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Lưu lịch sử
# ---------------------------------------------------------------------------

def save_history() -> bool:
    """
    Ghi toàn bộ chat_history ra file JSON.
    Thread-safe: lấy snapshot bằng lock trước khi ghi.
    Trả về True nếu thành công.
    """
    path = state.data_dir / HISTORY_FILENAME
    with _save_lock:
        if not state.history_writable:
            return False
        with state.lock:
            snapshot = {key: list(messages) for key, messages in state.chat_history.items() if messages}

        part_path = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w", encoding="utf-8", dir=path.parent, prefix=".chat-history-", delete=False,
            ) as temp:
                part_path = Path(temp.name)
                json.dump(snapshot, temp, ensure_ascii=False, indent=2)
            os.replace(part_path, path)
            part_path = None
        except (OSError, UnicodeError) as e:
            logger.error(f"[History] Lỗi khi lưu: {e}")
            return False
        finally:
            if part_path is not None:
                try:
                    part_path.unlink(missing_ok=True)
                except OSError as e:
                    logger.warning(f"[History] Không thể dọn '{part_path}': {e}")

        total = sum(len(messages) for messages in snapshot.values())
        logger.info(f"[History] Đã lưu {total} tin nhắn vào {path}")
        return True


# ---------------------------------------------------------------------------
# Đọc lịch sử
# ---------------------------------------------------------------------------

def load_history() -> int:
    """
    Đọc lịch sử từ file JSON vào state.chat_history khi khởi động.
    Chỉ điền vào key chưa có data (không ghi đè session hiện tại).
    Trả về số tin nhắn đã tải.

    Quan trọng: Gọi hàm này TRƯỚC khi start UDP threads để tránh race
    với upsert_peer (xem models.py).
    """
    path = state.data_dir / HISTORY_FILENAME
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict) or any(
            not isinstance(messages, list) or not all(isinstance(msg, str) for msg in messages)
            for messages in data.values()
        ):
            raise ValueError("Cấu trúc lịch sử chat không hợp lệ.")

        loaded = 0
        with state.lock:
            for key, messages in data.items():
                if not state.chat_history.get(key):
                    state.chat_history[key] = list(messages)
                    state.unread_counts.setdefault(key, 0)
                    loaded += len(messages)

        logger.info(f"[History] Đã tải {loaded} tin nhắn từ {path}")
        return loaded

    except FileNotFoundError:
        logger.info(f"[History] Không tìm thấy {path}, bắt đầu mới.")
        return 0
    except (ValueError, OSError) as e:
        state.history_writable = False
        logger.error(f"[History] Không thể đọc {path}. Giữ file hiện có: {e}")
        return 0


# ---------------------------------------------------------------------------
# Auto-save thread
# ---------------------------------------------------------------------------

def _autosave_worker(final_save: threading.Event, result: Future):
    """
    Worker thread: lưu lịch sử tự động mỗi 60 giây.

    Dùng stop_event.wait(timeout) thay vì time.sleep():
    - Nếu stop_event được set (app thoát), wait() trả về ngay lập tức
      thay vì ngủ hết 60 giây.
    - Đây là pattern chuẩn cho background thread có thể dừng nhanh.
    """
    while not state.stop_event.wait(timeout=60.0):
        save_history()
    final_save.wait()
    result.set_result(save_history())


def start_autosave_thread() -> tuple[threading.Thread, Callable[[float], bool | None]]:
    """Autosave and the final save share a single writer."""
    final_save = threading.Event()
    result: Future[bool] = Future()
    t = threading.Thread(
        target=_autosave_worker,
        args=(final_save, result),
        name="History-Autosave",
        daemon=True,
    )
    t.start()
    logger.info("[History] Autosave thread started (interval: 60s)")

    def finish(timeout: float) -> bool | None:
        final_save.set()
        try:
            return result.result(timeout=timeout)
        except FutureTimeout:
            return None

    return t, finish

