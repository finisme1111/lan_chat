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
import pathlib
import threading

from models import state

logger = logging.getLogger(__name__)

HISTORY_FILE = pathlib.Path("chat_history.json")


# ---------------------------------------------------------------------------
# Lưu lịch sử
# ---------------------------------------------------------------------------

def save_history() -> bool:
    """
    Ghi toàn bộ chat_history ra file JSON.
    Thread-safe: lấy snapshot bằng lock trước khi ghi.
    Trả về True nếu thành công.
    """
    try:
        # Lấy snapshot để không giữ lock trong lúc ghi disk
        with state.lock:
            snapshot = {
                key: list(msgs)
                for key, msgs in state.chat_history.items()
                if msgs  # Chỉ lưu những key có ít nhất 1 tin nhắn
            }

        HISTORY_FILE.write_text(
            json.dumps(snapshot, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        total = sum(len(v) for v in snapshot.values())
        logger.info(f"[History] Đã lưu {total} tin nhắn vào {HISTORY_FILE}")
        return True

    except IOError as e:
        logger.error(f"[History] Lỗi khi lưu: {e}")
        return False


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
    if not HISTORY_FILE.exists():
        logger.info(f"[History] Không tìm thấy {HISTORY_FILE}, bắt đầu mới.")
        return 0

    try:
        raw = HISTORY_FILE.read_text(encoding="utf-8")
        data: dict = json.loads(raw)

        loaded = 0
        with state.lock:
            for key, messages in data.items():
                if not isinstance(messages, list):
                    continue
                # Không ghi đè nếu key đã có data từ session hiện tại
                if not state.chat_history.get(key):
                    state.chat_history[key] = list(messages)
                    state.unread_counts.setdefault(key, 0)
                    loaded += len(messages)

        logger.info(f"[History] Đã tải {loaded} tin nhắn từ {HISTORY_FILE}")
        return loaded

    except (json.JSONDecodeError, IOError) as e:
        logger.error(f"[History] Lỗi khi tải: {e}")
        return 0


# ---------------------------------------------------------------------------
# Auto-save thread
# ---------------------------------------------------------------------------

def _autosave_worker():
    """
    Worker thread: lưu lịch sử tự động mỗi 60 giây.

    Dùng stop_event.wait(timeout) thay vì time.sleep():
    - Nếu stop_event được set (app thoát), wait() trả về ngay lập tức
      thay vì ngủ hết 60 giây.
    - Đây là pattern chuẩn cho background thread có thể dừng nhanh.
    """
    while not state.stop_event.is_set():
        # Ngủ 60 giây HOẶC cho đến khi stop_event được set
        state.stop_event.wait(timeout=60.0)
        if not state.stop_event.is_set():
            save_history()


def start_autosave_thread() -> threading.Thread:
    """Khởi động thread autosave. Trả về thread để main.py có thể join()."""
    t = threading.Thread(
        target=_autosave_worker,
        name="History-Autosave",
        daemon=True,
    )
    t.start()
    logger.info("[History] Autosave thread started (interval: 60s)")
    return t

