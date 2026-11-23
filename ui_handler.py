"""
ui_handler.py - Quản lý giao diện Console bằng prompt_toolkit

Tại sao dùng prompt_toolkit thay vì input() thông thường?
=========================================================
- input() builtin của Python BLOCK hoàn toàn khi chờ user gõ.
  Nếu tin nhắn đến trong khi user đang gõ → dùng print() sẽ đè lên chữ đang gõ,
  gây UX rất xấu.

- prompt_toolkit cung cấp `patch_stdout`:
  → Mọi lệnh print() từ các luồng khác sẽ được "bơm" lên trên dòng input,
    không làm hỏng nội dung đang gõ của user.

- `PromptSession` cung cấp lịch sử gõ (↑↓), autocomplete, và nhiều tính năng khác.
"""

import threading

from prompt_toolkit import PromptSession
from prompt_toolkit.completion import Completer, Completion, PathCompleter
from prompt_toolkit.history import InMemoryHistory
from prompt_toolkit.formatted_text import FormattedText
from prompt_toolkit.styles import Style
from prompt_toolkit.shortcuts import print_formatted_text

from models import state


# ---------------------------------------------------------------------------
# Style cho giao diện
# ---------------------------------------------------------------------------

APP_STYLE = Style.from_dict({
    # Màu cho prompt
    "prompt.name":    "ansigreen bold",
    "prompt.arrow":   "ansiyellow",
    "prompt.bracket": "ansicyan",

    # Màu cho các loại thông báo
    "info":    "ansicyan",
    "success": "ansigreen bold",
    "warning": "ansiyellow",
    "error":   "ansired bold",
    "peer":    "ansiblue bold",
    "system":  "ansimagenta",
    "self":    "ansigreen",
    "history": "ansigray",
})


_output_lock = threading.Lock()
_display_controls = dict.fromkeys(
    (code for code in (*range(32), *range(127, 160)) if code not in (9, 10)),
    "\uFFFD",
)


def _display_text(text: str) -> str:
    return text.translate(_display_controls)


def _print_fragments(*fragments):
    with _output_lock:
        print_formatted_text(FormattedText(fragments), style=APP_STYLE)


def _print_notice(style: str, label: str, message: str):
    _print_fragments((f"class:{style}", label), ("", " " + _display_text(message)))

def print_info(msg: str):
    """In thông báo hệ thống màu cyan."""
    _print_notice("info", "[INFO]", msg)


def print_success(msg: str):
    """In thông báo thành công màu xanh."""
    _print_notice("success", "[OK]", msg)


def print_warning(msg: str):
    """In cảnh báo màu vàng."""
    _print_notice("warning", "[WARN]", msg)


def print_error(msg: str):
    """In thông báo lỗi màu đỏ."""
    _print_notice("error", "[ERR]", msg)


def print_system(msg: str):
    """In thông báo hệ thống (peer join/leave/file) màu tím."""
    _print_fragments(("class:system", f"*** {_display_text(msg)} ***"))


def print_chat(sender: str, msg: str, is_self: bool = False):
    """
    In một dòng tin nhắn chat.
    is_self=True → màu xanh lá (tin của mình).
    is_self=False → màu xanh dương (tin của peer).
    """
    tag = "self" if is_self else "peer"
    _print_fragments(
        (f"class:{tag}", f"[{_display_text(sender)}]"),
        ("", f" {_display_text(msg)}"),
    )


# ---------------------------------------------------------------------------
# Banner và Help
# ---------------------------------------------------------------------------

BANNER = r"""
╔══════════════════════════════════════════════════╗
║          LAN Chat & File Sharing P2P             ║
║         Bước 2: TCP Chat + File Transfer         ║
╚══════════════════════════════════════════════════╝
"""

HELP_TEXT = """
┌─ Lệnh Discovery ──────────────────────────────────────┐
│  /list              Hiển thị danh sách peer online     │
│  /status <s>        Đổi trạng thái (online|away)       │
├─ Lệnh Chat ───────────────────────────────────────────┤
│  /chat <tên|số>     Bắt đầu chat với peer              │
│  Nhập nội dung      Gửi tin nhắn đến peer hiện tại    │
│  /history           Xem lại lịch sử chat               │
│  /leave             Rời chat (giữ kết nối TCP)         │
├─ Lệnh File ───────────────────────────────────────────┤
│  /send <tệp>        Gửi file (/file là alias)          │
├─ Chung ───────────────────────────────────────────────┤
│  /help              Hiển thị trợ giúp này              │
│  /quit              Thoát chương trình                 │
└───────────────────────────────────────────────────────┘
"""


def print_banner():
    _print_fragments(("class:success", BANNER))


def print_help():
    """In hướng dẫn sử dụng."""
    _print_fragments(("", HELP_TEXT))


# ---------------------------------------------------------------------------
# Hiển thị danh sách Peer
# ---------------------------------------------------------------------------

def display_peer_list(peers):
    """
    In danh sách tất cả peer đang online.
    Dùng cùng snapshot với chỉ số mà lệnh /chat sẽ tra cứu.
    """
    if not peers:
        print_warning("Không có peer nào online. Đang chờ discovery...")
        return

    _print_fragments(("class:info", f"\n── Danh sách Peer Online ({len(peers)} người) ──"))
    for i, (key, peer) in enumerate(peers.items(), 1):
        with state.lock:
            unread = state.unread_counts.get(key, 0)
        with state.conn_lock:
            is_connected = key in state.connections

        # Hiển thị badge: kết nối TCP đang mở và tin chưa đọc
        badges = []
        if is_connected:
            badges.append("🔗 connected")
        if unread > 0:
            badges.append(f"💬 {unread} chưa đọc")
        badge_str = f"  [{', '.join(badges)}]" if badges else ""

        status_icon = "🟢" if peer.status == "available" else ("🟡" if peer.status == "away" else "🔴")
        status = "online" if peer.status == "available" else peer.status
        _print_fragments(
            ("bold", f"  {i}. "),
            ("class:peer", _display_text(peer.name)),
            ("", f" ({peer.ip}:{peer.tcp_port}) {status_icon} {status}{badge_str}"),
        )
    _print_fragments(("", ""))


# ---------------------------------------------------------------------------
# Hiển thị lịch sử chat
# ---------------------------------------------------------------------------

def display_chat_history(peer_key: str, last_n: int = 20):
    """
    In lịch sử chat với một peer cụ thể và xóa unread count.

    Tham số:
        peer_key: "ip:port" của peer.
        last_n:   Số tin gần nhất cần hiển thị, mở rộng khi còn nhiều tin chưa đọc.
    """
    with state.lock:
        history = list(state.chat_history.get(peer_key, []))
        peer_name = peer_key  # fallback
        if peer_key in state.peer_list:
            peer_name = state.peer_list[peer_key].name
        unread = state.unread_counts.get(peer_key, 0)
        first_unread = state.first_unread.get(peer_key, len(history) - unread)

    if not history:
        print_info(f"Chưa có tin nhắn với {peer_name}.")
        return

    start = max(0, min(len(history) - last_n, first_unread))
    shown = history[start:]
    omitted = len(history) - len(shown)
    _print_fragments(
        ("class:info", f"\n── Lịch sử chat với {_display_text(peer_name)} ({len(shown)}/{len(history)}) ──"),
    )
    if omitted > 0:
        _print_fragments(("class:history", f"  ... {omitted} tin nhắn cũ hơn ..."))
    for entry in shown:
        # entry là plain text đã format sẵn: "[HH:MM:SS] Name: content"
        _print_fragments(("", _display_text(entry)))
    _print_fragments(("", ""))
    with state.lock:
        remaining = max(0, state.unread_counts.get(peer_key, 0) - unread)
        state.unread_counts[peer_key] = remaining
        if remaining and unread:
            state.first_unread[peer_key] = len(history)
        elif not remaining:
            state.first_unread.pop(peer_key, None)


# ---------------------------------------------------------------------------
# Tab Autocomplete
# ---------------------------------------------------------------------------

# Danh sách tất cả lệnh — dùng để complete khi user gõ '/'
_ALL_COMMANDS = [
    "/list", "/chat", "/send", "/history", "/leave",
    "/file", "/broadcast", "/status", "/help", "/quit",
]


class LanChatCompleter(Completer):
    """
    Completer động cho LAN Chat — cập nhật gợi ý theo context lệnh.

    Cơ chế hoạt động:
    - prompt_toolkit gọi get_completions() mỗi khi user nhấn Tab.
    - document.text_before_cursor = chuỗi từ đầu dòng đến cursor.
    - Completion(text, start_position) chèn 'text' vào vị trí cursor.
      start_position âm = xóa N ký tự trước cursor rồi chèn (dùng để
      replace phần đã gõ bằng gợi ý đầy đủ).

    Các trường hợp xử lý:
      Gõ '/'          → gợi ý tên lệnh
      '/chat M'       → gợi ý tên peer bắt đầu bằng 'M'
      '/broadcast '   → gợi ý tên tất cả peer
      '/status '      → gợi ý 'online' / 'away'
      '/send ./'      → gợi ý đường dẫn file (PathCompleter)
    """

    def __init__(self):
        # PathCompleter: built-in của prompt_toolkit, tự động duyệt filesystem
        self._path_completer = PathCompleter(expanduser=True)

    def get_completions(self, document, complete_event):
        text = document.text_before_cursor
        stripped = text.lstrip()

        if not stripped:
            return

        # --- Chưa có khoảng trắng → đang gõ tên lệnh ---
        if " " not in stripped:
            for cmd in _ALL_COMMANDS:
                if cmd.startswith(stripped):
                    # start_position=-len(stripped): xóa phần đã gõ, thay bằng gợi ý đầy đủ
                    yield Completion(
                        cmd,
                        start_position=-len(stripped),
                        display=cmd,
                        display_meta="lệnh",
                    )
            return

        # --- Đã có lệnh → tách lệnh và phần argument ---
        parts = stripped.split(maxsplit=1)
        cmd = parts[0].lower()
        arg = parts[1] if len(parts) > 1 else ""

        # /chat <tên_peer> và /broadcast <tên_peer>
        if cmd in ("/chat", "/broadcast"):
            peers = state.get_peer_list_snapshot()
            for peer in peers.values():
                if peer.name.lower().startswith(arg.lower()):
                    # Thay thế phần đã gõ của tên peer
                    yield Completion(
                        peer.name,
                        start_position=-len(arg),
                        display=_display_text(peer.name),
                        display_meta=f"{peer.ip}:{peer.tcp_port} [{peer.status}]",
                    )

        # /status <trạng thái>
        elif cmd == "/status":
            for s in ("online", "away"):
                if s.startswith(arg):
                    yield Completion(s, start_position=-len(arg))

        # /file <đường_dẫn> — delegate cho PathCompleter
        elif cmd in ("/send", "/file"):
            # Tạo sub-document chỉ chứa phần đường dẫn
            import prompt_toolkit.document as _doc
            sub = _doc.Document(arg, cursor_position=len(arg))
            yield from self._path_completer.get_completions(sub, complete_event)


# ---------------------------------------------------------------------------
# Tạo PromptSession
# ---------------------------------------------------------------------------

def create_prompt_session() -> PromptSession:
    """
    Khởi tạo PromptSession với:
    - LanChatCompleter: gợi ý lệnh và tên peer khi nhấn Tab.
    - InMemoryHistory: ↑↓ để duyệt lại các lệnh đã gõ.
    - complete_while_typing=False: chỉ hiện gợi ý khi nhấn Tab,
      không pop-up tự động (phù hợp với chat app).
    """
    return PromptSession(
        completer=LanChatCompleter(),
        complete_while_typing=False,
        history=InMemoryHistory(),
    )


def get_prompt_text() -> str:
    """
    Tạo chuỗi prompt động.
    Ví dụ: "[Quan|available]> " hoặc "[Quan|busy → Minh]> "
    """
    with state.lock:
        name = state.my_name
        status = "online" if state.my_status == "available" else state.my_status
        partner_key = state.current_partner

    if partner_key:
        peer_name = partner_key  # fallback
        with state.lock:
            if partner_key in state.peer_list:
                peer_name = state.peer_list[partner_key].name
        return _display_text(f"[{name}|{status} → {peer_name}]> ")
    return _display_text(f"[{name}|{status}]> ")
