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

import html as html_lib

from prompt_toolkit import PromptSession
from prompt_toolkit.completion import Completer, Completion, PathCompleter
from prompt_toolkit.history import InMemoryHistory
from prompt_toolkit.patch_stdout import patch_stdout
from prompt_toolkit.formatted_text import HTML
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


# ---------------------------------------------------------------------------
# Hàm tiện ích in có màu (thread-safe với patch_stdout)
# ---------------------------------------------------------------------------
# Tất cả hàm dưới đây dùng html_lib.escape() cho dữ liệu user-provided
# vì HTML() của prompt_toolkit dùng XML parser nghiêm ngặt:
# & → &amp;  |  < → &lt;  |  > → &gt;

def print_info(msg: str):
    """In thông báo hệ thống màu cyan."""
    print_formatted_text(
        HTML(f"<info>[INFO]</info> {html_lib.escape(msg)}"), style=APP_STYLE
    )


def print_success(msg: str):
    """In thông báo thành công màu xanh."""
    print_formatted_text(
        HTML(f"<success>[OK]</success>  {html_lib.escape(msg)}"), style=APP_STYLE
    )


def print_warning(msg: str):
    """In cảnh báo màu vàng."""
    print_formatted_text(
        HTML(f"<warning>[WARN]</warning> {html_lib.escape(msg)}"), style=APP_STYLE
    )


def print_error(msg: str):
    """In thông báo lỗi màu đỏ."""
    print_formatted_text(
        HTML(f"<error>[ERR]</error>  {html_lib.escape(msg)}"), style=APP_STYLE
    )


def print_system(msg: str):
    """In thông báo hệ thống (peer join/leave/file) màu tím."""
    print_formatted_text(
        HTML(f"<system>*** {html_lib.escape(msg)} ***</system>"), style=APP_STYLE
    )


def print_chat(sender: str, msg: str, is_self: bool = False):
    """
    In một dòng tin nhắn chat.
    is_self=True → màu xanh lá (tin của mình).
    is_self=False → màu xanh dương (tin của peer).
    """
    safe_sender = html_lib.escape(sender)
    safe_msg = html_lib.escape(msg)
    tag = "self" if is_self else "peer"
    print_formatted_text(
        HTML(f"<{tag}>[{safe_sender}]</{tag}> {safe_msg}"), style=APP_STYLE
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
│  /status <s>        Đổi trạng thái (available|busy)    │
├─ Lệnh Chat ───────────────────────────────────────────┤
│  /chat <tên>        Bắt đầu chat với peer              │
│  /send <tin nhắn>   Gửi tin nhắn đến peer hiện tại    │
│  /history           Xem lại lịch sử chat               │
│  /leave             Rời chat (giữ kết nối TCP)         │
├─ Lệnh File ───────────────────────────────────────────┤
│  /file <đường dẫn>  Gửi file đến peer hiện tại        │
├─ Chung ───────────────────────────────────────────────┤
│  /help              Hiển thị trợ giúp này              │
│  /quit              Thoát chương trình                 │
└───────────────────────────────────────────────────────┘
"""


def print_banner():
    """
    In banner khởi động.
    html_lib.escape() chuyển ký tự đặc biệt trong ASCII art thành HTML entities,
    tránh ExpatError khi HTML() parse chuỗi có &, <, >.
    """
    safe_banner = html_lib.escape(BANNER)
    print_formatted_text(HTML(f"<success>{safe_banner}</success>"), style=APP_STYLE)


def print_help():
    """In hướng dẫn sử dụng."""
    print_formatted_text(HELP_TEXT)


# ---------------------------------------------------------------------------
# Hiển thị danh sách Peer
# ---------------------------------------------------------------------------

def display_peer_list():
    """
    In danh sách tất cả peer đang online.
    Lấy snapshot từ state (thread-safe) rồi in ra.
    """
    peers = state.get_peer_list_snapshot()
    if not peers:
        print_warning("Không có peer nào online. Đang chờ discovery...")
        return

    print_formatted_text(
        HTML(f"\n<info>── Danh sách Peer Online ({len(peers)} người) ──</info>"),
        style=APP_STYLE,
    )
    for i, peer in enumerate(peers, 1):
        key = state.get_peer_key(peer.ip, peer.tcp_port)
        with state.lock:
            unread = state.unread_counts.get(key, 0)
            is_connected = key in state.connections

        # Hiển thị badge: kết nối TCP đang mở và tin chưa đọc
        badges = []
        if is_connected:
            badges.append("🔗 connected")
        if unread > 0:
            badges.append(f"💬 {unread} chưa đọc")
        badge_str = f"  [{', '.join(badges)}]" if badges else ""

        status_icon = "🟢" if peer.status == "available" else ("🟡" if peer.status == "away" else "🔴")
        print_formatted_text(
            HTML(
                f"  <b>{i}.</b> <peer>{html_lib.escape(peer.name)}</peer>"
                f" ({peer.ip}:{peer.tcp_port})"
                f" {status_icon} {peer.status}"
                f"{html_lib.escape(badge_str)}"
            ),
            style=APP_STYLE,
        )
    print_formatted_text("")


# ---------------------------------------------------------------------------
# Hiển thị lịch sử chat
# ---------------------------------------------------------------------------

def display_chat_history(peer_key: str, last_n: int = 20):
    """
    In lịch sử chat với một peer cụ thể và xóa unread count.

    Tham số:
        peer_key: "ip:port" của peer.
        last_n:   Chỉ hiển thị N tin nhắn gần nhất (tránh scroll quá nhiều).
    """
    with state.lock:
        history = list(state.chat_history.get(peer_key, []))
        peer_name = peer_key  # fallback
        if peer_key in state.peer_list:
            peer_name = state.peer_list[peer_key].name
        state.unread_counts[peer_key] = 0   # Đánh dấu đã đọc

    if not history:
        print_info(f"Chưa có tin nhắn với {peer_name}.")
        return

    shown = history[-last_n:]
    omitted = len(history) - len(shown)
    safe_name = html_lib.escape(peer_name)

    print_formatted_text(
        HTML(f"\n<info>── Lịch sử chat với {safe_name} (hiển thị {len(shown)}/{len(history)}) ──</info>"),
        style=APP_STYLE,
    )
    if omitted > 0:
        print_formatted_text(
            HTML(f"<history>  ... {omitted} tin nhắn cũ hơn ...</history>"),
            style=APP_STYLE,
        )
    for entry in shown:
        # entry là plain text đã format sẵn: "[HH:MM:SS] Name: content"
        print_formatted_text(html_lib.escape(entry))
    print_formatted_text("")


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
      '/status '      → gợi ý 'available' / 'busy'
      '/file ./'      → gợi ý đường dẫn file (PathCompleter)
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
            for peer in peers:
                if peer.name.lower().startswith(arg.lower()):
                    # Thay thế phần đã gõ của tên peer
                    yield Completion(
                        peer.name,
                        start_position=-len(arg),
                        display=peer.name,
                        display_meta=f"{peer.ip}:{peer.tcp_port} [{peer.status}]",
                    )

        # /status <trạng thái>
        elif cmd == "/status":
            for s in ("available", "busy", "away"):
                if s.startswith(arg):
                    yield Completion(s, start_position=-len(arg))

        # /file <đường_dẫn> — delegate cho PathCompleter
        elif cmd == "/file":
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
        status = state.my_status
        partner_key = state.current_partner

    if partner_key:
        peer_name = partner_key  # fallback
        with state.lock:
            if partner_key in state.peer_list:
                peer_name = state.peer_list[partner_key].name
        return f"[{name}|{status} → {peer_name}]> "
    return f"[{name}|{status}]> "
