"""
main.py - Điểm khởi đầu (Entry Point) của LAN Chat P2P

Bước 3 bổ sung:
  - Persistent history: load từ JSON khi khởi động, save khi thoát.
  - Tab autocomplete: gợi ý lệnh, tên peer, đường dẫn file.
  - /broadcast: gửi tin nhắn đến TẤT CẢ peer online.
  - File progress: thanh tiến độ khi gửi file.
  - Auto-reconnect: tự kết nối lại với current_partner khi connection drop.

Cách chạy:
  Terminal 1: python3 main.py --name Quan --port 5001
  Terminal 2: python3 main.py --name Minh --port 5002
"""

import argparse
import logging
import socket
import time

from prompt_toolkit.patch_stdout import patch_stdout

from history import load_history, save_history, start_autosave_thread
from models import state
from tcp_handler import send_chat_message, send_file, get_or_connect, start_tcp_threads
from udp_handler import start_udp_threads
from ui_handler import (
    create_prompt_session,
    display_chat_history,
    display_peer_list,
    get_prompt_text,
    print_banner,
    print_chat,
    print_error,
    print_help,
    print_info,
    print_success,
    print_system,
    print_warning,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Phát hiện IP local
# ---------------------------------------------------------------------------

def get_local_ip() -> str:
    """
    Phát hiện IP LAN bằng cách "connect" UDP đến 8.8.8.8 (không gửi gói thật).
    OS chọn interface phù hợp → lấy IP từ getsockname().
    """
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


# ---------------------------------------------------------------------------
# Callbacks từ TCP handler → UI
# ---------------------------------------------------------------------------

def on_tcp_message(peer_key: str, peer_name: str, content: str, is_current: bool):
    """
    Gọi bởi tcp_handler._receiver_loop() khi nhận MSG frame.
    Chạy trong receiver thread, nhưng patch_stdout đảm bảo output không
    đè lên dòng input.

    is_current=True  → hiển thị tin nhắn đầy đủ ngay lập tức.
    is_current=False → chỉ hiện notification ngắn (không làm loãng màn hình).
    """
    if is_current:
        print_chat(peer_name, content)
    else:
        preview = content if len(content) <= 45 else content[:45] + "..."
        print_system(f"💬 Tin nhắn mới từ {peer_name}: {preview}")


def on_tcp_system(message: str):
    """Gọi bởi tcp_handler cho các sự kiện hệ thống (connect, disconnect, file...)."""
    print_system(message)


# ---------------------------------------------------------------------------
# File progress helper
# ---------------------------------------------------------------------------

def _make_file_progress(filename: str):
    """
    Tạo progress callback cho send_file.
    Chỉ in tại các mốc 0%, 25%, 50%, 75%, 100% để không spam màn hình.

    Dùng list [last] thay vì nonlocal (tương thích Python 3.10 trở xuống).
    Closure capture biến từ outer scope — list cho phép gán lại bên trong.
    """
    last_milestone = [-1]

    def _fn(sent: int, total: int):
        if total == 0:
            return
        pct = int(sent * 100 / total)
        milestone = (pct // 25) * 25      # Làm tròn xuống mốc 25 gần nhất
        if milestone > last_milestone[0]:
            last_milestone[0] = milestone
            filled = milestone // 5        # 20 ô tổng, mỗi 5% = 1 ô
            bar = "█" * filled + "░" * (20 - filled)
            print_info(
                f"  [{bar}] {milestone}%"
                f"  ({sent // 1024}/{total // 1024} KB)"
                f"  '{filename}'"
            )

    return _fn


# ---------------------------------------------------------------------------
# Tìm peer theo tên
# ---------------------------------------------------------------------------

def _find_peer(name: str):
    """
    Tìm peer theo tên (case-insensitive).
    Trả về (peer_key, Peer) hoặc (None, None) nếu không tìm thấy.
    """
    for peer in state.get_peer_list_snapshot():
        if peer.name.lower() == name.lower():
            return state.get_peer_key(peer.ip, peer.tcp_port), peer
    return None, None


# ---------------------------------------------------------------------------
# Xử lý lệnh
# ---------------------------------------------------------------------------

def handle_command(cmd: str) -> bool:
    """
    Xử lý một lệnh từ user.
    Trả về True khi cần thoát (/quit).
    """
    cmd = cmd.strip()
    if not cmd:
        return False

    # Shortcut: nếu đang trong chat, gõ thẳng không cần /send
    if not cmd.startswith("/") and state.current_partner:
        return handle_command(f"/send {cmd}")

    if not cmd.startswith("/"):
        print_warning("Lệnh phải bắt đầu bằng '/'. Gõ /help để xem hướng dẫn.")
        return False

    parts = cmd.split(maxsplit=1)
    command = parts[0].lower()
    args = parts[1].strip() if len(parts) > 1 else ""

    # ── Discovery ──────────────────────────────────────────────────────────

    if command == "/list":
        display_peer_list()

    elif command == "/status":
        if args not in ("available", "busy"):
            print_error("Dùng: /status available  hoặc  /status busy")
        else:
            state.set_status(args)
            print_success(f"Đã đổi trạng thái thành '{args}'.")

    # ── Chat 1-1 ───────────────────────────────────────────────────────────

    elif command == "/chat":
        if not args:
            print_error("Dùng: /chat <tên_peer>  (Tab để gợi ý tên)")
            return False
        peer_key, peer = _find_peer(args)
        if peer is None:
            print_error(f"Không tìm thấy peer '{args}'. Dùng /list để xem danh sách.")
            return False
        print_info(f"Đang kết nối TCP đến {peer.name} ({peer.ip}:{peer.tcp_port})...")
        sock = get_or_connect(peer_key)
        if sock is None:
            print_error(f"Không thể kết nối đến {peer.name}.")
            return False
        with state.lock:
            state.current_partner = peer_key
        print_success(
            f"Đang chat với {peer.name}. "
            "Gõ trực tiếp hoặc /send. "
            "/leave để thoát."
        )
        display_chat_history(peer_key)

    elif command == "/send":
        if not state.current_partner:
            print_error("Chưa chọn người chat. Dùng /chat <tên> trước.")
            return False
        if not args:
            print_error("Dùng: /send <tin nhắn>")
            return False
        peer_key = state.current_partner
        if send_chat_message(peer_key, args):
            timestamp = time.strftime("%H:%M:%S")
            log_entry = f"[{timestamp}] {state.my_name} (bạn): {args}"
            with state.lock:
                state.chat_history.setdefault(peer_key, []).append(log_entry)
            print_chat(f"{state.my_name} (bạn)", args, is_self=True)
        else:
            print_error("Gửi thất bại. Peer có thể đã offline.")

    elif command == "/history":
        if not state.current_partner:
            print_error("Chưa chọn người chat. Dùng /chat <tên> trước.")
        else:
            display_chat_history(state.current_partner)

    elif command == "/leave":
        """
        /leave CHỈ xóa current_partner (UI state).
        KHÔNG gọi socket.close() → kết nối TCP GIỮ NGUYÊN.
        /chat lại sau → get_or_connect() tái sử dụng socket cũ.
        """
        if not state.current_partner:
            print_warning("Hiện không đang chat với ai.")
            return False
        with state.lock:
            key = state.current_partner
            name = state.peer_list.get(key)
            name = name.name if name else "?"
            state.current_partner = None   # Socket vẫn sống!
        print_info(f"Đã rời chat với {name}. (Kết nối TCP được giữ lại.)")

    # ── Broadcast ──────────────────────────────────────────────────────────

    elif command == "/broadcast":
        """
        Gửi một tin nhắn đến TẤT CẢ peer đang online.
        Với mỗi peer: gọi send_chat_message (tự connect nếu chưa có kết nối).
        Thống kê số peer gửi thành công / thất bại.
        """
        if not args:
            print_error("Dùng: /broadcast <tin nhắn>")
            return False
        peers = state.get_peer_list_snapshot()
        if not peers:
            print_warning("Không có peer nào online.")
            return False

        print_info(f"Đang broadcast đến {len(peers)} peer...")
        ok, fail = 0, 0
        timestamp = time.strftime("%H:%M:%S")
        for peer in peers:
            key = state.get_peer_key(peer.ip, peer.tcp_port)
            if send_chat_message(key, f"[Broadcast] {args}"):
                ok += 1
                log = f"[{timestamp}] {state.my_name} → broadcast: {args}"
                with state.lock:
                    state.chat_history.setdefault(key, []).append(log)
            else:
                fail += 1

        if fail == 0:
            print_success(f"Broadcast thành công đến {ok}/{len(peers)} peer.")
        else:
            print_warning(f"Broadcast: {ok} thành công, {fail} thất bại.")

    # ── File Transfer ──────────────────────────────────────────────────────

    elif command == "/file":
        if not state.current_partner:
            print_error("Chưa chọn người nhận. Dùng /chat <tên> trước.")
            return False
        if not args:
            print_error("Dùng: /file <đường_dẫn>  (Tab để gợi ý đường dẫn)")
            return False

        peer_key = state.current_partner
        with state.lock:
            p = state.peer_list.get(peer_key)
            peer_name = p.name if p else "?"

        import pathlib
        file_size = 0
        try:
            file_size = pathlib.Path(args).stat().st_size
        except OSError:
            print_error(f"File không tồn tại: '{args}'")
            return False

        print_info(
            f"Gửi '{pathlib.Path(args).name}' "
            f"({file_size // 1024} KB) đến {peer_name}..."
        )
        progress = _make_file_progress(pathlib.Path(args).name)
        success = send_file(peer_key, args, progress_fn=progress)
        if success:
            print_success(f"Đã gửi file thành công!")
        else:
            print_error("Gửi file thất bại.")

    # ── Chung ──────────────────────────────────────────────────────────────

    elif command == "/help":
        print_help()

    elif command == "/quit":
        print_info("Đang lưu lịch sử và thoát... 👋")
        state.stop_event.set()
        return True

    else:
        print_warning(f"Lệnh không nhận ra: '{command}'. Gõ /help để xem hướng dẫn.")

    return False


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------

def main():
    # --- 1. Đọc tham số dòng lệnh ---
    parser = argparse.ArgumentParser(
        description="LAN Chat & File Sharing P2P",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Ví dụ:\n"
            "  python3 main.py --name Quan --port 5001\n"
            "  python3 main.py --name Minh --port 5002\n"
        ),
    )
    parser.add_argument("--name", required=True, help="Tên hiển thị của bạn")
    parser.add_argument("--port", required=True, type=int, help="TCP port (vd: 5001)")
    parser.add_argument("--debug", action="store_true", help="Bật debug logging")
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    # --- 2. Khởi tạo state ---
    state.my_name = args.name
    state.my_tcp_port = args.port
    state.my_ip = get_local_ip()

    # --- 3. Load lịch sử chat từ disk ---
    # Gọi TRƯỚC khi start UDP threads để tránh race condition với upsert_peer.
    # (upsert_peer không ghi đè history đã load nhờ fix trong models.py)
    loaded = load_history()

    # --- 4. Tạo prompt session ---
    session = create_prompt_session()

    with patch_stdout():

        print_banner()
        print_info(
            f"Tên: {state.my_name} | IP: {state.my_ip} | TCP: {state.my_tcp_port}"
        )
        if loaded:
            print_info(f"Đã tải {loaded} tin nhắn từ lịch sử cũ.")
        print_help()

        # --- 5. Start luồng UDP ---
        t_broadcast, t_listen = start_udp_threads(notify_fn=print_system)
        print_success("UDP Discovery sẵn sàng.")

        # --- 6. Start luồng TCP (acceptor + reconnect) ---
        t_tcp = start_tcp_threads(
            msg_callback=on_tcp_message,
            system_callback=on_tcp_system,
        )
        print_success(f"TCP Acceptor sẵn sàng trên port {state.my_tcp_port}.")

        # --- 7. Start autosave lịch sử (mỗi 60 giây) ---
        t_autosave = start_autosave_thread()

        print_info("Gõ /list để xem peer, /chat <tên> để bắt đầu. Tab gợi ý lệnh.\n")

        # --- 8. Vòng lặp chính ---
        while not state.stop_event.is_set():
            try:
                user_input = session.prompt(get_prompt_text())
                if handle_command(user_input):
                    break
            except KeyboardInterrupt:
                print_info("\nCtrl+C. Đang thoát...")
                state.stop_event.set()
                break
            except EOFError:
                state.stop_event.set()
                break

        # --- 9. Dọn dẹp ---
        state.stop_event.set()

        # Lưu lịch sử lần cuối trước khi thoát
        save_history()

        # Đóng tất cả kết nối TCP
        with state.conn_lock:
            for sock in state.connections.values():
                try:
                    sock.close()
                except OSError:
                    pass
            state.connections.clear()

        # Chờ các luồng chính dừng
        t_broadcast.join(timeout=3)
        t_listen.join(timeout=3)
        t_tcp.join(timeout=3)

        print_success("Đã thoát thành công.")


if __name__ == "__main__":
    main()
