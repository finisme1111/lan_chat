"""
main.py - Điểm khởi đầu (Entry Point) của LAN Chat & File Sharing P2P

Các tính năng nổi bật:
- Tương thích đa nền tảng (Linux & Windows).
- Xử lý triệt để các trường hợp biên, lỗi ngắt kết nối đột ngột, chống văng traceback.
- Quản lý giao diện dòng lệnh mượt mà với prompt_toolkit và patch_stdout.
- Hỗ trợ đầy đủ các lệnh: /list, /chat, /send, /file, /broadcast, /history, /leave, /status, /help, /quit.
"""

import argparse
import logging
import os
import socket
import sys
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
# Cấu hình Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Tự động phát hiện IP máy trong mạng LAN (Đa nền tảng)
# ---------------------------------------------------------------------------

def get_local_ip() -> str:
    """
    Phát hiện IP nội bộ đáng tin cậy trên cả Linux và Windows.
    Thử nghiệm kết nối socket UDP dummy tới 8.8.8.8 để OS chọn đúng interface mạng.
    """
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(1.0)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        pass

    # Fallback cho trường hợp mạng LAN cô lập không có gateway internet
    try:
        hostname = socket.gethostname()
        ip = socket.gethostbyname(hostname)
        if ip and not ip.startswith("127."):
            return ip
    except Exception:
        pass

    return "127.0.0.1"


# ---------------------------------------------------------------------------
# Callbacks từ TCP handler đưa dữ liệu lên UI
# ---------------------------------------------------------------------------

def on_tcp_message(peer_key: str, peer_name: str, content: str, is_current: bool):
    """
    Hiển thị tin nhắn nhận được.
    patch_stdout đảm bảo tin nhắn không đè lên dòng lệnh đang gõ.
    """
    if is_current:
        print_chat(peer_name, content)
    else:
        preview = content if len(content) <= 45 else content[:45] + "..."
        print_system(f"💬 Tin nhắn mới từ {peer_name}: {preview}")


def on_tcp_system(message: str):
    """Hiển thị các thông báo hệ thống và sự kiện mạng."""
    print_system(message)


# ---------------------------------------------------------------------------
# Hàm hỗ trợ hiển thị tiến độ gửi file
# ---------------------------------------------------------------------------

def _make_send_file_progress(filename: str):
    """
    Tạo callback cập nhật tiến độ gửi file ra màn hình (0%, 25%, 50%, 75%, 100%).
    """
    last_milestone = [-1]

    def _progress(sent: int, total: int):
        if total <= 0:
            return
        pct = int(sent * 100 / total)
        milestone = (pct // 25) * 25
        if milestone > last_milestone[0]:
            last_milestone[0] = milestone
            filled = milestone // 5
            bar = "█" * filled + "░" * (20 - filled)
            print_info(
                f"  [Gửi file] [{bar}] {milestone}% "
                f"({sent // 1024}/{total // 1024} KB) '{filename}'"
            )

    return _progress


# ---------------------------------------------------------------------------
# Tìm kiếm Peer theo tên
# ---------------------------------------------------------------------------

def _find_peer_by_name(name: str):
    """
    Tìm peer theo tên hiển thị (không phân biệt chữ hoa/thường).
    """
    clean_target = name.strip().lower()
    for peer in state.get_peer_list_snapshot():
        if peer.name.lower() == clean_target:
            return state.get_peer_key(peer.ip, peer.tcp_port), peer
    return None, None


# ---------------------------------------------------------------------------
# Xử lý lệnh người dùng (Command Handler)
# ---------------------------------------------------------------------------

def handle_command(cmd_raw: str) -> bool:
    """
    Xử lý lệnh từ người dùng.
    Trả về True nếu cần thoát chương trình (/quit).
    """
    cmd = cmd_raw.strip()
    if not cmd:
        return False

    # Phím tắt tiện lợi: Khi đang trong cuộc trò chuyện, gõ trực tiếp nội dung để gửi
    if not cmd.startswith("/") and state.current_partner:
        return handle_command(f"/send {cmd}")

    if not cmd.startswith("/"):
        print_warning("Lệnh không hợp lệ (phải bắt đầu bằng '/'). Gõ /help để xem hướng dẫn.")
        return False

    parts = cmd.split(maxsplit=1)
    command = parts[0].lower()
    args = parts[1].strip() if len(parts) > 1 else ""

    # ------------------ /list ------------------
    if command == "/list":
        display_peer_list()

    # ------------------ /status ------------------
    elif command == "/status":
        if args not in ("available", "busy", "away"):
            print_warning("Cú pháp: /status available, busy, hoặc away")
        else:
            state.set_status(args)
            print_success(f"Đã cập nhật trạng thái cá nhân: '{args}'.")

    # ------------------ /chat ------------------
    elif command == "/chat":
        if not args:
            print_warning("Cú pháp: /chat <tên_peer> (Nhấn Tab để gợi ý)")
            return False

        peer_key, peer = _find_peer_by_name(args)
        if peer is None:
            print_warning(f"Không tìm thấy người dùng '{args}'. Gõ /list để xem danh sách online.")
            return False

        print_info(f"Đang kết nối tới {peer.name} ({peer.ip}:{peer.tcp_port})...")
        sock = get_or_connect(peer_key)
        if sock is None:
            print_error(f"Không thể kết nối TCP tới {peer.name}. Đối tác có thể vừa offline hoặc bị chặn tường lửa.")
            return False

        with state.lock:
            state.current_partner = peer_key

        print_success(f"Đã kết nối với {peer.name}. Nhập nội dung để chat, /file để gửi file, /leave để thoát.")
        display_chat_history(peer_key)

    # ------------------ /send ------------------
    elif command == "/send":
        if not state.current_partner:
            print_warning("Chưa chọn đối tác trò chuyện. Vui lòng dùng lệnh /chat <tên> trước.")
            return False

        if not args:
            print_warning("Cú pháp: /send <tin_nhắn>")
            return False

        # Kiểm tra trường hợp người dùng gõ /send kèm đường dẫn file
        if os.path.isfile(args):
            print_info(f"Phát hiện đường dẫn tệp tin. Đang chuyển sang gửi file: '{args}'...")
            return handle_command(f"/file {args}")

        peer_key = state.current_partner
        if send_chat_message(peer_key, args):
            timestamp = time.strftime("%H:%M:%S")
            log_entry = f"[{timestamp}] {state.my_name} (bạn): {args}"
            with state.lock:
                state.chat_history.setdefault(peer_key, []).append(log_entry)
            print_chat(f"{state.my_name} (bạn)", args, is_self=True)
        else:
            print_error("Gửi tin nhắn thất bại. Kết nối đã bị gián đoạn.")

    # ------------------ /history ------------------
    elif command == "/history":
        if not state.current_partner:
            print_warning("Chưa chọn đối tác. Vui lòng dùng lệnh /chat <tên> trước.")
        else:
            display_chat_history(state.current_partner)

    # ------------------ /leave ------------------
    elif command == "/leave":
        if not state.current_partner:
            print_warning("Hiện tại bạn không trong phiên chat nào.")
            return False

        with state.lock:
            partner_key = state.current_partner
            p = state.peer_list.get(partner_key)
            name = p.name if p else partner_key
            state.current_partner = None

        print_info(f"Đã rời cuộc trò chuyện với {name}. (Kết nối TCP vẫn được giữ để tái sử dụng).")

    # ------------------ /broadcast ------------------
    elif command == "/broadcast":
        if not args:
            print_warning("Cú pháp: /broadcast <nội_dung_thông_báo>")
            return False

        peers = state.get_peer_list_snapshot()
        if not peers:
            print_warning("Hiện không có ai online trong mạng LAN để gửi broadcast.")
            return False

        print_info(f"Đang gửi thông báo tới {len(peers)} người dùng...")
        success_count = 0
        timestamp = time.strftime("%H:%M:%S")

        for peer in peers:
            key = state.get_peer_key(peer.ip, peer.tcp_port)
            if send_chat_message(key, f"[Broadcast] {args}"):
                success_count += 1
                log_msg = f"[{timestamp}] {state.my_name} -> broadcast: {args}"
                with state.lock:
                    state.chat_history.setdefault(key, []).append(log_msg)

        print_success(f"Đã gửi thông báo thành công tới {success_count}/{len(peers)} người dùng.")

    # ------------------ /file ------------------
    elif command == "/file":
        if not state.current_partner:
            print_warning("Chưa chọn người nhận file. Vui lòng dùng /chat <tên> trước.")
            return False

        if not args:
            print_warning("Cú pháp: /file <đường_dẫn_tệp> (Nhấn Tab để duyệt file)")
            return False

        # Kiểm tra an toàn: File có tồn tại hay không?
        clean_path = os.path.expanduser(args.strip().strip("'\""))
        if not os.path.isfile(clean_path):
            print_warning(f"Tệp tin không tồn tại hoặc không phải file hợp lệ: '{clean_path}'")
            return False

        peer_key = state.current_partner
        with state.lock:
            p = state.peer_list.get(peer_key)
            peer_name = p.name if p else peer_key

        file_size = os.path.getsize(clean_path)
        filename = os.path.basename(clean_path)

        print_info(f"Bắt đầu gửi '{filename}' ({file_size // 1024} KB) tới {peer_name}...")
        progress_cb = _make_send_file_progress(filename)

        success = send_file(peer_key, clean_path, progress_fn=progress_cb)
        if success:
            print_success(f"Đã gửi file '{filename}' thành công tới {peer_name}!")
        else:
            print_error(f"Gửi file '{filename}' thất bại do lỗi đường truyền.")

    # ------------------ /help ------------------
    elif command == "/help":
        print_help()

    # ------------------ /quit ------------------
    elif command == "/quit":
        print_info("Đang lưu lịch sử chat và đóng ứng dụng... Hẹn gặp lại! 👋")
        state.stop_event.set()
        return True

    # ------------------ Lệnh không xác định ------------------
    else:
        print_warning(f"Lệnh không nhận dạng: '{command}'. Gõ /help để xem hướng dẫn.")

    return False


# ---------------------------------------------------------------------------
# Hàm khởi chạy chính (Main Function)
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="LAN Chat & File Sharing P2P (Hệ thống mạng đồng đẳng)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--name", required=True, help="Tên hiển thị của bạn (vd: Quan)")
    parser.add_argument("--port", required=True, type=int, help="Cổng TCP lắng nghe (vd: 5001)")
    parser.add_argument("--debug", action="store_true", help="Bật chế độ ghi log chi tiết")
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    # 1. Khởi tạo trạng thái danh tính cá nhân
    state.my_name = args.name.strip()
    state.my_tcp_port = args.port
    state.my_ip = get_local_ip()

    # 2. Tải lịch sử chat đã lưu từ phiên trước
    loaded_msgs = load_history()

    # 3. Tạo session giao diện điều khiển với prompt_toolkit
    session = create_prompt_session()

    with patch_stdout():
        print_banner()
        print_info(f"Danh tính: {state.my_name} | IP: {state.my_ip} | Cổng TCP: {state.my_tcp_port}")
        if loaded_msgs > 0:
            print_info(f"Đã nạp {loaded_msgs} tin nhắn từ tệp lịch sử.")
        print_help()

        # 4. Khởi chạy hệ thống UDP Discovery (Broadcast + Listener)
        t_broadcast, t_listen = start_udp_threads(notify_fn=print_system)
        print_success("Hệ thống UDP Discovery đã sẵn sàng.")

        # 5. Khởi chạy hệ thống TCP (Acceptor + Auto-reconnect)
        t_tcp = start_tcp_threads(
            msg_callback=on_tcp_message,
            system_callback=on_tcp_system,
        )
        print_success(f"Máy chủ TCP đang lắng nghe trên cổng {state.my_tcp_port}.")

        # 6. Khởi chạy luồng tự động lưu lịch sử mỗi 60s
        t_autosave = start_autosave_thread()

        print_info("Gõ /list để xem danh sách online, /chat <tên> để trò chuyện. Nhấn Tab để gợi ý lệnh.\n")

        # 7. Vòng lặp nhận lệnh chính
        while not state.stop_event.is_set():
            try:
                user_input = session.prompt(get_prompt_text())
                if handle_command(user_input):
                    break
            except KeyboardInterrupt:
                # Bắt tổ hợp Ctrl+C: Thoát sạch sẽ không để văng traceback
                print_info("\nĐã nhận tín hiệu ngắt (Ctrl+C). Đang đóng ứng dụng...")
                state.stop_event.set()
                break
            except EOFError:
                # Bắt tổ hợp Ctrl+D
                state.stop_event.set()
                break
            except Exception as e:
                # Bắt mọi ngoại lệ bất ngờ ở vòng lặp giao diện để giữ app không bị crash
                print_warning(f"Đã xảy ra lỗi giao diện: {e}")

        # 8. Quy trình dọn dẹp tài nguyên khi tắt ứng dụng
        state.stop_event.set()

        # Lưu lịch sử lần cuối
        save_history()

        # Đóng tất cả socket TCP đang hoạt động
        with state.conn_lock:
            for sock in state.connections.values():
                try:
                    sock.close()
                except OSError:
                    pass
            state.connections.clear()

        # Chờ các luồng kết thúc tối đa 2 giây
        t_broadcast.join(timeout=2.0)
        t_listen.join(timeout=2.0)
        t_tcp.join(timeout=2.0)

        print_success("Đã giải phóng tài nguyên và thoát an toàn.")


if __name__ == "__main__":
    main()
