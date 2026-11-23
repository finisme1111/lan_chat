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
import queue
import socket
import sys
import threading
import time
from concurrent.futures import Future
from functools import partial
from pathlib import Path
from typing import Callable

from prompt_toolkit.patch_stdout import patch_stdout

from history import load_history, start_autosave_thread
from models import DEFAULT_DOWNLOAD_QUOTA_GIB, GIB, Peer, state
from protocol import MAX_MSG_LEN, UDP_PORT, normalize_peer_name
from tcp_handler import (
    FileSendResult,
    close_chat_connections,
    drain_tcp_workers,
    get_or_connect,
    send_chat_message,
    start_file_send,
    start_tcp_threads,
    stop_file_sender,
)
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
    try:
        if is_current:
            print_chat(peer_name, content)
        else:
            preview = content if len(content) <= 45 else content[:45] + "..."
            print_system(f"💬 Tin nhắn mới từ {peer_name}: {preview}")
    except Exception:
        logger.exception("Không thể hiển thị tin nhắn từ %s.", peer_key)


def on_tcp_system(message: str):
    """Hiển thị các thông báo hệ thống và sự kiện mạng."""
    try:
        print_system(message)
    except Exception:
        logger.exception("Không thể hiển thị thông báo mạng.")


# ---------------------------------------------------------------------------
# Hàm hỗ trợ hiển thị tiến độ gửi file
# ---------------------------------------------------------------------------

def _make_send_file_progress(filename: str):
    """
    Tạo callback cập nhật tiến độ gửi file ra màn hình (0%, 25%, 50%, 75%, 100%).
    """
    last_milestone = [-1]

    def _progress(sent: int, total: int):
        pct = sent * 100 // total if total else 100
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

def _find_peer(selector: str):
    if not selector:
        print_warning("Cú pháp: /chat <tên|số> (Gõ /list để xem số)")
        return None, None
    peers = state.get_peer_list_snapshot()
    if selector.isascii() and selector.isdecimal():
        with state.lock:
            listed = state.listed_peer_keys
        try:
            index = int(selector) - 1
        except ValueError:
            index = -1
        if 0 <= index < len(listed) and listed[index] in peers:
            return listed[index], peers[listed[index]]
        print_warning("Số peer không hợp lệ hoặc peer đã offline. Gõ /list để cập nhật.")
        return None, None

    name = selector.casefold()
    matches = [(key, peer) for key, peer in peers.items() if peer.name.casefold() == name]
    if len(matches) == 1:
        return matches[0]
    if matches:
        print_warning("Có nhiều peer trùng tên. Gõ /list và chọn số của peer.")
    else:
        print_warning(f"Không tìm thấy người dùng '{selector}'. Gõ /list để xem danh sách online.")
    return None, None


def _send_chat_text(content: str):
    with state.lock:
        peer_key = state.current_partner
    if peer_key is None:
        print_warning("Chưa chọn đối tác. Dùng /chat <tên|số> hoặc /help.")
        return
    try:
        sent = send_chat_message(peer_key, content)
    except ValueError as e:
        print_warning(str(e))
        return
    if not sent:
        print_error("Gửi tin nhắn thất bại. Kết nối đã bị gián đoạn.")
        return

    timestamp = time.strftime("%H:%M:%S")
    log_entry = f"[{timestamp}] {state.my_name} (bạn): {content}"
    with state.lock:
        state.chat_history.setdefault(peer_key, []).append(log_entry)
    print_chat(f"{state.my_name} (bạn)", content, is_self=True)


def _open_chat(peer_key: str, peer: Peer) -> bool:
    print_info(f"Đang kết nối tới {peer.name} ({peer.ip}:{peer.tcp_port})...")
    if get_or_connect(peer_key) is None:
        print_error(f"Không thể kết nối TCP tới {peer.name}. Đối tác có thể vừa offline hoặc bị chặn tường lửa.")
        return False

    with state.conn_lock:
        with state.lock:
            connected = peer_key in state.connections and peer_key in state.peer_list
            if connected:
                state.current_partner = peer_key
                state.reconnect_partner = peer_key
    if not connected:
        print_error(f"Peer {peer.name} không còn khả dụng.")
        return False

    print_success(f"Đã kết nối với {peer.name}. Nhập nội dung để chat, /send để gửi file, /leave để thoát.")
    display_chat_history(peer_key)
    return True


# ---------------------------------------------------------------------------
# Xử lý lệnh người dùng (Command Handler)
# ---------------------------------------------------------------------------

def _start_command_worker(name: str, capacity: int):
    jobs: queue.Queue[Callable[[], None]] = queue.Queue(maxsize=capacity)

    def enqueue(job: Callable[[], None]) -> bool:
        if state.stop_event.is_set():
            return False
        try:
            jobs.put_nowait(job)
        except queue.Full:
            return False
        return True

    def run():
        while not state.stop_event.is_set():
            try:
                job = jobs.get(timeout=0.1)
            except queue.Empty:
                continue
            if state.stop_event.is_set():
                break
            try:
                job()
            except Exception:
                logger.exception("Lỗi khi xử lý lệnh trên %s.", name)

    worker = threading.Thread(target=run, name=name, daemon=True)
    worker.start()
    return worker, enqueue


def _start_chat_worker():
    worker, enqueue = _start_command_worker("Chat-Commands", 64)
    pending_selection: Future[bool] | None = None

    def submit(command: str, job: Callable[[], bool]) -> bool:
        nonlocal pending_selection
        selection = Future() if command == "/chat" else None
        depends_on = pending_selection if command in ("", "/send", "/file") else None
        if depends_on is not None and depends_on.done():
            depends_on = None

        def execute():
            succeeded = False
            try:
                if depends_on is not None and not depends_on.result():
                    print_warning("Chưa kết nối được peer đã chọn. Lệnh gửi đã bị bỏ qua.")
                else:
                    succeeded = job()
            finally:
                if selection is not None:
                    selection.set_result(succeeded)

        if not enqueue(execute):
            return False
        if selection is not None or command == "/leave":
            pending_selection = selection
        return True

    return worker, submit


BROADCAST_PREFIX = "[Broadcast] "


def _message_fits_queue(text: str, prefix: str = "") -> bool:
    limit = MAX_MSG_LEN - len(prefix)
    if len(text) <= limit // 4:
        return True
    if len(text) <= limit and len(text.encode("utf-8", errors="replace")) <= limit:
        return True
    print_warning(f"Tin nhắn vượt quá {MAX_MSG_LEN} bytes UTF-8.")
    return False


def _broadcast_text(content: str):
    peers = state.get_peer_list_snapshot()
    if not peers:
        print_warning("Hiện không có ai online trong mạng LAN để gửi broadcast.")
        return

    print_info(f"Đang gửi thông báo tới {len(peers)} người dùng...")
    success_count = 0
    timestamp = time.strftime("%H:%M:%S")
    for key in peers:
        if state.stop_event.is_set():
            return
        try:
            sent = send_chat_message(key, BROADCAST_PREFIX + content)
        except ValueError as e:
            print_warning(str(e))
            return
        if sent:
            success_count += 1
            log_msg = f"[{timestamp}] {state.my_name} -> broadcast: {content}"
            with state.lock:
                state.chat_history.setdefault(key, []).append(log_msg)

    print_success(f"Đã gửi thông báo thành công tới {success_count}/{len(peers)} người dùng.")


def handle_command(
    cmd_raw: str,
    submit_chat: Callable[[str, Callable[[], bool]], bool] | None = None,
    submit_broadcast: Callable[[Callable[[], None]], bool] | None = None,
) -> bool:
    """
    Xử lý lệnh từ người dùng.
    Trả về True nếu cần thoát chương trình (/quit).
    """
    cmd = cmd_raw.strip()
    if not cmd:
        return False

    parts = cmd.split(maxsplit=1)
    command = parts[0].lower() if cmd.startswith("/") else ""
    args = parts[1].strip() if len(parts) > 1 else ""
    if command == "/chat":
        peer_key, peer = _find_peer(args)
        if peer is not None:
            if submit_chat is None:
                _open_chat(peer_key, peer)
            elif not submit_chat(command, partial(_open_chat, peer_key, peer)):
                print_warning("Hàng đợi chat đang đầy. Vui lòng thử lại.")
        return False

    if submit_chat is not None and (
        command in ("", "/send", "/file", "/leave", "/history")
    ):
        if not command and not _message_fits_queue(cmd_raw):
            return False
        if not submit_chat(command, partial(handle_command, cmd_raw)):
            print_warning("Hàng đợi chat đang đầy. Vui lòng thử lại.")
        return False

    if not cmd.startswith("/"):
        _send_chat_text(cmd_raw)
        return False

    # ------------------ /list ------------------
    if command == "/list":
        peers = state.get_peer_list_snapshot()
        with state.lock:
            state.listed_peer_keys = tuple(peers)
        display_peer_list(peers)

    # ------------------ /status ------------------
    elif command == "/status":
        if args not in ("online", "available", "busy", "away"):
            print_warning("Cú pháp: /status <online|away>")
        else:
            state.set_status("available" if args == "online" else args)
            print_success(f"Đã cập nhật trạng thái cá nhân: '{args}'.")

    # ------------------ /history ------------------
    elif command == "/history":
        with state.lock:
            partner_key = state.current_partner
        if partner_key is None:
            print_warning("Chưa chọn đối tác. Vui lòng dùng /chat <tên|số> trước.")
        else:
            display_chat_history(partner_key)

    # ------------------ /leave ------------------
    elif command == "/leave":
        with state.lock:
            partner_key = state.current_partner or state.reconnect_partner
            p = state.peer_list.get(partner_key)
            name = p.name if p else partner_key
            state.current_partner = None
            state.reconnect_partner = None
        if partner_key is None:
            print_warning("Hiện tại bạn không trong phiên chat nào.")
            return False

        print_info(f"Đã rời cuộc trò chuyện với {name}. (Kết nối TCP vẫn được giữ để tái sử dụng).")

    # ------------------ /broadcast ------------------
    elif command == "/broadcast":
        if not args:
            print_warning("Cú pháp: /broadcast <nội_dung_thông_báo>")
        elif not _message_fits_queue(args, BROADCAST_PREFIX):
            return False
        elif submit_broadcast is None:
            _broadcast_text(args)
        elif not submit_broadcast(partial(_broadcast_text, args)):
            print_warning("Hàng đợi broadcast đang đầy. Vui lòng thử lại.")

    # ------------------ /send (/file alias) ------------------
    elif command in ("/send", "/file"):
        with state.lock:
            peer_key = state.current_partner
            peer = state.peer_list.get(peer_key) if peer_key else None
            peer_name = peer.name if peer else peer_key
        if not peer_key:
            print_warning("Chưa chọn người nhận file. Vui lòng dùng /chat <tên|số> trước.")
            return False

        if not args:
            print_warning("Cú pháp: /send <đường_dẫn_tệp> (Nhấn Tab để duyệt file)")
            return False

        # Kiểm tra an toàn: File có tồn tại hay không?
        path = args[1:-1] if len(args) >= 2 and args[0] == args[-1] and args[0] in ("'", '"') else args
        clean_path = os.path.expanduser(path)
        if not os.path.isfile(clean_path):
            print_warning(f"Tệp tin không tồn tại hoặc không phải file hợp lệ: '{clean_path}'")
            return False

        filename = os.path.basename(clean_path)
        progress_cb = _make_send_file_progress(filename)

        def report_result(result: FileSendResult):
            if result is FileSendResult.SAVED:
                print_success(f"{peer_name} đã xác nhận lưu file '{filename}'.")
            elif result is FileSendResult.UNCONFIRMED:
                print_warning(f"Đã gửi dữ liệu file '{filename}', chưa có xác nhận lưu từ {peer_name}.")
            else:
                print_error(f"Gửi file '{filename}' tới {peer_name} thất bại.")

        if not start_file_send(peer_key, clean_path, progress_cb, report_result):
            print_warning("Đang có lượt gửi file. Vui lòng thử lại khi lượt đó kết thúc.")

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

def _shutdown_workers(workers: list[threading.Thread], finish_history: Callable[[float], bool | None] | None):
    state.stop_event.set()
    deadline = time.monotonic() + 2.0

    def remaining():
        return max(0.0, deadline - time.monotonic())

    close_chat_connections()
    file_stopped = stop_file_sender(timeout=remaining())
    tcp_drained = drain_tcp_workers(timeout=remaining())
    for worker in workers:
        worker.join(timeout=remaining())

    if not file_stopped:
        print_warning("File sender chưa dừng trong thời gian chờ.")
    if not tcp_drained:
        print_warning("Một số TCP workers chưa dừng trong thời gian chờ.")
    for worker in workers:
        if worker.is_alive():
            print_warning(f"Worker {worker.name} chưa dừng trong thời gian chờ.")
    if finish_history is not None:
        saved = finish_history(2.0)
        if saved is None:
            print_warning("Hết thời gian chờ lưu lịch sử chat.")
        elif not saved:
            print_warning("Không thể lưu lịch sử chat.")


def main():
    parser = argparse.ArgumentParser(
        description="LAN Chat & File Sharing P2P (Hệ thống mạng đồng đẳng)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--name", required=True, help="Tên hiển thị của bạn (vd: Quan)")
    parser.add_argument("--port", required=True, type=int, help="Cổng TCP lắng nghe (vd: 5001)")
    parser.add_argument("--data-dir", type=Path, default=Path("."), help="Thư mục history và downloads")
    parser.add_argument(
        "--download-quota-gib", type=int, default=DEFAULT_DOWNLOAD_QUOTA_GIB,
        help="Quota downloads tính bằng GiB (mặc định: %(default)s)",
    )
    parser.add_argument("--debug", action="store_true", help="Bật chế độ ghi log chi tiết")
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("--port phải nằm trong khoảng 1–65535.")
    if args.download_quota_gib <= 0:
        parser.error("--download-quota-gib phải là số nguyên dương.")
    state.download_quota_bytes = args.download_quota_gib * GIB
    try:
        state.data_dir = args.data_dir.expanduser().resolve()
        state.data_dir.mkdir(parents=True, exist_ok=True)
    except (OSError, RuntimeError) as e:
        parser.error(f"Không thể mở --data-dir: {e}")

    # 1. Khởi tạo trạng thái danh tính cá nhân
    state.my_name = normalize_peer_name(args.name)
    state.my_tcp_port = args.port
    state.my_ip = get_local_ip()

    # 2. Tải lịch sử chat đã lưu từ phiên trước
    loaded_msgs = load_history()

    # 3. Tạo session giao diện điều khiển với prompt_toolkit
    session = create_prompt_session()

    def notify_system(message: str):
        on_tcp_system(message)
        session.app.invalidate()

    with patch_stdout():
        logging.basicConfig(
            level=logging.DEBUG if args.debug else logging.WARNING,
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%H:%M:%S",
        )
        print_banner()
        print_info(f"Danh tính: {state.my_name} | IP: {state.my_ip} | Cổng TCP: {state.my_tcp_port}")
        if loaded_msgs > 0:
            print_info(f"Đã nạp {loaded_msgs} tin nhắn từ tệp lịch sử.")
        print_help()

        # Chỉ quảng bá sau khi TCP listener đã bind thành công.
        try:
            t_tcp, t_reconnect = start_tcp_threads(
                msg_callback=on_tcp_message,
                system_callback=notify_system,
            )
        except (OSError, RuntimeError) as e:
            print_error(f"Không thể mở TCP port {state.my_tcp_port}: {e}")
            return
        print_success(f"Máy chủ TCP đang lắng nghe trên cổng {state.my_tcp_port}.")

        workers = [t_tcp, t_reconnect]
        finish_history = None
        try:
            try:
                workers.extend(start_udp_threads(notify_fn=notify_system))
            except (OSError, RuntimeError) as e:
                print_error(f"Không thể mở UDP discovery port {UDP_PORT}: {e}")
            else:
                print_success("Hệ thống UDP Discovery đã sẵn sàng.")

            _, finish_history = start_autosave_thread()
            t_chat, submit_chat = _start_chat_worker()
            workers.append(t_chat)
            t_broadcast, submit_broadcast = _start_command_worker("Chat-Broadcast", 8)
            workers.append(t_broadcast)
            print_info("Gõ /list để xem danh sách online, /chat <tên|số> để trò chuyện. Nhấn Tab để gợi ý lệnh.\n")

            while not state.stop_event.is_set():
                try:
                    user_input = session.prompt(get_prompt_text)
                    if handle_command(user_input, submit_chat, submit_broadcast):
                        break
                except EOFError:
                    break
                except Exception as e:
                    print_warning(f"Đã xảy ra lỗi giao diện: {e}")
        except KeyboardInterrupt:
            print_info("\nĐã nhận tín hiệu ngắt (Ctrl+C). Đang đóng ứng dụng...")
        except RuntimeError as e:
            print_error(f"Không thể khởi động worker: {e}")
        finally:
            _shutdown_workers(workers, finish_history)


if __name__ == "__main__":
    main()
