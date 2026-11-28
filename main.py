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
import threading
import time
from collections import deque
from concurrent.futures import Future
from contextlib import ExitStack, contextmanager
from functools import partial
from pathlib import Path
from typing import Callable

from prompt_toolkit.patch_stdout import patch_stdout

from data_dir_lock import lock_data_directory
from history import load_history, start_autosave_thread
from models import DEFAULT_DOWNLOAD_QUOTA_GIB, GIB, Peer, state
from protocol import MAX_MSG_LEN, MAX_UDP_DATAGRAM, PRESENCE_STATUSES, UDP_PORT, encode_hello, normalize_peer_name
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

def on_tcp_message(peer_key: str, peer_name: str, content: str, unread_count: int | None):
    """
    Hiển thị tin nhắn nhận được.
    patch_stdout đảm bảo tin nhắn không đè lên dòng lệnh đang gõ.
    """
    try:
        if unread_count is None:
            print_chat(peer_name, content)
        else:
            preview = content if len(content) <= 45 else content[:45] + "..."
            print_system(f"💬 Tin nhắn mới từ {peer_name}: {preview} ({unread_count} chưa đọc)")
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
        return None, None, "Cú pháp: /chat <tên|số> (Gõ /list để xem số)"
    peers = state.get_peer_list_snapshot()
    if selector.isascii() and selector.isdecimal():
        with state.lock:
            listed = state.listed_peer_keys
        try:
            index = int(selector) - 1
        except ValueError:
            index = -1
        if 0 <= index < len(listed) and listed[index] in peers:
            return listed[index], peers[listed[index]], None
        return None, None, "Số peer không hợp lệ hoặc peer đã offline. Gõ /list để cập nhật."

    name = selector.casefold()
    matches = [(key, peer) for key, peer in peers.items() if peer.name.casefold() == name]
    if len(matches) == 1:
        key, peer = matches[0]
        return key, peer, None
    if matches:
        return None, None, "Có nhiều peer trùng tên. Gõ /list và chọn số của peer."
    return None, None, f"Không tìm thấy người dùng '{selector}'. Gõ /list để xem danh sách online."


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


def _open_chat(
    peer_key: str | None, peer: Peer | None, lookup_error: str | None,
    *, can_activate: Callable[[], bool] | None = None,
) -> bool:
    with state.lock:
        if can_activate is not None and not can_activate():
            return False
        state.current_partner = None
        state.reconnect_partner = None

    if peer_key is None or peer is None:
        print_warning(lookup_error or "Không thể chọn peer.")
        return False

    print_info(f"Đang kết nối tới {peer.name} ({peer.ip}:{peer.tcp_port})...")
    if get_or_connect(peer_key) is None:
        print_error(f"Không thể kết nối TCP tới {peer.name}. Đối tác có thể vừa offline hoặc bị chặn tường lửa.")
        return False

    with state.conn_lock:
        with state.lock:
            if can_activate is not None and not can_activate():
                return False
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

CHAT_QUEUE_BYTES = 32 * 1024**2
BROADCAST_QUEUE_BYTES = 16 * 1024**2


class _CommandQueue:
    def __init__(self, name: str, capacity: int, payload_budget: int, stop_event: threading.Event):
        self.name = name
        self._capacity = capacity
        self._payload_budget = payload_budget
        self._stop_event = stop_event
        self._jobs: deque[tuple[Callable[[], None], int]] = deque()
        self._in_flight_bytes = 0
        self._ready = threading.Condition()

    @property
    def _reserved_bytes(self) -> int:
        return self._in_flight_bytes + sum(size for _, size in self._jobs)

    def enqueue(self, job: Callable[[], None], payload_bytes: int) -> bool:
        with self._ready:
            if (
                self._stop_event.is_set() or len(self._jobs) >= self._capacity
                or self._reserved_bytes + payload_bytes > self._payload_budget
            ):
                return False
            self._jobs.append((job, payload_bytes))
            self._ready.notify()
            return True

    def cancel_pending(self):
        # The worker owns any job it has already taken from the queue.
        with self._ready:
            count = len(self._jobs)
            self._jobs.clear()
            self._ready.notify_all()
        if count:
            print_warning(f"Đã hủy {count} lệnh chưa chạy trên {self.name} khi đóng ứng dụng.")

    def run(self):
        try:
            while True:
                with self._ready:
                    self._ready.wait_for(lambda: self._jobs or self._stop_event.is_set(), timeout=0.1)
                    if self._stop_event.is_set():
                        return
                    if not self._jobs:
                        continue
                    job, payload_bytes = self._jobs.popleft()
                    self._in_flight_bytes = payload_bytes
                try:
                    job()
                except Exception:
                    logger.exception("Lỗi khi xử lý lệnh trên %s.", self.name)
                finally:
                    del job
                    with self._ready:
                        self._in_flight_bytes = 0
        finally:
            self.cancel_pending()


def _start_command_worker(name: str, capacity: int, payload_budget: int):
    commands = _CommandQueue(name, capacity, payload_budget, state.stop_event)
    worker = threading.Thread(target=commands.run, name=name, daemon=True)
    worker.start()
    return worker, commands.enqueue, commands.cancel_pending


def _start_chat_worker():
    worker, enqueue, cancel_pending = _start_command_worker("Chat-Commands", 64, CHAT_QUEUE_BYTES)
    latest_selection: Future[bool] | None = None
    rejected_selection_version = 0

    def submit(command: str, job: Callable[..., bool], payload_bytes: int) -> bool:
        nonlocal latest_selection, rejected_selection_version
        selection = Future() if command == "/chat" else None
        depends_on = latest_selection if command in ("", "/send", "/file") else None
        version = rejected_selection_version

        def can_activate():
            # Called under state.lock when the worker changes the active peer.
            return version == rejected_selection_version

        def execute():
            if selection is None:
                if depends_on is not None and not depends_on.result():
                    print_warning("Lệnh gửi đã bị bỏ qua. Dùng /chat để chọn lại peer hoặc /leave để rời chat.")
                else:
                    job()
                return

            succeeded = False
            try:
                succeeded = job(can_activate=can_activate)
            finally:
                if not succeeded:
                    with state.lock:
                        state.current_partner = None
                        state.reconnect_partner = None
                selection.set_result(succeeded)

        if not enqueue(execute, payload_bytes):
            if selection is not None:
                with state.lock:
                    rejected_selection_version += 1
                    state.current_partner = None
                    state.reconnect_partner = None
                selection.set_result(False)
                latest_selection = selection
            return False
        if selection is not None or command == "/leave":
            latest_selection = selection
        return True

    return worker, submit, cancel_pending


BROADCAST_PREFIX = "[Broadcast] "


def _message_payload_size(text: str, prefix_bytes: int = 0) -> int | None:
    if len(text) + prefix_bytes <= MAX_MSG_LEN:
        size = len(text) if text.isascii() else len(text.encode("utf-8", errors="replace"))
        if size + prefix_bytes <= MAX_MSG_LEN:
            return size + prefix_bytes
    print_warning(f"Tin nhắn vượt quá {MAX_MSG_LEN} bytes UTF-8.")
    return None


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
            print_warning(f"Đã dừng broadcast khi đóng ứng dụng. Đã gửi tới {success_count}/{len(peers)} người dùng.")
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
    submit_chat: Callable[[str, Callable[..., bool], int], bool] | None = None,
    submit_broadcast: Callable[[Callable[[], None], int], bool] | None = None,
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
        selection = partial(_open_chat, *_find_peer(args))
        if submit_chat is None:
            selection()
        elif not submit_chat(command, selection, 0):
            print_warning("Hàng đợi chat đang đầy. Dùng /chat để chọn lại peer hoặc /leave để rời chat.")
        return False

    if submit_chat is not None and (
        command in ("", "/send", "/file", "/leave", "/history")
    ):
        payload_bytes = _message_payload_size(cmd_raw) if not command else 0
        if payload_bytes is None:
            return False
        if not submit_chat(command, partial(handle_command, cmd_raw), payload_bytes):
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
        status = "available" if args == "online" else args
        if status not in PRESENCE_STATUSES:
            print_warning("Cú pháp: /status <online|away>")
        else:
            state.set_status(status)
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
            return False
        payload_bytes = _message_payload_size(args, len(BROADCAST_PREFIX))
        if payload_bytes is None:
            return False
        if submit_broadcast is None:
            _broadcast_text(args)
        elif not submit_broadcast(partial(_broadcast_text, args), payload_bytes):
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

def _shutdown_workers(
    workers: list[threading.Thread],
    finish_history: Callable[[float], bool | None] | None,
    cancel_queued: list[Callable[[], None]],
):
    state.stop_event.set()
    deadline = time.monotonic() + 2.0

    def remaining():
        return max(0.0, deadline - time.monotonic())

    for cancel in cancel_queued:
        cancel()
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


@contextmanager
def _interactive_console(parser: argparse.ArgumentParser):
    console_error = (
        "lan_chat: an interactive Windows console is required. "
        "Run from Windows Terminal or PowerShell without redirecting stdin or stdout.\n"
    )
    console_errors: tuple[type[Exception], ...] = ()
    if sys.platform == "win32":
        if any(stream is None or not stream.isatty() for stream in (sys.stdin, sys.stdout)):
            parser.exit(2, console_error)

        from prompt_toolkit.output.win32 import NoConsoleScreenBufferError

        console_errors = (NoConsoleScreenBufferError, OSError)

    with ExitStack() as console:
        try:
            session = create_prompt_session()
            console.enter_context(patch_stdout())
        except console_errors:
            parser.exit(2, console_error)
        yield session


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

    name = normalize_peer_name(args.name)
    if any(
        len(encode_hello(name, args.port, status, instance_id)) > MAX_UDP_DATAGRAM
        for status in PRESENCE_STATUSES
        for instance_id in (None, state.instance_id)
    ):
        parser.error(f"--name quá dài. Gói UDP discovery sau khi mã hóa UTF-8 tối đa {MAX_UDP_DATAGRAM} byte.")

    with _interactive_console(parser) as session:
        state.download_quota_bytes = args.download_quota_gib * GIB
        try:
            state.data_dir = args.data_dir.expanduser().resolve()
            state.data_dir.mkdir(parents=True, exist_ok=True)
        except (OSError, RuntimeError) as e:
            parser.error(f"Không thể mở --data-dir: {e}")

        try:
            lock_data_directory(state.data_dir)
        except OSError as e:
            parser.error(f"Không thể khóa --data-dir '{state.data_dir}'. {e}")

        # 1. Khởi tạo trạng thái danh tính cá nhân
        state.my_name = name
        state.my_tcp_port = args.port
        state.my_ip = get_local_ip()

        # 2. Tải lịch sử chat đã lưu từ phiên trước
        loaded_msgs = load_history()

        def notify_system(message: str):
            on_tcp_system(message)
            session.app.invalidate()

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
            raise SystemExit(1) from None
        print_success(f"Máy chủ TCP đang lắng nghe trên cổng {state.my_tcp_port}.")

        workers = [t_tcp, t_reconnect]
        cancel_queued = []
        finish_history = None
        try:
            try:
                workers.extend(start_udp_threads(notify_fn=notify_system))
            except (OSError, RuntimeError) as e:
                print_error(f"Không thể mở UDP discovery port {UDP_PORT}: {e}")
            else:
                print_success("Hệ thống UDP Discovery đã sẵn sàng.")

            _, finish_history = start_autosave_thread()
            t_chat, submit_chat, cancel_chat = _start_chat_worker()
            workers.append(t_chat)
            cancel_queued.append(cancel_chat)
            t_broadcast, submit_broadcast, cancel_broadcast = _start_command_worker(
                "Chat-Broadcast", 8, BROADCAST_QUEUE_BYTES,
            )
            workers.append(t_broadcast)
            cancel_queued.append(cancel_broadcast)
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
            _shutdown_workers(workers, finish_history, cancel_queued)


if __name__ == "__main__":
    main()
