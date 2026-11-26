"""
tcp_handler.py - Xử lý kết nối TCP (Chat 1-1 và Truyền file nhị phân)

Các tính năng kỹ thuật nâng cao:
1. Đa nền tảng & Ổn định (Robustness):
   - Bắt toàn bộ ngoại lệ ngắt kết nối đột ngột: ConnectionResetError, ConnectionAbortedError,
     BrokenPipeError, ConnectionError, OSError.
   - Thông báo rõ ràng: "[*] Kết nối với <Peer> đã bị ngắt."
   - Dọn dẹp trạng thái kết nối và đưa giao diện về dấu nhắc chính tự động.

2. Chống crash do gói tin rác (Malformed Packets / DoS protection):
   - Đọc header có giới hạn kích thước (MAX_FIELD_LEN = 4096 bytes).
   - Kiểm tra tính hợp lệ của framing (MSG|, FILE|, HELLO|), parse số nguyên an toàn.
   - Đóng ngay kết nối bất thường mà không làm ảnh hưởng đến tiến trình chính.

3. Truyền file nhị phân mọi định dạng (.part & streaming):
   - Hỗ trợ toàn bộ định dạng: .txt, .pdf, .docx, .pptx, .xlsx, .jpg, .png, .mp3, .mp4, .zip, .rar,...
   - Đọc/ghi nhị phân (rb/wb) theo từng chunk 64KB, tối ưu bộ nhớ RAM.
   - Sử dụng os.path.basename loại bỏ tấn công Directory Traversal.
   - Ghi dữ liệu vào file .part riêng cho mỗi lượt nhận và cập nhật tiến độ %.
   - Khi nhận đủ dữ liệu, publish file hoàn chỉnh và gửi kết quả cho bên gửi.
   - Nếu đứt kết nối giữa chừng: Tự động xóa file .part dở dang để tránh rác ổ đĩa.
"""

import errno
import logging
import math
import os
import select
import shutil
import socket
import stat
import struct
import sys
import tempfile
import threading
import time
from enum import Enum, auto
from functools import partial
from itertools import count
from typing import Callable, Optional, Tuple, Any

from models import ChatConnection, state
from protocol import (
    encode_msg,
    encode_file_ack,
    encode_file_header,
    DELIMITER,
    MAX_FIELD_LEN,
    MAX_MSG_LEN,
    MSG_TYPE_HELLO,
    MSG_TYPE_MSG,
    MSG_TYPE_FILE,
    MSG_TYPE_BYE,
    MSG_TYPE_FILE_ACK,
    FILE_ACK_VERSION,
    normalize_peer_name,
)

logger = logging.getLogger(__name__)

# Kích thước mỗi chunk khi gửi/nhận file (64 KB)
CHUNK_SIZE = 65536
CHAT_CONNECT_TIMEOUT = 5.0
CHAT_WRITE_TIMEOUT = 10.0
CHAT_HEADER_TIMEOUT = 10.0
CHAT_PAYLOAD_IDLE_TIMEOUT = 10.0
CHAT_PAYLOAD_TOTAL_TIMEOUT = 60.0
FILE_IO_TIMEOUT = 10.0
HANDSHAKE_TIMEOUT = 10.0
MAX_INCOMING_WORKERS = 32
MAX_FILE_SIZE = 2 * 1024**3

# Thư mục lưu file nhận được
DOWNLOADS_DIR = "downloads"

# Callbacks hiển thị lên giao diện console
_msg_cb: Optional[Callable[[str, str, str, bool], None]] = None
_system_cb: Optional[Callable[[str], None]] = None
_file_sender_lock = threading.Lock()
_file_sender: Optional[threading.Thread] = None
_file_sender_socket: Optional[socket.socket] = None
_download_lock = threading.Lock()
_download_reservations: dict[str, dict[str, int]] = {}
_CONNECT_IN_PROGRESS = {
    errno.EINPROGRESS, errno.EWOULDBLOCK, errno.EALREADY, errno.EINTR,
    getattr(errno, "WSAEWOULDBLOCK", errno.EWOULDBLOCK),
}


# ---------------------------------------------------------------------------
# Các hàm đọc dữ liệu Socket mức thấp có bảo vệ (Low-level Helpers)
# ---------------------------------------------------------------------------

def _read_field(recv: Callable[[int], bytes]) -> str:
    """Read one UTF-8 field without consuming any bytes of the next field."""
    buf = bytearray()
    while True:
        ch = recv(1)
        if not ch:
            raise ConnectionResetError("Đối tác đã đóng kết nối đột ngột.")

        if ch == b"|":
            try:
                return buf.decode("utf-8")
            except UnicodeDecodeError as e:
                raise ValueError(f"Dữ liệu header không phải UTF-8 hợp lệ: {e}")

        buf += ch
        if len(buf) > MAX_FIELD_LEN:
            raise ValueError(f"Độ dài trường header vượt quá giới hạn an toàn ({MAX_FIELD_LEN} bytes).")


def _recv_field(sock: socket.socket, deadline: float) -> str:
    """Read a field during handshake or FILE_ACK, before sharing the socket."""
    def recv(size: int) -> bytes:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("Hết thời gian đọc header TCP.")
        sock.settimeout(remaining)
        return sock.recv(size)

    return _read_field(recv)


def _recv_chat_before(sock: socket.socket, size: int, deadline: float) -> bytes:
    # CHAT has one reader and a concurrent writer. Keep the socket mode unchanged.
    remaining = deadline - time.monotonic()
    if remaining > 0 and select.select([sock], [], [], remaining)[0]:
        chunk = sock.recv(size)
        if time.monotonic() < deadline:
            return chunk
    raise TimeoutError("Hết thời gian nhận frame CHAT.")


def _recv_chat_header(sock: socket.socket) -> tuple[str, int]:
    deadline = None

    def recv(size: int) -> bytes:
        nonlocal deadline
        if deadline is None:
            # An established connection can stay idle between frames.
            first_byte = sock.recv(size)
            deadline = time.monotonic() + CHAT_HEADER_TIMEOUT
            return first_byte
        return _recv_chat_before(sock, size, deadline)

    msg_type = _read_field(recv)
    if msg_type != MSG_TYPE_MSG:
        return msg_type, 0

    length_str = _read_field(recv)
    try:
        length = int(length_str)
        if not 0 <= length <= MAX_MSG_LEN:
            raise ValueError(f"Độ dài tin nhắn không hợp lệ: {length}")
    except ValueError as e:
        raise ValueError(f"Header MSG sai cú pháp: {e}") from e
    return msg_type, length


def _recv_chat_payload(sock: socket.socket, n: int) -> bytes:
    started = time.monotonic()
    total_deadline = started + CHAT_PAYLOAD_TOTAL_TIMEOUT
    idle_deadline = started + CHAT_PAYLOAD_IDLE_TIMEOUT
    buf = bytearray()
    while len(buf) < n:
        chunk = _recv_chat_before(sock, min(n - len(buf), CHUNK_SIZE), min(total_deadline, idle_deadline))
        if not chunk:
            raise ConnectionResetError(f"Kết nối bị đứt: mong đợi {n} bytes nhưng chỉ nhận được {len(buf)} bytes.")
        buf += chunk
        idle_deadline = time.monotonic() + CHAT_PAYLOAD_IDLE_TIMEOUT
    return bytes(buf)


def _send_chat_frame(sock: socket.socket, data: bytes, deadline: float):
    pending = memoryview(data)
    while pending:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("Hết thời gian gửi tin nhắn TCP.")

        # A send-only timeout leaves the receiver's blocking reads unchanged.
        if sys.platform == "win32":
            timeout = struct.pack("@I", math.ceil(remaining * 1000))
        else:
            seconds, microseconds = divmod(math.ceil(remaining * 1_000_000), 1_000_000)
            fmt = "@li0l" if sys.platform == "darwin" else "@ll"
            timeout = struct.pack(fmt, seconds, microseconds)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDTIMEO, timeout)

        try:
            sent = sock.send(pending)
        except BlockingIOError as e:
            raise TimeoutError("Hết thời gian gửi tin nhắn TCP.") from e
        if not sent:
            raise ConnectionResetError("Socket đóng trong lúc gửi tin nhắn TCP.")
        pending = pending[sent:]


def _connect_socket(sock: socket.socket, endpoint: tuple[str, int], timeout: float):
    if state.stop_event.is_set():
        raise ConnectionAbortedError("Đang đóng ứng dụng.")
    deadline = time.monotonic() + timeout
    sock.setblocking(False)
    error = sock.connect_ex(endpoint)
    while error in _CONNECT_IN_PROGRESS and not state.stop_event.is_set():
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("Hết thời gian kết nối TCP.")
        _, writable, failed = select.select([], [sock], [sock], min(remaining, 0.1))
        if writable or failed:
            error = sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)

    if state.stop_event.is_set():
        raise ConnectionAbortedError("Đang đóng ứng dụng.")
    if error:
        raise OSError(error, os.strerror(error))
    sock.settimeout(timeout)


def _shutdown_socket(sock: socket.socket):
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass


def _close_chat_socket(sock: socket.socket):
    _shutdown_socket(sock)
    try:
        sock.close()
    except OSError:
        pass


def _register_chat_connection(
    peer_key: str, peer_ip: str, peer_port: int, candidate: ChatConnection
) -> Optional[ChatConnection]:
    # At both ends, prefer the connection initiated by the lower listening endpoint.
    local_endpoint = (candidate.sock.getsockname()[0], state.my_tcp_port)
    prefer_outgoing = local_endpoint < (peer_ip, peer_port)

    with state.conn_lock:
        existing = state.connections.get(peer_key)
        if state.stop_event.is_set():
            winner = None
        elif existing is None:
            winner = candidate
        elif existing.outgoing != candidate.outgoing:
            winner = candidate if candidate.outgoing == prefer_outgoing else existing
        else:
            winner = existing if candidate.outgoing else candidate

        if winner is candidate:
            state.connections[peer_key] = candidate

    if winner is not candidate:
        _close_chat_socket(candidate.sock)
    elif existing is not None:
        _close_chat_socket(existing.sock)
    return winner


def _cleanup_connection(peer_key: str, sock: socket.socket) -> bool:
    """Close this socket and clear the session only if it still owns the entry."""
    with state.conn_lock:
        current = state.connections.get(peer_key)
        removed = current is not None and current.sock is sock
        if removed:
            del state.connections[peer_key]
            state.reset_partner_if(peer_key)
    if removed:
        with state.lock:
            peer = state.peer_list.get(peer_key)
            if peer is not None and peer.via_tcp and not peer.is_static:
                peer.last_seen = time.monotonic()
    _close_chat_socket(sock)
    return removed


def close_peer_connection(peer_key: str):
    """Đóng kết nối TCP đang mở với một peer cụ thể (nếu có)."""
    with state.conn_lock:
        conn = state.connections.pop(peer_key, None)
    if conn is not None:
        _close_chat_socket(conn.sock)


def close_chat_connections():
    with state.conn_lock:
        sockets = {conn.sock for conn in state.connections.values()}
        dialing = state.dialing_chat_sockets - sockets
        state.connections.clear()
        for ready in state.pending_chat_dials.values():
            ready.set()
    for sock in sockets:
        _close_chat_socket(sock)
    for sock in dialing:
        _shutdown_socket(sock)


def _start_tcp_worker(sock, handler, args, *, incoming=False) -> bool:
    def run():
        try:
            handler(sock, *args)
        finally:
            try:
                sock.close()
            finally:
                with state.conn_lock:
                    state.tcp_workers.pop(sock, None)

    with state.conn_lock:
        if state.stop_event.is_set():
            return False
        if incoming and sum(is_incoming for _, is_incoming in state.tcp_workers.values()) >= MAX_INCOMING_WORKERS:
            return False
        worker = threading.Thread(target=run, name="TCP-Incoming" if incoming else "TCP-Recv", daemon=True)
        state.tcp_workers[sock] = (worker, incoming)
        try:
            worker.start()
        except RuntimeError:
            del state.tcp_workers[sock]
            logger.exception("[TCP] Không thể khởi động connection worker.")
            return False
        return True


def drain_tcp_workers(timeout: float = 2.0) -> bool:
    """Wake registered TCP workers after stop_event has been set."""
    with state.conn_lock:
        workers = list(state.tcp_workers.items())
    for sock, _ in workers:
        _shutdown_socket(sock)

    deadline = time.monotonic() + timeout
    for _, (worker, _) in workers:
        worker.join(timeout=max(0.0, deadline - time.monotonic()))
    return all(not worker.is_alive() for _, (worker, _) in workers)


# ---------------------------------------------------------------------------
# Xử lý nhận file nhị phân với cơ chế .part
# ---------------------------------------------------------------------------

class FileSendResult(Enum):
    SAVED = auto()
    UNCONFIRMED = auto()
    FAILED = auto()


def _open_download_part(directory: str, filesize: int):
    with _download_lock:
        transfers = _download_reservations.get(directory, {})
        reserved = sum(transfers.values())
        existing = 0
        written = 0
        with os.scandir(directory) as entries:
            for entry in entries:
                try:
                    metadata = entry.stat(follow_symlinks=False)
                except FileNotFoundError:
                    continue
                if not stat.S_ISREG(metadata.st_mode):
                    continue
                planned = transfers.get(entry.path)
                if planned is None:
                    existing += metadata.st_size
                else:
                    written += min(metadata.st_size, planned)

        if existing + reserved + filesize > state.download_quota_bytes:
            raise OSError(errno.ENOSPC, "Lượt nhận vượt quota thư mục downloads.")
        if reserved - written + filesize > shutil.disk_usage(directory).free:
            raise OSError(errno.ENOSPC, "Không đủ dung lượng trống cho lượt nhận.")

        part = tempfile.NamedTemporaryFile(
            mode="wb", prefix=".lan-chat-", suffix=".part", dir=directory, delete=False,
        )
        _download_reservations.setdefault(directory, {})[part.name] = filesize
        return part


def _discard_download_part(directory: str, part_path: str):
    """Release a reservation under _download_lock after removing its temp file."""
    try:
        os.unlink(part_path)
    except OSError as e:
        logger.warning(f"[TCP] Không thể dọn file tạm '{part_path}': {e}")
    transfers = _download_reservations[directory]
    del transfers[part_path]
    if not transfers:
        del _download_reservations[directory]


def _publish_file(part_path: str, filename: str, directory: str) -> str:
    stem, ext = os.path.splitext(filename)
    for suffix in count():
        name = f"{stem}_{suffix}{ext}" if suffix else filename
        final_path = os.path.join(directory, name)
        try:
            os.link(part_path, final_path)
        except FileExistsError:
            continue
        return final_path


_WINDOWS_RESERVED_NAMES = {
    "CON", "PRN", "AUX", "NUL",
    *(f"COM{i}" for i in range(1, 10)),
    *(f"LPT{i}" for i in range(1, 10)),
}


def _sanitize_download_filename(raw_filename: str) -> str:
    """Chuẩn hóa tên file nhận được để an toàn trên cả Linux và Windows."""
    base = raw_filename.replace("\\", "/").rsplit("/", 1)[-1]
    cleaned = "".join(
        "_" if (ord(ch) < 32 or ch in '<>:"/\\|?*') else ch
        for ch in base
    ).strip().rstrip(". ")
    if cleaned in ("", ".", ".."):
        return "downloaded_file"
    stem, ext = os.path.splitext(cleaned)
    if stem.upper() in _WINDOWS_RESERVED_NAMES:
        return f"_{stem}{ext}"
    return cleaned


def _receive_file_stream(sock: socket.socket, raw_filename: str, filesize: int, sender_name: str):
    """
    Nhận dữ liệu nhị phân của file theo chunk, ghi vào .part và đổi tên khi hoàn tất.
    Tự động xóa file .part nếu kết nối bị đứt giữa chừng.
    """
    safe_filename = _sanitize_download_filename(raw_filename)

    part_path = None
    saved = False
    directory = os.path.abspath(os.path.join(state.data_dir, DOWNLOADS_DIR))
    try:
        sock.settimeout(FILE_IO_TIMEOUT)
        if state.stop_event.is_set():
            return
        if not 0 <= filesize <= MAX_FILE_SIZE:
            raise ValueError(f"Kích thước file phải từ 0 đến {MAX_FILE_SIZE} bytes.")
        os.makedirs(directory, exist_ok=True)
        part = _open_download_part(directory, filesize)
        part_path = part.name
        with part:
            if _system_cb:
                _system_cb(f"[*] Đang nhận file '{safe_filename}' từ {sender_name} ({filesize} bytes)...")
            received = 0
            last_milestone = -1
            while received < filesize:
                chunk = sock.recv(min(CHUNK_SIZE, filesize - received))
                if not chunk:
                    raise ConnectionResetError("Kết nối truyền file bị đứt giữa chừng.")
                part.write(chunk)
                received += len(chunk)
                milestone = (received * 4 // filesize) * 25
                if milestone > last_milestone and _system_cb:
                    last_milestone = milestone
                    filled = milestone // 5
                    bar = "█" * filled + "░" * (20 - filled)
                    _system_cb(
                        f"  [Tiến độ file] [{bar}] {milestone}% "
                        f"({received // 1024}/{filesize // 1024} KB) '{safe_filename}'"
                    )

        with _download_lock:
            if state.stop_event.is_set():
                raise ConnectionAbortedError("Đang đóng ứng dụng.")
            final_path = _publish_file(part_path, safe_filename, directory)
            _discard_download_part(directory, part_path)
            part_path = None
        saved = True
        if _system_cb:
            _system_cb(f"[OK] Đã lưu file tại {os.path.abspath(final_path)}")

    except (OSError, ValueError) as e:
        if not state.stop_event.is_set():
            logger.error(f"[TCP] Nhận file '{safe_filename}' thất bại: {e}")
            if _system_cb:
                reason = e.strerror if isinstance(e, OSError) and e.errno == errno.ENOSPC else "Lượt nhận bị gián đoạn."
                _system_cb(f"[!] Nhận file '{safe_filename}' thất bại. {reason}")
    finally:
        if part_path is not None:
            with _download_lock:
                _discard_download_part(directory, part_path)
        try:
            sock.sendall(encode_file_ack(saved))
        except OSError:
            pass
        finally:
            sock.close()


# ---------------------------------------------------------------------------
# Luồng đọc tin nhắn liên tục từ Peer (Receiver Loop)
# ---------------------------------------------------------------------------

def _receiver_loop(sock: socket.socket, peer_key: str, peer_name_hint: str = ""):
    """
    Luồng độc lập đọc các khung tin nhắn TCP từ đối tác.
    Bắt toàn bộ lỗi ngắt kết nối đột ngột, dọn dẹp và thông báo rõ ràng.
    """
    peer_name = peer_name_hint
    if not peer_name:
        with state.lock:
            p = state.peer_list.get(peer_key)
            peer_name = p.name if p else peer_key

    logger.info(f"[TCP] Luồng nhận tin bắt đầu cho: {peer_name} ({peer_key})")

    notice = None
    try:
        while not state.stop_event.is_set():
            msg_type, length = _recv_chat_header(sock)

            if msg_type == MSG_TYPE_MSG:
                content_bytes = _recv_chat_payload(sock, length)
                content = content_bytes.decode("utf-8", errors="replace")

                timestamp = time.strftime("%H:%M:%S")
                log_entry = f"[{timestamp}] {peer_name}: {content}"

                with state.lock:
                    history = state.chat_history.setdefault(peer_key, [])
                    is_current = (state.current_partner == peer_key)
                    if not is_current:
                        unread = state.unread_counts.get(peer_key, 0)
                        if unread == 0:
                            state.first_unread[peer_key] = len(history)
                        state.unread_counts[peer_key] = unread + 1
                    history.append(log_entry)

                if _msg_cb:
                    _msg_cb(peer_key, peer_name, content, is_current)

            elif msg_type == MSG_TYPE_BYE:
                # Đối tác chủ động thông báo rời đi
                break

            else:
                # Gói tin lạ không đúng chuẩn
                raise ValueError(f"Loại gói tin không xác định: '{msg_type}'")

    except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError, ConnectionError, OSError) as e:
        # Bắt lỗi khi peer ngắt kết nối đột ngột (tắt terminal, rút dây mạng...)
        logger.info(f"[TCP] Peer {peer_name} ngắt kết nối: {e}")
        notice = f"[*] Kết nối với {peer_name} đã bị ngắt."

    except ValueError as e:
        # Bắt lỗi gói tin rác (malformed packet) từ client lạ
        logger.warning(f"[TCP] Nhận gói tin rác từ {peer_name}: {e}")
        notice = f"[!] Gói tin không hợp lệ từ {peer_name}, đã đóng kết nối."

    finally:
        removed = _cleanup_connection(peer_key, sock)
        if removed and notice and _system_cb and not state.stop_event.is_set():
            _system_cb(notice)


# ---------------------------------------------------------------------------
# Xử lý kết nối đến (Incoming Connection Handler)
# ---------------------------------------------------------------------------

def _handle_incoming(conn: socket.socket, addr: Tuple[str, int]):
    """
    Xử lý kết nối TCP được Acceptor tiếp nhận.
    Phân loại kết nối: CHAT (lâu dài) hoặc FILE (truyền 1 lần).
    """
    try:
        deadline = time.monotonic() + HANDSHAKE_TIMEOUT
        read_field = partial(_recv_field, conn, deadline=deadline)
        first_field = read_field()

        if first_field == MSG_TYPE_HELLO:
            # Format: HELLO|<name>|<tcp_port>|<conn_type>|
            peer_name = read_field()
            peer_tcp_port = int(read_field())
            if not 1 <= peer_tcp_port <= 65535:
                raise ValueError("TCP port của peer ngoài phạm vi hợp lệ.")
            conn_type = read_field()  # "CHAT" hoặc "FILE"
            peer_ip = addr[0]
            peer_key = state.resolve_peer_key(peer_ip, peer_tcp_port)

            if conn_type == MSG_TYPE_FILE:
                # Đây là kết nối riêng biệt để nhận file
                next_field = read_field()
                if next_field != MSG_TYPE_FILE:
                    raise ValueError("Kỳ vọng gói tin FILE sau HELLO|FILE")
                filename = read_field()
                filesize = int(read_field())
                _receive_file_stream(conn, filename, filesize, peer_name)
                return

            elif conn_type == "CHAT":
                # Kết nối CHAT thông thường
                conn.settimeout(None)
                candidate = ChatConnection(conn, outgoing=False)
                if _register_chat_connection(peer_key, peer_ip, peer_tcp_port, candidate) is not candidate:
                    return
                peer_key, _ = state.upsert_tcp_peer(peer_name, peer_ip, peer_tcp_port)

                _restore_selected_peer(peer_key, expected_socket=conn)
                logger.info(f"[TCP] Chấp nhận kết nối CHAT từ {peer_name} ({peer_key})")
                if _system_cb:
                    _system_cb(f"[*] {peer_name} đã kết nối vào trò chuyện.")

                _receiver_loop(conn, peer_key, peer_name_hint=peer_name)
            else:
                raise ValueError("Loại connection TCP không hợp lệ.")

        elif first_field == MSG_TYPE_FILE:
            # Hỗ trợ nhận diện file gửi trực tiếp: FILE|<filename>|<filesize>|
            filename = read_field()
            filesize = int(read_field())
            sender_name = addr[0]
            with state.lock:
                for p in state.peer_list.values():
                    if p.ip == addr[0]:
                        sender_name = p.name
                        break
            _receive_file_stream(conn, filename, filesize, sender_name)

        else:
            # Gói tin rác hoặc client lạ (như nc hoặc telnet)
            raise ValueError(f"Header nhận diện đầu tiên không hợp lệ: '{first_field}'")

    except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError, ConnectionError, OSError):
        logger.debug(f"[TCP] Kết nối từ {addr} bị ngắt trong quá trình bắt tay.")
        try:
            conn.close()
        except OSError:
            pass
    except (ValueError, IndexError) as e:
        logger.warning(f"[TCP] Đóng kết nối lạ từ {addr}: {e}")
        try:
            conn.close()
        except OSError:
            pass


# ---------------------------------------------------------------------------
# Luồng Acceptor (Lắng nghe kết nối TCP)
# ---------------------------------------------------------------------------

def acceptor_thread(server_sock: socket.socket):
    """
    Luồng lắng nghe kết nối TCP trên cổng state.my_tcp_port.
    """
    logger.info(f"[TCP] Acceptor đang lắng nghe trên cổng {state.my_tcp_port}")

    with server_sock:
        while not state.stop_event.is_set():
            try:
                conn, addr = server_sock.accept()
                if not _start_tcp_worker(conn, _handle_incoming, (addr,), incoming=True):
                    conn.close()
            except socket.timeout:
                pass
            except OSError as e:
                if not state.stop_event.is_set():
                    logger.error(f"[TCP] Lỗi Acceptor: {e}")
    logger.info("[TCP] Acceptor đã dừng an toàn.")


# ---------------------------------------------------------------------------
# Giao diện gửi dữ liệu (Public API)
# ---------------------------------------------------------------------------

def get_or_connect(peer_key: str) -> Optional[socket.socket]:
    """
    Lấy kết nối hiện có hoặc thiết lập kết nối TCP mới tới peer_key.
    Bắt lỗi mạng và trả về None nếu không thể kết nối.
    """
    with state.conn_lock:
        if state.stop_event.is_set():
            return None
        existing = state.connections.get(peer_key)
        if existing is not None:
            return existing.sock
        pending = state.pending_chat_dials.get(peer_key)
        if pending is None:
            ready = threading.Event()
            state.pending_chat_dials[peer_key] = ready

    if pending is not None:
        pending.wait(timeout=2 * CHAT_CONNECT_TIMEOUT)
        with state.conn_lock:
            existing = state.connections.get(peer_key)
            return existing.sock if existing is not None and not state.stop_event.is_set() else None

    try:
        return _dial_chat(peer_key)
    finally:
        with state.conn_lock:
            del state.pending_chat_dials[peer_key]
            ready.set()


def _dial_chat(peer_key: str) -> Optional[socket.socket]:
    with state.lock:
        peer = state.peer_list.get(peer_key)
        if peer is None:
            return None
        peer_ip, peer_port, peer_name = peer.ip, peer.tcp_port, peer.name

    sock = None
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        with state.conn_lock:
            if state.stop_event.is_set():
                raise ConnectionAbortedError("Đang đóng ứng dụng.")
            state.dialing_chat_sockets.add(sock)
        _connect_socket(sock, (peer_ip, peer_port), CHAT_CONNECT_TIMEOUT)

        # Gửi lời chào nhận diện loại CHAT
        hello_frame = f"{MSG_TYPE_HELLO}|{normalize_peer_name(state.my_name)}|{state.my_tcp_port}|CHAT|".encode("utf-8")
        sock.sendall(hello_frame)
        sock.settimeout(None)

        candidate = ChatConnection(sock, outgoing=True)
        winner = _register_chat_connection(peer_key, peer_ip, peer_port, candidate)
        if winner is not candidate:
            return winner.sock if winner is not None else None

        # Bật luồng đọc tin từ phía đối tác
        if not _start_tcp_worker(sock, _receiver_loop, (peer_key, peer_name)):
            _cleanup_connection(peer_key, sock)
            return None
        return sock

    except (OSError, RuntimeError) as e:
        logger.debug(f"[TCP] Không thể kết nối tới {peer_key}: {e}")
        if sock is not None:
            _cleanup_connection(peer_key, sock)
        with state.conn_lock:
            existing = state.connections.get(peer_key)
            return existing.sock if existing is not None and not state.stop_event.is_set() else None
    finally:
        if sock is not None:
            with state.conn_lock:
                state.dialing_chat_sockets.discard(sock)


def send_chat_message(peer_key: str, content: str) -> bool:
    """
    Gửi tin nhắn văn bản tới peer qua TCP.
    Bắt toàn bộ lỗi ngắt kết nối đột ngột, dọn dẹp state và không ném traceback.
    """
    data = encode_msg(content)
    if get_or_connect(peer_key) is None:
        return False
    with state.conn_lock:
        conn = state.connections.get(peer_key)
    if conn is None:
        return False

    deadline = time.monotonic() + CHAT_WRITE_TIMEOUT
    if not conn.write_lock.acquire(timeout=CHAT_WRITE_TIMEOUT):
        return False

    try:
        with state.conn_lock:
            if state.stop_event.is_set() or state.connections.get(peer_key) is not conn:
                return False
        _send_chat_frame(conn.sock, data, deadline)
        return True
    except OSError as e:
        logger.error(f"[TCP] Lỗi khi gửi tin nhắn tới {peer_key}: {e}")
        removed = _cleanup_connection(peer_key, conn.sock)
        if removed and _system_cb:
            with state.lock:
                peer = state.peer_list.get(peer_key)
                name = peer.name if peer else peer_key
            _system_cb(f"[*] Kết nối với {name} đã bị ngắt.")
        return False
    finally:
        conn.write_lock.release()


def _receive_file_ack(sock: socket.socket) -> FileSendResult:
    read_field = partial(_recv_field, sock, deadline=time.monotonic() + FILE_IO_TIMEOUT)
    try:
        if read_field() == MSG_TYPE_FILE_ACK and read_field() == FILE_ACK_VERSION:
            result = read_field()
            if result == "OK":
                return FileSendResult.SAVED
            if result == "ERROR":
                return FileSendResult.FAILED
    except (OSError, ValueError):
        pass
    return FileSendResult.UNCONFIRMED


def _open_source(path: str, flags: int) -> int:
    # A path replaced by a FIFO after the CLI check must not block open().
    return os.open(path, flags | getattr(os, "O_NONBLOCK", 0))


def send_file(
    peer_key: str, filepath: str, progress_fn: Optional[Callable[[int, int], None]] = None,
) -> FileSendResult:
    """
    Mở một kết nối TCP độc lập để truyền file nhị phân mọi định dạng.
    Đọc file theo từng chunk 64KB, cập nhật tiến độ %.
    Bắt lỗi mạng và giải phóng socket an toàn.
    """
    global _file_sender_socket
    with state.lock:
        peer = state.peer_list.get(peer_key)
        if peer is None:
            return FileSendResult.FAILED
        endpoint = (peer.ip, peer.tcp_port)

    try:
        filename = os.path.basename(filepath)
        with open(filepath, "rb", opener=_open_source) as source:
            metadata = os.fstat(source.fileno())
            if not stat.S_ISREG(metadata.st_mode):
                raise OSError(errno.EINVAL, "Nguồn gửi phải là file thường.")
            filesize = metadata.st_size
            if filesize > MAX_FILE_SIZE:
                raise OSError(f"File vượt quá {MAX_FILE_SIZE} bytes.")

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                with _file_sender_lock:
                    if state.stop_event.is_set():
                        return FileSendResult.FAILED
                    _file_sender_socket = sock
                try:
                    _connect_socket(sock, endpoint, FILE_IO_TIMEOUT)
                    hello = f"{MSG_TYPE_HELLO}|{normalize_peer_name(state.my_name)}|{state.my_tcp_port}|{MSG_TYPE_FILE}|"
                    sock.sendall(hello.encode("utf-8"))
                    sock.sendall(encode_file_header(filename, filesize))

                    sent_bytes = 0
                    while sent_bytes < filesize and not state.stop_event.is_set():
                        chunk = source.read(min(CHUNK_SIZE, filesize - sent_bytes))
                        if not chunk:
                            raise OSError("Tệp nguồn ngắn hơn kích thước đã thông báo.")
                        sock.sendall(chunk)
                        sent_bytes += len(chunk)
                        if progress_fn and not state.stop_event.is_set():
                            progress_fn(sent_bytes, filesize)
                    if state.stop_event.is_set():
                        return FileSendResult.FAILED
                    if progress_fn and filesize == 0:
                        progress_fn(0, 0)
                    return _receive_file_ack(sock)
                finally:
                    with _file_sender_lock:
                        _file_sender_socket = None

    except OSError as e:
        if not state.stop_event.is_set():
            logger.error(f"[TCP] Gửi file '{filepath}' tới {peer_key} thất bại: {e}")
        return FileSendResult.FAILED


def start_file_send(
    peer_key: str,
    filepath: str,
    progress_fn: Callable[[int, int], None],
    done_fn: Callable[[FileSendResult], None],
) -> bool:
    """Start one outgoing transfer and report its result on the worker."""
    global _file_sender

    def transfer():
        result = send_file(peer_key, filepath, progress_fn)
        if not state.stop_event.is_set():
            done_fn(result)

    with _file_sender_lock:
        if state.stop_event.is_set() or (_file_sender is not None and _file_sender.is_alive()):
            return False
        worker = threading.Thread(target=transfer, name="TCP-File-Sender", daemon=True)
        worker.start()
        _file_sender = worker
        return True


def stop_file_sender(timeout: float = 2.0) -> bool:
    """Interrupt the active upload after stop_event is set."""
    with _file_sender_lock:
        worker = _file_sender
        sock = _file_sender_socket
    if sock is not None:
        _shutdown_socket(sock)
    if worker is not None:
        worker.join(timeout=timeout)
        return not worker.is_alive()
    return True


# ---------------------------------------------------------------------------
# Luồng Auto-reconnect
# ---------------------------------------------------------------------------

def _restore_selected_peer(peer_key: str, *, expected_socket: socket.socket | None = None):
    with state.conn_lock:
        connection = state.connections.get(peer_key)
        if connection is None or (expected_socket is not None and connection.sock is not expected_socket):
            return
        with state.lock:
            peer = state.peer_list.get(peer_key)
            if (
                state.stop_event.is_set() or state.reconnect_partner != peer_key
                or peer is None or state.current_partner is not None
            ):
                return
            state.current_partner = peer_key
            name = peer.name
    if _system_cb:
        _system_cb(f"[*] Đã tự động kết nối lại thành công với {name}.")


def _reconnect_selected_peer():
    with state.conn_lock:
        with state.lock:
            partner = state.reconnect_partner
            if partner not in state.peer_list:
                return
            if state.current_partner == partner and partner in state.connections:
                return

    if get_or_connect(partner) is not None:
        _restore_selected_peer(partner)


def _reconnect_worker():
    while not state.stop_event.wait(timeout=15.0):
        _reconnect_selected_peer()


def start_tcp_threads(
    msg_callback: Optional[Callable[[str, str, str, bool], None]] = None,
    system_callback: Optional[Callable[[str], None]] = None,
) -> Tuple[threading.Thread, threading.Thread]:
    """
    Khởi động các luồng TCP Acceptor và Auto-reconnect.
    """
    global _msg_cb, _system_cb
    _msg_cb = msg_callback
    _system_cb = system_callback

    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind((state.my_bind_ip, state.my_tcp_port))
        server_sock.listen(15)
        server_sock.settimeout(1.0)
        t_acceptor = threading.Thread(
            target=acceptor_thread, args=(server_sock,), name="TCP-Acceptor", daemon=True,
        )
        t_acceptor.start()
    except (OSError, RuntimeError):
        server_sock.close()
        raise

    t_reconnect = threading.Thread(
        target=_reconnect_worker,
        name="TCP-Reconnect",
        daemon=True,
    )
    try:
        t_reconnect.start()
    except RuntimeError:
        state.stop_event.set()
        server_sock.close()
        t_acceptor.join(timeout=2.0)
        close_chat_connections()
        drain_tcp_workers()
        raise

    return t_acceptor, t_reconnect
