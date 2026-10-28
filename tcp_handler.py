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
   - Ghi dữ liệu tạm vào downloads/<filename>.part kèm cập nhật tiến độ %.
   - Khi nhận đủ 100%: đổi tên thành file chính thức.
   - Nếu đứt kết nối giữa chừng: Tự động xóa file .part dở dang để tránh rác ổ đĩa.
"""

import logging
import os
import socket
import threading
import time
from typing import Callable, Optional, Tuple, Any

from models import state
from protocol import (
    encode_msg,
    encode_file_header,
    DELIMITER,
    MAX_FIELD_LEN,
    MAX_MSG_LEN,
    MSG_TYPE_HELLO,
    MSG_TYPE_MSG,
    MSG_TYPE_FILE,
    MSG_TYPE_BYE,
)

logger = logging.getLogger(__name__)

# Kích thước mỗi chunk khi gửi/nhận file (64 KB)
CHUNK_SIZE = 65536

# Thư mục lưu file nhận được
DOWNLOADS_DIR = "downloads"

# Callbacks hiển thị lên giao diện console
_msg_cb: Optional[Callable[[str, str, str, bool], None]] = None
_system_cb: Optional[Callable[[str], None]] = None


# ---------------------------------------------------------------------------
# Các hàm đọc dữ liệu Socket mức thấp có bảo vệ (Low-level Helpers)
# ---------------------------------------------------------------------------

def _recv_field(sock: socket.socket) -> str:
    """
    Đọc từng byte cho đến khi gặp ký tự phân tách '|'.
    Chống DoS: Nếu đọc quá MAX_FIELD_LEN (4KB) mà chưa thấy '|', ném ValueError.
    Bắt lỗi ngắt kết nối và chuyển thành ConnectionResetError.
    """
    buf = bytearray()
    while True:
        try:
            ch = sock.recv(1)
        except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError, OSError) as e:
            raise ConnectionResetError(f"Lỗi kết nối khi đọc trường dữ liệu: {e}")

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


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    """
    Đọc chính xác n bytes từ socket bằng vòng lặp.
    Đảm bảo không bị thiếu hụt dữ liệu do phân mảnh gói tin TCP (TCP chunking).
    """
    buf = bytearray()
    while len(buf) < n:
        try:
            chunk = sock.recv(min(n - len(buf), CHUNK_SIZE))
        except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError, OSError) as e:
            raise ConnectionResetError(f"Lỗi kết nối khi đọc dữ liệu payload: {e}")

        if not chunk:
            raise ConnectionResetError(f"Kết nối bị đứt: mong đợi {n} bytes nhưng chỉ nhận được {len(buf)} bytes.")
        buf += chunk
    return bytes(buf)


def _cleanup_connection(peer_key: str):
    """
    Dọn dẹp socket khi kết nối bị đóng hoặc gặp sự cố.
    Đồng thời đưa trạng thái chat về màn hình chính nếu đang chat với peer này.
    """
    with state.conn_lock:
        sock = state.connections.pop(peer_key, None)
    if sock:
        try:
            sock.close()
        except OSError:
            pass

    # Nếu đang chat với peer này thì đưa dấu nhắc về trạng thái ngoài
    state.reset_partner_if(peer_key)


# ---------------------------------------------------------------------------
# Xử lý nhận file nhị phân với cơ chế .part
# ---------------------------------------------------------------------------

def _receive_file_stream(sock: socket.socket, raw_filename: str, filesize: int, sender_name: str):
    """
    Nhận dữ liệu nhị phân của file theo chunk, ghi vào .part và đổi tên khi hoàn tất.
    Tự động xóa file .part nếu kết nối bị đứt giữa chừng.
    """
    # 1. Bảo vệ chống Directory Traversal: Chỉ lấy tên file gốc
    safe_filename = os.path.basename(raw_filename).replace("|", "_").strip()
    if not safe_filename:
        safe_filename = "downloaded_file"

    # Tạo thư mục downloads nếu chưa tồn tại
    os.makedirs(DOWNLOADS_DIR, exist_ok=True)

    # 2. Xử lý trùng tên file: Nếu file đã tồn tại, tự sinh tên mới <stem>_1.<ext>
    base_name, ext = os.path.splitext(safe_filename)
    counter = 1
    final_filename = safe_filename
    final_path = os.path.join(DOWNLOADS_DIR, final_filename)
    while os.path.exists(final_path):
        final_filename = f"{base_name}_{counter}{ext}"
        final_path = os.path.join(DOWNLOADS_DIR, final_filename)
        counter += 1

    # Tên file tạm thời trong quá trình truyền
    part_path = os.path.join(DOWNLOADS_DIR, f"{final_filename}.part")

    if _system_cb:
        size_kb = filesize / 1024
        _system_cb(f"[*] Đang nhận file '{safe_filename}' ({size_kb:.1f} KB) từ {sender_name}...")

    received_bytes = 0
    file_obj = None

    try:
        # Mở file nhị phân ở chế độ 'wb' (hỗ trợ mọi định dạng: .pdf, .docx, .zip, .mp4, ...)
        file_obj = open(part_path, "wb")
        last_reported_pct = -1

        while received_bytes < filesize:
            to_read = min(CHUNK_SIZE, filesize - received_bytes)
            chunk = sock.recv(to_read)

            if not chunk:
                raise ConnectionResetError("Kết nối truyền file bị đứt giữa chừng từ phía người gửi.")

            file_obj.write(chunk)
            received_bytes += len(chunk)

            # Cập nhật tiến độ % ra màn hình console (mỗi mốc 25%)
            pct = int(received_bytes * 100 / filesize) if filesize > 0 else 100
            milestone = (pct // 25) * 25
            if milestone > last_reported_pct and _system_cb:
                last_reported_pct = milestone
                filled = milestone // 5
                bar = "█" * filled + "░" * (20 - filled)
                _system_cb(
                    f"  [Tiến độ file] [{bar}] {milestone}% "
                    f"({received_bytes // 1024}/{filesize // 1024} KB) '{safe_filename}'"
                )

        # Đóng file tạm sau khi đã ghi đủ 100% số byte
        file_obj.close()
        file_obj = None

        # Đổi tên file từ .part thành tên chính thức
        if os.path.exists(part_path):
            os.replace(part_path, final_path)

        if _system_cb:
            _system_cb(f"[OK] Đã nhận thành công '{final_filename}' -> Lưu tại: {os.path.abspath(final_path)}")

    except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError, ConnectionError, OSError) as e:
        # Nếu đứt kết nối hoặc có lỗi I/O:
        if file_obj:
            try:
                file_obj.close()
            except OSError:
                pass

        # BẮT BUỘC: Tự động xóa file .part dở dang để bảo vệ ổ đĩa
        if os.path.exists(part_path):
            try:
                os.remove(part_path)
            except OSError:
                pass

        logger.error(f"[TCP] Nhận file '{safe_filename}' thất bại: {e}")
        if _system_cb:
            _system_cb(f"[!] Nhận file '{safe_filename}' thất bại do mất kết nối giữa chừng.")
    finally:
        try:
            sock.close()
        except OSError:
            pass


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

    try:
        while not state.stop_event.is_set():
            # 1. Đọc loại frame
            msg_type = _recv_field(sock)

            if msg_type == MSG_TYPE_MSG:
                # Format: MSG|<length>|<content>
                length_str = _recv_field(sock)
                try:
                    length = int(length_str)
                    if length < 0 or length > MAX_MSG_LEN:
                        raise ValueError(f"Độ dài tin nhắn không hợp lệ: {length}")
                except ValueError as e:
                    raise ValueError(f"Header MSG sai cú pháp: {e}")

                content_bytes = _recv_exact(sock, length)
                content = content_bytes.decode("utf-8", errors="replace")

                timestamp = time.strftime("%H:%M:%S")
                log_entry = f"[{timestamp}] {peer_name}: {content}"

                with state.lock:
                    state.chat_history.setdefault(peer_key, []).append(log_entry)
                    is_current = (state.current_partner == peer_key)
                    if not is_current:
                        state.unread_counts[peer_key] = state.unread_counts.get(peer_key, 0) + 1

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
        if _system_cb and not state.stop_event.is_set():
            _system_cb(f"[*] Kết nối với {peer_name} đã bị ngắt.")

    except ValueError as e:
        # Bắt lỗi gói tin rác (malformed packet) từ client lạ
        logger.warning(f"[TCP] Nhận gói tin rác từ {peer_name}: {e}")
        if _system_cb and not state.stop_event.is_set():
            _system_cb(f"[!] Gói tin không hợp lệ từ {peer_name}, đã đóng kết nối.")

    finally:
        _cleanup_connection(peer_key)


# ---------------------------------------------------------------------------
# Xử lý kết nối đến (Incoming Connection Handler)
# ---------------------------------------------------------------------------

def _handle_incoming(conn: socket.socket, addr: Tuple[str, int]):
    """
    Xử lý kết nối TCP được Acceptor tiếp nhận.
    Phân loại kết nối: CHAT (lâu dài) hoặc FILE (truyền 1 lần).
    """
    try:
        conn.settimeout(10.0)  # Chờ header nhận diện tối đa 10s
        first_field = _recv_field(conn)

        if first_field == MSG_TYPE_HELLO:
            # Format: HELLO|<name>|<tcp_port>|<conn_type>|
            peer_name = _recv_field(conn)
            peer_tcp_port = int(_recv_field(conn))
            conn_type = _recv_field(conn)  # "CHAT" hoặc "FILE"
            peer_ip = addr[0]
            peer_key = state.get_peer_key(peer_ip, peer_tcp_port)

            conn.settimeout(None)  # Trả về chế độ blocking bình thường

            if conn_type == MSG_TYPE_FILE:
                # Đây là kết nối riêng biệt để nhận file
                next_field = _recv_field(conn)
                if next_field != MSG_TYPE_FILE:
                    raise ValueError("Kỳ vọng gói tin FILE sau HELLO|FILE")
                filename = _recv_field(conn)
                filesize = int(_recv_field(conn))
                _receive_file_stream(conn, filename, filesize, peer_name)
                return

            else:
                # Kết nối CHAT thông thường
                with state.conn_lock:
                    old_sock = state.connections.get(peer_key)
                    if old_sock:
                        try:
                            old_sock.close()
                        except OSError:
                            pass
                    state.connections[peer_key] = conn

                logger.info(f"[TCP] Chấp nhận kết nối CHAT từ {peer_name} ({peer_key})")
                if _system_cb:
                    _system_cb(f"[*] {peer_name} đã kết nối vào trò chuyện.")

                _receiver_loop(conn, peer_key, peer_name_hint=peer_name)

        elif first_field == MSG_TYPE_FILE:
            # Hỗ trợ nhận diện file gửi trực tiếp: FILE|<filename>|<filesize>|
            filename = _recv_field(conn)
            filesize = int(_recv_field(conn))
            conn.settimeout(None)
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

def acceptor_thread():
    """
    Luồng lắng nghe kết nối TCP trên cổng state.my_tcp_port.
    """
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    try:
        server_sock.bind(("", state.my_tcp_port))
        server_sock.listen(15)
        server_sock.settimeout(1.0)
    except OSError as e:
        logger.error(f"[TCP] Không thể bind cổng TCP {state.my_tcp_port}: {e}")
        if _system_cb:
            _system_cb(f"[!] Cổng TCP {state.my_tcp_port} đang bị chiếm dụng!")
        server_sock.close()
        return

    logger.info(f"[TCP] Acceptor đang lắng nghe trên cổng {state.my_tcp_port}")

    while not state.stop_event.is_set():
        try:
            conn, addr = server_sock.accept()
            # Khởi chạy một luồng daemon riêng để xử lý kết nối này
            t = threading.Thread(
                target=_handle_incoming,
                args=(conn, addr),
                name=f"TCP-Conn-{addr}",
                daemon=True,
            )
            t.start()
        except socket.timeout:
            pass
        except OSError as e:
            if not state.stop_event.is_set():
                logger.error(f"[TCP] Lỗi Acceptor: {e}")

    try:
        server_sock.close()
    except OSError:
        pass
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
        existing = state.connections.get(peer_key)
    if existing:
        return existing

    with state.lock:
        peer = state.peer_list.get(peer_key)
    if peer is None:
        return None

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5.0)
        sock.connect((peer.ip, peer.tcp_port))
        sock.settimeout(None)

        # Gửi lời chào nhận diện loại CHAT
        hello_frame = f"{MSG_TYPE_HELLO}|{state.my_name}|{state.my_tcp_port}|CHAT|".encode("utf-8")
        sock.sendall(hello_frame)

        with state.conn_lock:
            state.connections[peer_key] = sock

        # Bật luồng đọc tin từ phía đối tác
        t = threading.Thread(
            target=_receiver_loop,
            args=(sock, peer_key, peer.name),
            name=f"TCP-Recv-{peer.name}",
            daemon=True,
        )
        t.start()
        return sock

    except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError, ConnectionError, OSError) as e:
        logger.debug(f"[TCP] Không thể kết nối tới {peer_key}: {e}")
        return None


def send_chat_message(peer_key: str, content: str) -> bool:
    """
    Gửi tin nhắn văn bản tới peer qua TCP.
    Bắt toàn bộ lỗi ngắt kết nối đột ngột, dọn dẹp state và không ném traceback.
    """
    sock = get_or_connect(peer_key)
    if sock is None:
        return False

    try:
        data = encode_msg(content)
        sock.sendall(data)
        return True
    except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError, ConnectionError, OSError) as e:
        logger.error(f"[TCP] Lỗi khi gửi tin nhắn tới {peer_key}: {e}")
        _cleanup_connection(peer_key)
        if _system_cb:
            with state.lock:
                peer = state.peer_list.get(peer_key)
                name = peer.name if peer else peer_key
            _system_cb(f"[*] Kết nối với {name} đã bị ngắt.")
        return False


def send_file(peer_key: str, filepath: str, progress_fn: Optional[Callable[[int, int], None]] = None) -> bool:
    """
    Mở một kết nối TCP độc lập để truyền file nhị phân mọi định dạng.
    Đọc file theo từng chunk 64KB, cập nhật tiến độ %.
    Bắt lỗi mạng và giải phóng socket an toàn.
    """
    with state.lock:
        peer = state.peer_list.get(peer_key)
    if peer is None:
        return False

    if not os.path.isfile(filepath):
        logger.error(f"[TCP] Đường dẫn file không hợp lệ hoặc không tồn tại: {filepath}")
        return False

    try:
        filesize = os.path.getsize(filepath)
        filename = os.path.basename(filepath)

        # Mở kết nối TCP riêng biệt chuyên dụng cho File Transfer
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10.0)
        sock.connect((peer.ip, peer.tcp_port))
        sock.settimeout(None)

        # 1. Gửi định danh HELLO loại FILE
        hello_frame = f"{MSG_TYPE_HELLO}|{state.my_name}|{state.my_tcp_port}|{MSG_TYPE_FILE}|".encode("utf-8")
        sock.sendall(hello_frame)

        # 2. Gửi header FILE: FILE|<filename>|<filesize>|
        file_header = encode_file_header(filename, filesize)
        sock.sendall(file_header)

        # 3. Đọc nhị phân ('rb') và truyền từng chunk 64KB
        sent_bytes = 0
        with open(filepath, "rb") as f:
            while sent_bytes < filesize:
                chunk = f.read(CHUNK_SIZE)
                if not chunk:
                    break
                sock.sendall(chunk)
                sent_bytes += len(chunk)
                if progress_fn:
                    progress_fn(sent_bytes, filesize)

        try:
            sock.close()
        except OSError:
            pass

        return True

    except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError, ConnectionError, OSError) as e:
        logger.error(f"[TCP] Gửi file '{filepath}' tới {peer_key} thất bại: {e}")
        return False


# ---------------------------------------------------------------------------
# Luồng Auto-reconnect
# ---------------------------------------------------------------------------

def _reconnect_worker():
    """
    Luồng tự động kết nối lại với đối tác hiện tại nếu kết nối bị đứt
    nhưng peer vẫn còn online trên mạng LAN (nhận qua UDP).
    """
    while not state.stop_event.is_set():
        state.stop_event.wait(timeout=15.0)
        if state.stop_event.is_set():
            break

        with state.lock:
            partner = state.current_partner
        if not partner:
            continue

        with state.conn_lock:
            connected = partner in state.connections
        if connected:
            continue

        with state.lock:
            peer = state.peer_list.get(partner)
        if not peer:
            continue

        # Thử tái lập kết nối
        sock = get_or_connect(partner)
        if sock and _system_cb:
            _system_cb(f"[*] Đã tự động kết nối lại thành công với {peer.name}.")


def start_tcp_threads(
    msg_callback: Optional[Callable[[str, str, str, bool], None]] = None,
    system_callback: Optional[Callable[[str], None]] = None,
) -> threading.Thread:
    """
    Khởi động các luồng TCP Acceptor và Auto-reconnect.
    """
    global _msg_cb, _system_cb
    _msg_cb = msg_callback
    _system_cb = system_callback

    t_acceptor = threading.Thread(
        target=acceptor_thread,
        name="TCP-Acceptor",
        daemon=True,
    )
    t_acceptor.start()

    t_reconnect = threading.Thread(
        target=_reconnect_worker,
        name="TCP-Reconnect",
        daemon=True,
    )
    t_reconnect.start()

    return t_acceptor
