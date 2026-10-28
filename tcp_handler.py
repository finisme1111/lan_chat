"""
tcp_handler.py - Xử lý kết nối TCP (Chat 1-1 và File Transfer)

# TCP Framing Protocol (Length-Prefix)
======================================================
TCP là luồng byte liên tục — receiver KHÔNG biết điểm kết thúc của một
"message" nếu không có quy ước rõ ràng. Chúng ta dùng delimiter '|' cho
phần header và length-prefix cho phần body:

  Identification: "HELLO|<name>|<tcp_port>|<conn_type>|"
  Chat message:   "MSG|<length>|" + <length bytes nội dung UTF-8>
  File transfer:  "FILE|<filename>|<size>|" + <size bytes dữ liệu thô>

Hàm _recv_field() đọc từng byte cho đến khi gặp '|' → lấy được 1 trường.
Hàm _recv_exact() đọc đúng N byte dùng vòng lặp (vì recv() có thể trả
về ít byte hơn yêu cầu do kernel buffer).

# Vòng đời kết nối TCP
======================================================
  Mở   → user gõ /chat, HOẶC peer khác kết nối vào chúng ta (acceptor).
  Giữ  → /leave CHỈ xóa current_partner. Socket vẫn sống.
  Đóng → lỗi mạng, hoặc /quit.

  File transfer dùng kết nối TMP riêng biệt (HELLO|...|FILE|) → tự đóng
  sau khi gửi xong, không ảnh hưởng kết nối chat đang chạy.

# Phân biệt kết nối CHAT và FILE
======================================================
  Sau khi connect(), bên gửi ngay lập tức gửi HELLO với conn_type:
    CHAT → acceptor lưu vào state.connections, bắt đầu receiver_loop.
    FILE → acceptor đọc 1 FILE frame, lưu file, đóng kết nối, KHÔNG lưu vào connections.
"""

import logging
import pathlib
import socket
import threading
import time

from models import state
from protocol import encode_msg

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level callbacks (set bởi start_tcp_threads từ main.py)
# ---------------------------------------------------------------------------
# Dùng biến module-level thay vì truyền callback qua tham số vì các receiver
# thread được spawn động khi connection đến, không thể pass callback lúc đó.

_msg_cb = None       # fn(peer_key, peer_name, content) → hiển thị tin nhắn đến
_system_cb = None    # fn(message) → hiển thị thông báo hệ thống

DOWNLOADS_DIR = pathlib.Path("downloads")  # Thư mục lưu file nhận được


# ---------------------------------------------------------------------------
# Low-level socket helpers
# ---------------------------------------------------------------------------

def _recv_field(sock: socket.socket) -> str:
    """
    Đọc từng byte từ socket cho đến khi gặp ký tự '|'.
    Trả về chuỗi đã decode (không bao gồm '|').

    Lý do đọc từng byte: header có độ dài không cố định (tên file, tên peer...),
    không thể dùng recv(N) với N cố định.

    Raise ConnectionError nếu connection đóng giữa chừng.
    """
    buf = bytearray()
    while True:
        ch = sock.recv(1)
        if not ch:
            raise ConnectionError("Connection closed by peer")
        if ch == b"|":
            return buf.decode("utf-8")
        buf += ch


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    """
    Đọc đúng N byte từ socket.

    Tại sao cần vòng lặp? recv(n) KHÔNG đảm bảo trả về đúng n byte.
    OS có thể chia nhỏ dữ liệu thành nhiều segment TCP → mỗi recv() chỉ
    trả về 1 segment. Vòng lặp ghép chúng lại cho đến khi đủ n byte.

    Raise ConnectionError nếu connection đóng trước khi đọc đủ.
    """
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError(f"Connection closed: expected {n} bytes, got {len(buf)}")
        buf += chunk
    return bytes(buf)


def _read_frame(sock: socket.socket) -> dict:
    """
    Đọc một frame hoàn chỉnh từ socket, trả về dict theo loại:

      HELLO → {"type": "HELLO", "name": str, "tcp_port": int, "conn_type": str}
      MSG   → {"type": "MSG",   "content": str}
      FILE  → {"type": "FILE",  "filename": str, "data": bytes}

    Raise ConnectionError khi connection đóng.
    Raise ValueError khi nhận frame không rõ loại (có thể là app khác kết nối nhầm).
    """
    msg_type = _recv_field(sock)

    if msg_type == "HELLO":
        # Format: HELLO|<name>|<tcp_port>|<conn_type>|
        name = _recv_field(sock)
        tcp_port = int(_recv_field(sock))
        conn_type = _recv_field(sock)   # "CHAT" hoặc "FILE"
        return {"type": "HELLO", "name": name, "tcp_port": tcp_port, "conn_type": conn_type}

    elif msg_type == "MSG":
        # Format: MSG|<length>|<content bytes>
        length = int(_recv_field(sock))
        content = _recv_exact(sock, length).decode("utf-8")
        return {"type": "MSG", "content": content}

    elif msg_type == "FILE":
        # Format: FILE|<filename>|<size>|<data bytes>
        filename = _recv_field(sock)
        size = int(_recv_field(sock))
        data = _recv_exact(sock, size)
        return {"type": "FILE", "filename": filename, "data": data}

    else:
        raise ValueError(f"Unknown frame type: '{msg_type}'")


def _send_hello(sock: socket.socket, conn_type: str = "CHAT"):
    """
    Gửi frame HELLO để xác định danh tính ngay sau khi connect().
    conn_type: "CHAT" (kết nối chat lâu dài) | "FILE" (chỉ gửi 1 file rồi đóng).

    sendall() khác send() ở chỗ: sendall() đảm bảo toàn bộ dữ liệu được gửi,
    tự xử lý trường hợp kernel buffer đầy và phải gửi nhiều lần.
    """
    frame = f"HELLO|{state.my_name}|{state.my_tcp_port}|{conn_type}|"
    sock.sendall(frame.encode("utf-8"))


# ---------------------------------------------------------------------------
# Connection management
# ---------------------------------------------------------------------------

def _cleanup_connection(peer_key: str):
    """
    Dọn dẹp sau khi kết nối TCP bị đóng (do lỗi mạng hoặc peer thoát).
    Xóa khỏi state.connections và đóng socket.

    Lưu ý: KHÔNG xóa current_partner — user có thể /chat lại khi peer quay lại.
    """
    with state.conn_lock:
        sock = state.connections.pop(peer_key, None)
    if sock:
        try:
            sock.close()
        except OSError:
            pass


def get_or_connect(peer_key: str) -> socket.socket | None:
    """
    Trả về kết nối TCP đang mở đến peer_key.
    Nếu chưa có, tạo kết nối mới (active connect).

    Flow khi tạo mới:
      1. Lấy thông tin peer từ state.peer_list.
      2. socket.connect() đến (peer.ip, peer.tcp_port) — TCP 3-way handshake.
      3. Gửi HELLO để bên kia nhận dạng chúng ta.
      4. Lưu socket vào state.connections.
      5. Spawn receiver thread để lắng nghe phản hồi từ peer.

    Trả về None nếu peer không online hoặc kết nối thất bại.
    """
    # Fast path: kết nối đã có sẵn
    with state.conn_lock:
        existing = state.connections.get(peer_key)
    if existing:
        return existing

    # Lấy thông tin peer
    with state.lock:
        peer = state.peer_list.get(peer_key)
    if peer is None:
        logger.warning(f"[TCP] get_or_connect: peer '{peer_key}' not in peer_list")
        return None

    # Tạo kết nối TCP mới
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5.0)                  # Timeout cho bước connect()
        sock.connect((peer.ip, peer.tcp_port))
        sock.settimeout(None)                  # Blocking mode sau khi connect xong
        _send_hello(sock, conn_type="CHAT")   # Tự giới thiệu

        # Lưu vào state trước khi spawn thread (tránh race với acceptor)
        with state.conn_lock:
            state.connections[peer_key] = sock

        # Spawn receiver thread: lắng nghe các frame ngược chiều từ peer
        t = threading.Thread(
            target=_receiver_loop,
            args=(sock, peer_key, peer.name),
            name=f"TCP-Recv-{peer.name}",
            daemon=True,
        )
        t.start()
        logger.info(f"[TCP] Connected to {peer.name} ({peer_key})")
        return sock

    except OSError as e:
        logger.error(f"[TCP] Connect error to {peer_key}: {e}")
        return None


# ---------------------------------------------------------------------------
# Receiver Loop (chạy trong thread riêng cho mỗi kết nối)
# ---------------------------------------------------------------------------

def _save_file(filename: str, data: bytes) -> str:
    """
    Lưu file nhận được vào thư mục downloads/.
    Xử lý trùng tên bằng cách thêm hậu tố _1, _2, ...
    Trả về đường dẫn tuyệt đối của file đã lưu.
    """
    DOWNLOADS_DIR.mkdir(exist_ok=True)
    dest = DOWNLOADS_DIR / filename
    counter = 1
    while dest.exists():
        stem = pathlib.Path(filename).stem
        suffix = pathlib.Path(filename).suffix
        dest = DOWNLOADS_DIR / f"{stem}_{counter}{suffix}"
        counter += 1
    dest.write_bytes(data)
    return str(dest.resolve())


def _receiver_loop(sock: socket.socket, peer_key: str, peer_name_hint: str = ""):
    """
    Vòng lặp đọc frame liên tục từ một kết nối TCP.
    Chạy trong thread daemon riêng, tự kết thúc khi connection đóng.

    Xử lý 2 loại frame:
      MSG  → thêm vào history, gọi _msg_cb để hiển thị
      FILE → lưu file, gọi _system_cb để thông báo

    peer_name_hint: Tên peer biết từ HELLO frame. Ưu tiên hơn peer_list
    (peer_list cần UDP HELLO trước mới có, nhưng TCP có thể đến trước).
    """
    # Ưu tiên: hint từ HELLO > peer_list từ UDP > fallback key
    peer_name = peer_name_hint
    if not peer_name:
        with state.lock:
            if peer_key in state.peer_list:
                peer_name = state.peer_list[peer_key].name
    if not peer_name:
        peer_name = peer_key

    logger.info(f"[TCP] Receiver loop started for {peer_name} ({peer_key})")

    try:
        while not state.stop_event.is_set():
            frame = _read_frame(sock)

            if frame["type"] == "MSG":
                content = frame["content"]
                timestamp = time.strftime("%H:%M:%S")
                log_entry = f"[{timestamp}] {peer_name}: {content}"

                # Cập nhật history và unread count (thread-safe)
                with state.lock:
                    state.chat_history.setdefault(peer_key, []).append(log_entry)
                    is_current = state.current_partner == peer_key
                    if not is_current:
                        state.unread_counts[peer_key] = (
                            state.unread_counts.get(peer_key, 0) + 1
                        )

                # Gọi callback để UI hiển thị
                if _msg_cb:
                    _msg_cb(peer_key, peer_name, content, is_current)

            elif frame["type"] == "FILE":
                save_path = _save_file(frame["filename"], frame["data"])
                size_kb = len(frame["data"]) / 1024
                if _system_cb:
                    _system_cb(
                        f"📁 [{peer_name}] gửi file '{frame['filename']}' "
                        f"({size_kb:.1f} KB) → lưu tại {save_path}"
                    )

    except ConnectionError:
        # Peer đã ngắt kết nối (đóng app hoặc mất mạng) — đây là kết thúc bình thường
        logger.info(f"[TCP] {peer_name} ({peer_key}) disconnected.")
    except Exception as e:
        if not state.stop_event.is_set():
            logger.error(f"[TCP] Receiver error for {peer_key}: {e}")
    finally:
        _cleanup_connection(peer_key)
        if _system_cb and not state.stop_event.is_set():
            _system_cb(f"⚠️  Kết nối với {peer_name} đã đóng.")


# ---------------------------------------------------------------------------
# Incoming connection handler (chạy trong thread spawn từ acceptor)
# ---------------------------------------------------------------------------

def _handle_incoming(conn: socket.socket, addr: tuple):
    """
    Xử lý một kết nối TCP mới từ phía acceptor (bị động - passive).

    Bước 1: Đọc frame HELLO để nhận dạng peer và loại kết nối.
    Bước 2a (CHAT): Lưu socket, spawn receiver_loop.
    Bước 2b (FILE): Đọc frame FILE, lưu file, đóng kết nối. KHÔNG lưu vào connections.
    """
    try:
        conn.settimeout(10.0)   # Chờ HELLO tối đa 10 giây
        frame = _read_frame(conn)

        if frame["type"] != "HELLO":
            logger.warning(f"[TCP] Unexpected first frame from {addr}: {frame['type']}")
            conn.close()
            return

        peer_name = frame["name"]
        peer_tcp_port = frame["tcp_port"]
        conn_type = frame["conn_type"]  # "CHAT" hoặc "FILE"
        peer_ip = addr[0]
        peer_key = state.get_peer_key(peer_ip, peer_tcp_port)

        conn.settimeout(None)   # Chuyển sang blocking mode

        if conn_type == "FILE":
            # --- Kết nối chỉ để nhận 1 file ---
            # Không lưu vào state.connections, không spawn receiver_loop.
            file_frame = _read_frame(conn)
            conn.close()   # Đóng ngay sau khi nhận xong
            if file_frame["type"] == "FILE":
                save_path = _save_file(file_frame["filename"], file_frame["data"])
                size_kb = len(file_frame["data"]) / 1024
                if _system_cb:
                    _system_cb(
                        f"📁 [{peer_name}] gửi file '{file_frame['filename']}' "
                        f"({size_kb:.1f} KB) → lưu tại {save_path}"
                    )

        else:
            # --- Kết nối chat lâu dài ---
            with state.conn_lock:
                # Nếu đã có kết nối cũ (peer reconnect), đóng cái cũ
                old_sock = state.connections.get(peer_key)
                if old_sock:
                    try:
                        old_sock.close()
                    except OSError:
                        pass
                state.connections[peer_key] = conn

            logger.info(f"[TCP] Accepted CHAT connection from {peer_name} ({peer_key})")
            if _system_cb:
                _system_cb(f"🔗 {peer_name} đã kết nối vào.")

            # Chạy receiver_loop ngay trong thread này (đã là thread riêng)
            _receiver_loop(conn, peer_key, peer_name_hint=peer_name)

    except ConnectionError:
        logger.info(f"[TCP] Incoming connection from {addr} closed during handshake.")
    except Exception as e:
        logger.error(f"[TCP] Handle incoming error from {addr}: {e}")
        try:
            conn.close()
        except OSError:
            pass


# ---------------------------------------------------------------------------
# Acceptor Thread
# ---------------------------------------------------------------------------

def acceptor_thread():
    """
    Luồng lắng nghe và chấp nhận kết nối TCP đến (passive open).

    server_sock.listen(N): Cho phép tối đa N kết nối chờ trong hàng đợi
    trước khi accept() lấy ra xử lý. Kernel tự quản lý hàng đợi này.

    Mỗi kết nối mới được spawn thành thread riêng (_handle_incoming) để
    acceptor không bị block trong khi xử lý kết nối hiện tại.
    """
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # SO_REUSEADDR: Cho phép bind lại port ngay khi restart app.
    # Không có option này → phải đợi ~60s (TIME_WAIT state của TCP).
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind(("", state.my_tcp_port))
    server_sock.listen(10)
    server_sock.settimeout(1.0)  # Timeout để kiểm tra stop_event

    logger.info(f"[TCP] Acceptor started → listening on port {state.my_tcp_port}")

    while not state.stop_event.is_set():
        try:
            # accept() block cho đến khi có kết nối mới.
            # Với settimeout(1.0), nó raise socket.timeout sau 1 giây nếu không có ai.
            conn, addr = server_sock.accept()
            logger.debug(f"[TCP] Incoming connection from {addr}")

            # Spawn thread riêng để xử lý, không block acceptor
            t = threading.Thread(
                target=_handle_incoming,
                args=(conn, addr),
                name=f"TCP-Handler-{addr}",
                daemon=True,
            )
            t.start()

        except socket.timeout:
            pass
        except OSError as e:
            if not state.stop_event.is_set():
                logger.error(f"[TCP] Acceptor error: {e}")

    server_sock.close()
    logger.info("[TCP] Acceptor stopped.")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def send_chat_message(peer_key: str, content: str) -> bool:
    """
    Gửi tin nhắn chat đến peer.
    Tự động kết nối nếu chưa có kết nối.
    Trả về True nếu gửi thành công.
    """
    sock = get_or_connect(peer_key)
    if sock is None:
        return False
    try:
        # encode_msg từ protocol.py: tạo frame "MSG|<len>|<content>"
        sock.sendall(encode_msg(content))
        return True
    except OSError as e:
        logger.error(f"[TCP] Send error to {peer_key}: {e}")
        _cleanup_connection(peer_key)
        return False


def send_file(peer_key: str, filepath: str, progress_fn=None) -> bool:
    """
    Gửi file đến peer qua kết nối TCP RIÊNG BIỆT, có hỗ trợ progress.

    Tại sao dùng kết nối riêng?
    - File lớn → việc gửi chiếm nhiều thời gian.
    - Nếu dùng kết nối chat chung, receiver của peer sẽ block khi đọc file
      → không nhận được tin nhắn chat trong thời gian đó.
    - Kết nối riêng → file transfer và chat hoàn toàn độc lập.

    Chunked sending:
    - Gửi từng chunk 64KB thay vì sendall(toàn bộ file 1 lần).
    - Sau mỗi chunk gọi progress_fn(sent, total) để cập nhật UI.
    - sendall() vẫn được dùng cho từng chunk (đảm bảo chunk được gửi đủ).

    Tham số:
        progress_fn: fn(sent: int, total: int) → None | None để bỏ qua.
    """
    with state.lock:
        peer = state.peer_list.get(peer_key)
    if peer is None:
        return False

    path = pathlib.Path(filepath)
    if not path.exists() or not path.is_file():
        logger.error(f"[TCP] File not found: {filepath}")
        return False

    try:
        data = path.read_bytes()
        total = len(data)

        # Mở kết nối riêng biệt (không lưu vào state.connections)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10.0)
        sock.connect((peer.ip, peer.tcp_port))
        sock.settimeout(None)

        # Bước 1: Xác định danh tính và loại kết nối = FILE
        _send_hello(sock, conn_type="FILE")

        # Bước 2: Gửi header "FILE|<filename>|<size>|"
        header = f"FILE|{path.name}|{total}|".encode("utf-8")
        sock.sendall(header)

        # Bước 3: Gửi dữ liệu theo từng chunk 64KB
        # Ưu điểm so với sendall(data) 1 lần:
        # - Cập nhật progress sau mỗi chunk.
        # - Kernel buffer không bị tắc nghẽn với file rất lớn.
        CHUNK = 65536  # 64 KB per chunk
        sent = 0
        while sent < total:
            end = min(sent + CHUNK, total)
            sock.sendall(data[sent:end])
            sent = end
            if progress_fn:
                progress_fn(sent, total)

        sock.close()
        return True

    except OSError as e:
        logger.error(f"[TCP] File send error to {peer_key}: {e}")
        return False


# ---------------------------------------------------------------------------
# Auto-reconnect Thread
# ---------------------------------------------------------------------------

def _reconnect_worker():
    """
    Luồng nền: tự động kết nối lại với current_partner khi connection bị mất.

    Kịch bản: User đang chat với Minh → Minh restart app → kết nối TCP drop.
    Không có reconnect → user phải gõ /chat Minh lại.
    Có reconnect → sau tối đa 15 giây, kết nối tự phục hồi.

    Thuật toán:
    1. Kiểm tra mỗi 15 giây.
    2. Nếu current_partner không có trong connections (connection đã drop)
       VÀ peer vẫn trong peer_list (vẫn online qua UDP) → thử kết nối lại.
    3. Nếu thành công → thông báo cho user.
    """
    while not state.stop_event.is_set():
        # Ngủ 15 giây (dừng ngay nếu stop_event được set)
        state.stop_event.wait(timeout=15.0)
        if state.stop_event.is_set():
            break

        with state.lock:
            partner = state.current_partner
        if partner is None:
            continue

        # Kiểm tra connection hiện tại
        with state.conn_lock:
            has_conn = partner in state.connections
        if has_conn:
            continue  # Còn kết nối → không cần reconnect

        # Kiểm tra peer còn online (UDP vẫn nhận HELLO)
        with state.lock:
            peer = state.peer_list.get(partner)
        if peer is None:
            continue  # Peer đã offline hoàn toàn → không reconnect

        # Thử kết nối lại
        logger.info(f"[TCP] Reconnecting to {peer.name} ({partner})...")
        sock = get_or_connect(partner)
        if sock and _system_cb:
            _system_cb(f"🔄 Đã tự động kết nối lại với {peer.name}.")


def start_tcp_threads(msg_callback=None, system_callback=None) -> threading.Thread:
    """
    Khởi động TCP acceptor thread và reconnect thread.

    Tham số:
        msg_callback:    fn(peer_key, peer_name, content, is_current_partner)
        system_callback: fn(message)

    Trả về:
        Thread acceptor để main.py có thể join() khi thoát.
        (Reconnect thread là daemon, không cần join riêng.)
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

    # Reconnect thread chạy ngầm, không cần trả về để join
    t_reconnect = threading.Thread(
        target=_reconnect_worker,
        name="TCP-Reconnect",
        daemon=True,
    )
    t_reconnect.start()

    return t_acceptor
