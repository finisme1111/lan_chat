"""
udp_handler.py - Xử lý UDP Discovery (Broadcast & Listen)

Cơ chế UDP Discovery hoạt động như thế nào?
============================================
1. BROADCASTER (luồng gửi):
   - Tạo UDP socket, bật SO_BROADCAST.
   - Mỗi 3 giây gửi HELLO tới 2 địa chỉ:
       a. "<broadcast>" / "255.255.255.255"  → các máy khác trong LAN
       b. "127.255.255.255"                  → fallback cho localhost (cùng máy)
   Lý do cần gửi 2 lần:
     Linux thường DROP gói 255.255.255.255 trên loopback interface.
     127.255.255.255 là địa chỉ broadcast của subnet 127.0.0.0/8 (loopback),
     kernel sẽ deliver gói này cho tất cả socket đang bind ("", 9999) trên
     cùng máy mà không phải qua network interface vật lý.

2. LISTENER (luồng nhận):
   - Tạo UDP socket riêng.
   - Set SO_REUSEADDR và SO_REUSEPORT TRƯỚC khi bind() — thứ tự bắt buộc.
   - bind(("", 9999)) = lắng nghe mọi interface, nhận cả 255.255.255.255
     lẫn 127.255.255.255 và unicast.
   - Self-filter: bỏ qua gói có (name, tcp_port) trùng với bản thân.

3. TIMEOUT CHECKER:
   - Chạy trong luồng Listener sau mỗi lần recv / timeout.
   - Xóa peer im lặng > 10 giây.
"""

import socket
import threading
import time
import logging

from models import state
from protocol import (
    encode_hello, decode_hello,
    UDP_PORT, UDP_BROADCAST_ADDR,
    UDP_BROADCAST_INTERVAL, PEER_TIMEOUT,
)

logger = logging.getLogger(__name__)

# Địa chỉ broadcast phụ cho loopback (đảm bảo delivery trên cùng máy)
UDP_LOOPBACK_BROADCAST = "127.255.255.255"


# ---------------------------------------------------------------------------
# Broadcaster Thread
# ---------------------------------------------------------------------------

def broadcaster_thread(print_fn=None):
    """
    Luồng gửi broadcast UDP định kỳ.

    Gửi 2 lần mỗi chu kỳ:
      1. "<broadcast>" → LAN (các máy vật lý khác)
      2. "127.255.255.255" → loopback subnet (các instance khác trên cùng máy)
    """
    # AF_INET + SOCK_DGRAM = UDP socket IPv4
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # SO_BROADCAST: bắt buộc để gửi đến địa chỉ broadcast.
    # Thiếu option này → sendto() raise PermissionError (errno 13).
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

    logger.info(
        f"[UDP] Broadcaster started → '{state.my_name}' "
        f"(port {state.my_tcp_port}), broadcasting on UDP:{UDP_PORT}"
    )

    while not state.stop_event.is_set():
        try:
            packet = encode_hello(state.my_name, state.my_tcp_port, state.my_status)

            # --- Gửi lần 1: LAN broadcast (cho máy vật lý khác) ---
            # "<broadcast>" là alias portable hơn "255.255.255.255";
            # Python/OS tự resolve thành địa chỉ broadcast của interface phù hợp.
            sock.sendto(packet, ("<broadcast>", UDP_PORT))

            # --- Gửi lần 2: Loopback subnet broadcast (fallback cùng máy) ---
            # Linux kernel KHÔNG forward gói 255.255.255.255 qua loopback.
            # 127.255.255.255 là broadcast của 127.0.0.0/8 → kernel deliver
            # nội bộ, không cần qua card mạng vật lý.
            sock.sendto(packet, (UDP_LOOPBACK_BROADCAST, UDP_PORT))

            logger.debug(
                f"[UDP] Sent HELLO x2 ({len(packet)} bytes each) "
                f"→ <broadcast> + {UDP_LOOPBACK_BROADCAST}"
            )

        except OSError as e:
            logger.error(f"[UDP] Broadcast error: {e}")

        # Ngủ từng giây để stop_event được kiểm tra nhanh khi /quit
        for _ in range(int(UDP_BROADCAST_INTERVAL)):
            if state.stop_event.is_set():
                break
            time.sleep(1.0)

    sock.close()
    logger.info("[UDP] Broadcaster stopped.")


# ---------------------------------------------------------------------------
# Listener Thread
# ---------------------------------------------------------------------------

def listener_thread(notify_fn=None):
    """
    Luồng lắng nghe gói UDP từ các peer.

    Socket options phải được set theo đúng thứ tự:
      setsockopt(SO_REUSEADDR) → setsockopt(SO_REUSEPORT) → bind()
    Sau khi bind() đã được gọi, thay đổi SO_REUSEPORT không còn hiệu lực.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # --- BƯỚC 1: SO_REUSEADDR (TRƯỚC bind) ---
    # Cho phép bind lại port ngay sau khi process trước đóng socket,
    # không phải chờ hết TIME_WAIT (~60 giây). Cần thiết khi restart app.
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    # --- BƯỚC 2: SO_REUSEPORT (TRƯỚC bind) ---
    # Cho phép NHIỀU process/socket cùng bind vào 1 port — cốt lõi để
    # Quan (5001) và Minh (5002) cùng nhận gói trên UDP:9999 trên cùng máy.
    # hasattr() kiểm tra hằng số tồn tại (Python/OS compile-time).
    # try/except OSError bắt trường hợp kernel runtime không hỗ trợ.
    if hasattr(socket, "SO_REUSEPORT"):
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except OSError:
            logger.warning("[UDP] SO_REUSEPORT not supported by this kernel.")
    else:
        logger.warning("[UDP] SO_REUSEPORT not available (Windows?).")

    # --- BƯỚC 3: bind() — SAU tất cả setsockopt ---
    # ("", 9999) = ("0.0.0.0", 9999): nhận datagram từ mọi interface,
    # bao gồm eth0 (LAN), lo (loopback), và 127.255.255.255.
    sock.bind(("", UDP_PORT))
    sock.settimeout(1.0)   # Timeout để vòng lặp kiểm tra stop_event định kỳ

    last_timeout_check = time.time()
    logger.info(f"[UDP] Listener started → bound to 0.0.0.0:{UDP_PORT}")

    while not state.stop_event.is_set():
        try:
            # recvfrom trả về (data, (sender_ip, sender_udp_port))
            # sender_udp_port là ephemeral port của OS, KHÁC với tcp_port trong payload
            data, (sender_ip, _sender_udp_port) = sock.recvfrom(1024)

            parsed = decode_hello(data)
            if parsed is None:
                continue  # Gói không hợp lệ / từ app khác, bỏ qua

            # --- Self-filter: bỏ qua gói của chính instance này ---
            # ❌ KHÔNG dùng sender_ip: 2 instance cùng máy có cùng IP → lọc oan nhau.
            # ✅ Dùng (name + tcp_port) từ PAYLOAD — định danh duy nhất của instance.
            if (parsed["tcp_port"] == state.my_tcp_port
                    and parsed["name"] == state.my_name):
                continue

            # --- Cập nhật danh sách peer ---
            is_new = state.upsert_peer(
                name=parsed["name"],
                ip=sender_ip,
                tcp_port=parsed["tcp_port"],
                status=parsed["status"],
            )

            if is_new and notify_fn:
                notify_fn(
                    f"✅ {parsed['name']} "
                    f"({sender_ip}:{parsed['tcp_port']}) đã tham gia mạng!"
                )

        except socket.timeout:
            pass  # Bình thường: không có gói nào trong 1 giây → tiếp tục
        except OSError as e:
            if not state.stop_event.is_set():
                logger.error(f"[UDP] Listener error: {e}")

        # --- Kiểm tra peer timeout (mỗi 2 giây) ---
        now = time.time()
        if now - last_timeout_check >= 2.0:
            last_timeout_check = now
            removed_names = state.remove_timed_out_peers()
            for name in removed_names:
                logger.info(f"[UDP] '{name}' timed out → removed")
                if notify_fn:
                    notify_fn(f"❌ {name} đã rời khỏi mạng (timeout).")

    sock.close()
    logger.info("[UDP] Listener stopped.")


# ---------------------------------------------------------------------------
# Khởi động các luồng UDP
# ---------------------------------------------------------------------------

def start_udp_threads(notify_fn=None) -> tuple[threading.Thread, threading.Thread]:
    """
    Tạo và start 2 luồng UDP (broadcaster + listener).

    Returns:
        (t_broadcast, t_listen) để main.py có thể join() khi thoát.
    """
    t_broadcast = threading.Thread(
        target=broadcaster_thread,
        args=(notify_fn,),
        name="UDP-Broadcaster",
        daemon=True,
    )
    t_listen = threading.Thread(
        target=listener_thread,
        args=(notify_fn,),
        name="UDP-Listener",
        daemon=True,
    )

    t_broadcast.start()
    t_listen.start()

    return t_broadcast, t_listen
