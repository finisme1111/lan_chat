"""
udp_handler.py - Xử lý UDP Discovery (Broadcaster & Listener)

Cơ chế hoạt động đa nền tảng (Linux & Windows):
1. Broadcaster (Gửi định kỳ 3s):
   - Socket UDP bật SO_BROADCAST.
   - Gửi tới "<broadcast>" (mạng LAN thực tế).
   - Gửi dự phòng tới "127.255.255.255" (loopback broadcast cho Linux test localhost).
   - Mỗi lệnh sendto được bọc trong try-except OSError riêng biệt để tránh
     lỗi rớt luồng trên Windows (khi loopback broadcast không được hỗ trợ).

2. Listener (Lắng nghe cổng UDP 9999):
   - Thiết lập SO_REUSEADDR và kiểm tra an toàn SO_REUSEPORT bằng hasattr()
     trước khi bind() (Windows không hỗ trợ SO_REUSEPORT).
   - Bắt ngoại lệ ConnectionResetError (WSAECONNRESET 10054) - lỗi kinh điển
     trên hệ điều hành Windows khi nhận UDP socket.
   - Lọc gói tin của chính mình bằng instance ID hoặc UDP source port cho HELLO cũ.
   - Tự động phát hiện và loại bỏ các peer không hoạt động sau 10 giây (timeout).
"""

import logging
import socket
import threading
import time
from typing import Callable, Optional, Tuple

from models import state
from protocol import (
    encode_hello,
    decode_hello,
    MAX_UDP_DATAGRAM,
    UDP_PORT,
    UDP_BROADCAST_ADDR,
    UDP_LOOPBACK_ADDR,
    UDP_BROADCAST_INTERVAL,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Luồng Broadcaster
# ---------------------------------------------------------------------------

def broadcaster_thread(sock: socket.socket):
    """
    Luồng gửi gói tin quảng bá UDP HELLO mỗi 3 giây.
    """
    logger.info(f"[UDP] Broadcaster khởi động -> Danh tính: {state.my_name} ({state.my_tcp_port})")

    with sock:
        while not state.stop_event.is_set():
            name, port, status = state.my_name, state.my_tcp_port, state.my_status
            packets = (encode_hello(name, port, status, state.instance_id), encode_hello(name, port, status))
            for address in dict.fromkeys((UDP_BROADCAST_ADDR, UDP_LOOPBACK_ADDR)):
                for packet in packets:
                    try:
                        sock.sendto(packet, (address, UDP_PORT))
                    except OSError as e:
                        logger.debug("[UDP] Không gửi được tới %s: %s", address, e)
            state.stop_event.wait(UDP_BROADCAST_INTERVAL)
    logger.info("[UDP] Broadcaster đã dừng an toàn.")


# ---------------------------------------------------------------------------
# Luồng Listener
# ---------------------------------------------------------------------------

def _open_listener() -> socket.socket:
    """
    Bind UDP before reporting discovery readiness.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, "SO_REUSEPORT"):
            try:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            except OSError:
                pass
        sock.bind(("", UDP_PORT))
        sock.settimeout(1.0)
    except OSError:
        sock.close()
        raise
    return sock


def listener_thread(sock: socket.socket, broadcast_port: int, notify_fn: Optional[Callable[[str], None]] = None):
    """Lắng nghe UDP HELLO và kiểm tra expiry cả khi datagram bị bỏ qua."""
    last_timeout_check = time.monotonic()
    logger.info(f"[UDP] Listener đang lắng nghe trên 0.0.0.0:{UDP_PORT}")
    while not state.stop_event.is_set():
        try:
            data, (sender_ip, sender_port) = sock.recvfrom(MAX_UDP_DATAGRAM + 1)
            parsed = decode_hello(data)
            if parsed is not None:
                instance_id = parsed.get("instance_id")
                is_self = instance_id == state.instance_id or (
                    instance_id is None and
                    (sender_port, parsed["tcp_port"], parsed["name"]) == (
                        broadcast_port, state.my_tcp_port, state.my_name,
                    )
                )
                if not is_self:
                    is_new = state.upsert_peer(
                        name=parsed["name"], ip=sender_ip, tcp_port=parsed["tcp_port"],
                        status=parsed["status"], instance_id=instance_id,
                    )
                    if is_new and notify_fn:
                        notify_fn(f"[*] {parsed['name']} ({sender_ip}:{parsed['tcp_port']}) đã online.")

        except socket.timeout:
            # Hết thời gian chờ 1s mà không có gói tin nào -> Bình thường
            pass
        except ConnectionResetError:
            # Trên Windows, nếu gói UDP gửi đi trước đó bị từ chối (ICMP Port Unreachable),
            # lệnh recvfrom tiếp theo sẽ ném ra ConnectionResetError (WSAECONNRESET 10054).
            # Bắt ngoại lệ này giúp ứng dụng không bị văng crash trên hệ điều hành Windows!
            pass
        except OSError as e:
            if sock.fileno() == -1:
                break
            if not state.stop_event.is_set():
                logger.debug(f"[UDP] Listener socket error: {e}")

        # Kiểm tra và xóa các peer bị mất kết nối (timeout > 10s)
        now = time.monotonic()
        if now - last_timeout_check >= 1.0:
            last_timeout_check = now
            removed = state.remove_timed_out_peers()
            for peer_name in removed:
                if notify_fn:
                    notify_fn(f"[*] {peer_name} đã offline (hết thời gian chờ).")

    try:
        sock.close()
    except OSError:
        pass
    logger.info("[UDP] Listener đã dừng an toàn.")


# ---------------------------------------------------------------------------
# Khởi động các luồng UDP
# ---------------------------------------------------------------------------

def start_udp_threads(notify_fn: Optional[Callable[[str], None]] = None) -> Tuple[threading.Thread, threading.Thread]:
    """
    Khởi tạo và chạy các luồng UDP Broadcaster và Listener.
    """
    listener = _open_listener()
    broadcaster = None
    try:
        broadcaster = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        broadcaster.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        broadcaster.bind(("", 0))
        broadcast_port = broadcaster.getsockname()[1]
    except OSError:
        listener.close()
        if broadcaster is not None:
            broadcaster.close()
        raise

    t_broadcaster = threading.Thread(
        target=broadcaster_thread,
        args=(broadcaster,),
        name="UDP-Broadcaster",
        daemon=True,
    )
    t_listener = threading.Thread(
        target=listener_thread,
        args=(listener, broadcast_port, notify_fn),
        name="UDP-Listener",
        daemon=True,
    )

    try:
        t_listener.start()
    except RuntimeError:
        listener.close()
        broadcaster.close()
        raise
    try:
        t_broadcaster.start()
    except RuntimeError:
        listener.close()
        broadcaster.close()
        t_listener.join(timeout=2.0)
        raise

    return t_broadcaster, t_listener
