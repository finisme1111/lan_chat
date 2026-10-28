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
   - Lọc gói tin của chính mình dựa vào (name, tcp_port) trong payload.
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
    UDP_PORT,
    UDP_BROADCAST_ADDR,
    UDP_LOOPBACK_ADDR,
    UDP_BROADCAST_INTERVAL,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Luồng Broadcaster
# ---------------------------------------------------------------------------

def broadcaster_thread(notify_fn: Optional[Callable[[str], None]] = None):
    """
    Luồng gửi gói tin quảng bá UDP HELLO mỗi 3 giây.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # Bắt buộc bật cờ SO_BROADCAST để hệ điều hành cho phép gửi gói broadcast
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    except OSError as e:
        logger.error(f"[UDP] Không thể thiết lập SO_BROADCAST: {e}")

    logger.info(f"[UDP] Broadcaster khởi động -> Danh tính: {state.my_name} ({state.my_tcp_port})")

    while not state.stop_event.is_set():
        try:
            packet = encode_hello(state.my_name, state.my_tcp_port, state.my_status)

            # 1. Gửi tới địa chỉ broadcast của mạng LAN thực tế
            try:
                sock.sendto(packet, (UDP_BROADCAST_ADDR, UDP_PORT))
            except OSError as e:
                logger.debug(f"[UDP] Gửi LAN broadcast thất bại: {e}")

            # 2. Gửi dự phòng tới loopback broadcast (đặc trị cho Linux khi test cùng máy)
            try:
                sock.sendto(packet, (UDP_LOOPBACK_ADDR, UDP_PORT))
            except OSError:
                # Trên Windows hoặc một số router, địa chỉ 127.255.255.255 sẽ báo lỗi WSAENETUNREACH
                # Khối try-except này bỏ qua an toàn mà không làm sập luồng
                pass

        except Exception as e:
            logger.error(f"[UDP] Lỗi bất ngờ trong broadcaster: {e}")

        # Chia nhỏ chu kỳ ngủ thành từng 0.5s để phản ứng nhanh khi nhận lệnh thoát /quit
        sleep_elapsed = 0.0
        while sleep_elapsed < UDP_BROADCAST_INTERVAL:
            if state.stop_event.is_set():
                break
            time.sleep(0.5)
            sleep_elapsed += 0.5

    try:
        sock.close()
    except OSError:
        pass
    logger.info("[UDP] Broadcaster đã dừng an toàn.")


# ---------------------------------------------------------------------------
# Luồng Listener
# ---------------------------------------------------------------------------

def listener_thread(notify_fn: Optional[Callable[[str], None]] = None):
    """
    Luồng lắng nghe gói tin UDP HELLO trên cổng 9999 và cập nhật trạng thái mạng.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # 1. Thiết lập SO_REUSEADDR cho phép tái sử dụng cổng nhanh chóng
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    # 2. Kiểm tra điều kiện an toàn cho SO_REUSEPORT (Bắt buộc theo chuẩn đa nền tảng)
    # Windows không có SO_REUSEPORT, Linux và macOS có hỗ trợ
    if hasattr(socket, "SO_REUSEPORT"):
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except OSError:
            pass

    # 3. Gắn socket vào tất cả interface mạng trên cổng UDP_PORT (9999)
    try:
        sock.bind(("", UDP_PORT))
    except OSError as e:
        logger.error(f"[UDP] Không thể bind cổng UDP {UDP_PORT}: {e}")
        if notify_fn:
            notify_fn(f"[!] Cổng UDP {UDP_PORT} đang bị chiếm dụng. Vui lòng kiểm tra lại tiến trình khác.")
        sock.close()
        return

    # Đặt timeout để vòng lặp định kỳ kiểm tra stop_event và peer timeout
    sock.settimeout(1.0)
    last_timeout_check = time.time()

    logger.info(f"[UDP] Listener đang lắng nghe trên 0.0.0.0:{UDP_PORT}")

    while not state.stop_event.is_set():
        try:
            data, (sender_ip, _) = sock.recvfrom(2048)
            parsed = decode_hello(data)
            if parsed is None:
                # Gói tin rác hoặc không đúng định dạng -> Bỏ qua
                continue

            # Bỏ qua gói tin phát ra từ chính instance này
            # (Phân biệt qua tcp_port và name vì khi test cùng máy IP sẽ giống nhau)
            if parsed["tcp_port"] == state.my_tcp_port and parsed["name"] == state.my_name:
                continue

            # Cập nhật hoặc thêm peer vào danh sách
            is_new = state.upsert_peer(
                name=parsed["name"],
                ip=sender_ip,
                tcp_port=parsed["tcp_port"],
                status=parsed["status"],
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
            if not state.stop_event.is_set():
                logger.debug(f"[UDP] Listener socket error: {e}")

        # Kiểm tra và xóa các peer bị mất kết nối (timeout > 10s)
        now = time.time()
        if now - last_timeout_check >= 2.0:
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
    t_broadcaster = threading.Thread(
        target=broadcaster_thread,
        args=(notify_fn,),
        name="UDP-Broadcaster",
        daemon=True,
    )
    t_listener = threading.Thread(
        target=listener_thread,
        args=(notify_fn,),
        name="UDP-Listener",
        daemon=True,
    )

    t_broadcaster.start()
    t_listener.start()

    return t_broadcaster, t_listener
