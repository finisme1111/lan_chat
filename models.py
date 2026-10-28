"""
models.py - Cấu trúc dữ liệu và trạng thái chia sẻ toàn cục (Shared State)

Mục đích:
- Lưu trữ danh sách peer online, lịch sử chat, các kết nối TCP đang mở.
- Thread-safe tuyệt đối: sử dụng threading.Lock() (Mutex) cho dữ liệu chia sẻ
  và conn_lock riêng biệt cho socket connections để tránh deadlock.
- Tương thích đa nền tảng: Linux & Windows.
"""

import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Cấu trúc dữ liệu Peer
# ---------------------------------------------------------------------------

@dataclass
class Peer:
    """
    Đại diện cho một người dùng khác trong mạng LAN.

    Attributes:
        name:      Tên hiển thị của peer (từ gói UDP HELLO).
        ip:        Địa chỉ IP của peer.
        tcp_port:  Port TCP mà peer lắng nghe kết nối.
        status:    Trạng thái: "available" | "busy".
        last_seen: Timestamp (time.time()) lần cuối nhận HELLO.
    """
    name: str
    ip: str
    tcp_port: int
    status: str = "available"
    last_seen: float = field(default_factory=time.time)

    def is_timed_out(self, timeout_sec: float = 10.0) -> bool:
        """Kiểm tra peer đã quá thời gian chờ (10s không gửi HELLO) hay chưa."""
        return (time.time() - self.last_seen) > timeout_sec

    def __str__(self) -> str:
        status_icon = "🟢" if self.status == "available" else ("🟡" if self.status == "away" else "🔴")
        return f"{status_icon} {self.name} ({self.ip}:{self.tcp_port}) [{self.status}]"


# ---------------------------------------------------------------------------
# Shared State - Singleton Thread-safe
# ---------------------------------------------------------------------------

class SharedState:
    """
    Quản lý toàn bộ trạng thái runtime của ứng dụng.
    Tất cả các thao tác đọc/ghi vào dữ liệu chia sẻ đều được bảo vệ bởi Lock.
    """

    def __init__(self):
        # Thông tin của chính mình
        self.my_name: str = ""
        self.my_tcp_port: int = 0
        self.my_ip: str = ""
        self.my_status: str = "available"

        # Danh sách peer online: key = "ip:tcp_port", value = Peer
        self.peer_list: Dict[str, Peer] = {}

        # Khóa của partner hiện đang chat cùng (hoặc None nếu đang ở màn hình ngoài)
        self.current_partner: Optional[str] = None

        # Lock bảo vệ peer_list, chat_history, unread_counts
        self.lock: threading.Lock = threading.Lock()

        # Lịch sử chat theo từng peer_key
        self.chat_history: Dict[str, List[str]] = {}

        # Số tin nhắn chưa đọc
        self.unread_counts: Dict[str, int] = {}

        # Event báo hiệu dừng toàn bộ các luồng khi /quit
        self.stop_event: threading.Event = threading.Event()

        # Quản lý kết nối TCP (giữ kết nối để tái sử dụng)
        # conn_lock tách rời để tránh contention/deadlock với data lock
        self.connections: Dict[str, Any] = {}
        self.conn_lock: threading.Lock = threading.Lock()

    # --- Các hàm thao tác thread-safe ---

    @staticmethod
    def get_peer_key(ip: str, tcp_port: int) -> str:
        """Tạo khóa định danh duy nhất cho peer."""
        return f"{ip}:{tcp_port}"

    def upsert_peer(self, name: str, ip: str, tcp_port: int, status: str) -> bool:
        """
        Cập nhật hoặc thêm mới peer.
        Trả về True nếu đây là peer mới tham gia mạng.
        """
        key = self.get_peer_key(ip, tcp_port)
        with self.lock:
            is_new = key not in self.peer_list
            if is_new:
                self.peer_list[key] = Peer(
                    name=name, ip=ip, tcp_port=tcp_port, status=status
                )
                # Chỉ khởi tạo nếu chưa có trong history (tránh ghi đè khi đã load từ JSON)
                if key not in self.chat_history:
                    self.chat_history[key] = []
                if key not in self.unread_counts:
                    self.unread_counts[key] = 0
            else:
                peer = self.peer_list[key]
                peer.name = name
                peer.status = status
                peer.last_seen = time.time()
            return is_new

    def remove_timed_out_peers(self) -> List[str]:
        """
        Xóa các peer không còn hoạt động (>10s không có HELLO).
        Trả về danh sách tên các peer đã bị loại bỏ.
        """
        removed = []
        with self.lock:
            timed_out_keys = [
                k for k, p in self.peer_list.items() if p.is_timed_out()
            ]
            for key in timed_out_keys:
                peer = self.peer_list.pop(key, None)
                if peer:
                    removed.append(peer.name)
                # Đưa về dấu nhắc chính nếu partner bị timeout
                if self.current_partner == key:
                    self.current_partner = None
        return removed

    def get_peer_list_snapshot(self) -> List[Peer]:
        """Lấy bản sao danh sách peer an toàn (shallow copy)."""
        with self.lock:
            return list(self.peer_list.values())

    def set_status(self, status: str):
        """Thay đổi trạng thái cá nhân (available | busy)."""
        with self.lock:
            self.my_status = status

    def reset_partner_if(self, peer_key: str):
        """
        Đưa dấu nhắc lệnh về trạng thái chính nếu đối tác đang chat bị ngắt kết nối.
        """
        with self.lock:
            if self.current_partner == peer_key:
                self.current_partner = None


# Khởi tạo singleton dùng chung toàn dự án
state = SharedState()
