"""
models.py - Cấu trúc dữ liệu và trạng thái chia sẻ toàn cục (Shared State)

Tại sao cần module này?
- Nhiều luồng (thread) cùng đọc/ghi danh sách peer → cần Lock để tránh race condition.
- Tập trung dữ liệu vào 1 chỗ → dễ debug, không bị circular import.
"""

import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional


# ---------------------------------------------------------------------------
# Cấu trúc dữ liệu Peer
# ---------------------------------------------------------------------------

@dataclass
class Peer:
    """
    Đại diện cho một người dùng khác đang online trong mạng LAN.

    Attributes:
        name:      Tên hiển thị của peer (nhận từ gói UDP HELLO).
        ip:        Địa chỉ IP của peer (lấy từ địa chỉ gói UDP đến).
        tcp_port:  Port TCP mà peer đang lắng nghe kết nối chat.
        status:    Trạng thái hiện tại: "available" | "busy".
        last_seen: Timestamp (time.time()) lần cuối nhận gói HELLO từ peer.
                   Dùng để phát hiện peer timeout (> 10 giây không thấy = offline).
    """
    name: str
    ip: str
    tcp_port: int
    status: str = "available"
    last_seen: float = field(default_factory=time.time)

    def is_timed_out(self, timeout_sec: float = 10.0) -> bool:
        """Trả về True nếu peer không gửi HELLO trong vòng timeout_sec giây."""
        return (time.time() - self.last_seen) > timeout_sec

    def __str__(self) -> str:
        status_icon = "🟢" if self.status == "available" else "🔴"
        return f"{status_icon} {self.name} ({self.ip}:{self.tcp_port}) [{self.status}]"


# ---------------------------------------------------------------------------
# Shared State - Trạng thái chia sẻ giữa các luồng
# ---------------------------------------------------------------------------

class SharedState:
    """
    Singleton chứa toàn bộ trạng thái runtime của ứng dụng.

    Thiết kế:
    - Dùng threading.Lock() (Mutex) để bảo vệ peer_list và chat_history.
    - Lock phải được acquire() trước khi đọc/ghi, release() sau khi xong.
    - Dùng 'with self.lock:' để đảm bảo Lock luôn được giải phóng kể cả khi có exception.
    """

    def __init__(self):
        # --- Thông tin của bản thân (được set bởi main.py khi khởi động) ---
        self.my_name: str = ""
        self.my_tcp_port: int = 0
        self.my_ip: str = ""

        # Trạng thái của bản thân: "available" | "busy"
        # "busy" khi đang trong cuộc chat với ai đó
        self.my_status: str = "available"

        # --- Danh sách peer online ---
        # Key: "ip:tcp_port" (định danh duy nhất cho mỗi peer)
        # Value: đối tượng Peer
        # PHẢI dùng self.lock khi truy cập dict này!
        self.peer_list: Dict[str, Peer] = {}

        # --- Partner hiện tại đang chat ---
        # None nếu không đang chat với ai
        self.current_partner: Optional[str] = None  # key trong peer_list

        # --- Lock bảo vệ dữ liệu chia sẻ ---
        # threading.Lock() là Mutual Exclusion lock (Mutex):
        # - Chỉ 1 luồng được giữ lock tại 1 thời điểm.
        # - Luồng khác gọi acquire() sẽ bị block cho đến khi lock được release().
        self.lock: threading.Lock = threading.Lock()

        # --- Lịch sử chat ---
        # Key: peer_key ("ip:port"), Value: list[str] (các dòng chat)
        self.chat_history: Dict[str, list] = {}

        # --- Unread message count ---
        # Key: peer_key, Value: int (số tin nhắn chưa đọc)
        self.unread_counts: Dict[str, int] = {}

        # --- Flag để dừng các luồng khi thoát ---
        # threading.Event() cho phép một luồng ra hiệu (set) cho luồng khác (wait/is_set).
        self.stop_event: threading.Event = threading.Event()

        # --- Kết nối TCP đang mở ---
        # Key: peer_key ("ip:port"), Value: socket.socket
        # Kết nối KHÔNG bị đóng khi /leave — chỉ đóng khi lỗi mạng hoặc /quit.
        # Dùng conn_lock riêng biệt để tránh deadlock với self.lock.
        self.connections: Dict[str, Any] = {}
        self.conn_lock: threading.Lock = threading.Lock()

    # --- Các hàm helper an toàn (thread-safe) ---

    def get_peer_key(self, ip: str, tcp_port: int) -> str:
        """Tạo khóa duy nhất cho peer."""
        return f"{ip}:{tcp_port}"

    def upsert_peer(self, name: str, ip: str, tcp_port: int, status: str) -> bool:
        """
        Thêm peer mới hoặc cập nhật thông tin peer đã có.
        Trả về True nếu đây là peer MỚI (để UI có thể thông báo).
        Thread-safe: dùng lock nội bộ.
        """
        key = self.get_peer_key(ip, tcp_port)
        with self.lock:
            is_new = key not in self.peer_list
            if is_new:
                self.peer_list[key] = Peer(
                    name=name, ip=ip, tcp_port=tcp_port, status=status
                )
                # Khởi tạo lịch sử và unread count CHỈ KHI chưa có.
                # Không ghi đè lịch sử đã được load từ file bởi history.load_history().
                if key not in self.chat_history:
                    self.chat_history[key] = []
                if key not in self.unread_counts:
                    self.unread_counts[key] = 0
            else:
                # Chỉ cập nhật các trường thay đổi
                peer = self.peer_list[key]
                peer.name = name
                peer.status = status
                peer.last_seen = time.time()
            return is_new

    def remove_timed_out_peers(self) -> list:
        """
        Xóa các peer đã timeout khỏi danh sách.
        Trả về danh sách tên các peer bị xóa (để hiển thị thông báo).
        Thread-safe.
        """
        removed = []
        with self.lock:
            timed_out_keys = [
                k for k, p in self.peer_list.items() if p.is_timed_out()
            ]
            for key in timed_out_keys:
                removed.append(self.peer_list[key].name)
                del self.peer_list[key]
                # Nếu peer đang được chat thì xóa trạng thái partner
                if self.current_partner == key:
                    self.current_partner = None
        return removed

    def get_peer_list_snapshot(self) -> list:
        """
        Trả về bản sao (snapshot) danh sách peer tại thời điểm gọi.
        Dùng snapshot thay vì trả về dict trực tiếp để tránh race condition
        khi luồng khác sửa dict trong lúc đang duyệt.
        """
        with self.lock:
            return list(self.peer_list.values())

    def set_status(self, status: str):
        """Cập nhật trạng thái của bản thân một cách thread-safe."""
        with self.lock:
            self.my_status = status


# ---------------------------------------------------------------------------
# Singleton instance - Dùng toàn dự án
# ---------------------------------------------------------------------------

# Tất cả các module khác sẽ import state từ đây:
#   from models import state
state = SharedState()

