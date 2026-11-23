"""
models.py - Cấu trúc dữ liệu và trạng thái chia sẻ toàn cục (Shared State)

Mục đích:
- Lưu trữ danh sách peer online, lịch sử chat, các kết nối TCP đang mở.
- Thread-safe tuyệt đối: sử dụng threading.Lock() (Mutex) cho dữ liệu chia sẻ
  và conn_lock riêng biệt cho socket connections để tránh deadlock.
- Tương thích đa nền tảng: Linux & Windows.
"""

import socket
import threading
import time
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from uuid import uuid4

from protocol import PEER_TIMEOUT

DEFAULT_DOWNLOAD_QUOTA_GIB = 8
GIB = 1024**3


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
        last_seen: Thời điểm monotonic nhận HELLO gần nhất.
    """
    name: str
    ip: str
    tcp_port: int
    status: str = "available"
    last_seen: float = field(default_factory=lambda: time.monotonic())
    endpoint_seen: float = field(default_factory=lambda: time.monotonic())
    instance_id: Optional[str] = None

    def is_timed_out(self, timeout_sec: float = PEER_TIMEOUT) -> bool:
        """Kiểm tra peer đã quá thời gian chờ (10s không gửi HELLO) hay chưa."""
        return time.monotonic() - self.last_seen >= timeout_sec

    def __str__(self) -> str:
        status_icon = "🟢" if self.status == "available" else ("🟡" if self.status == "away" else "🔴")
        return f"{status_icon} {self.name} ({self.ip}:{self.tcp_port}) [{self.status}]"


# ---------------------------------------------------------------------------
# Shared State - Singleton Thread-safe
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ChatConnection:
    sock: socket.socket
    outgoing: bool
    write_lock: threading.Lock = field(default_factory=threading.Lock, compare=False, repr=False)


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
        self.instance_id = uuid4().hex
        self.data_dir = Path(".")
        self.download_quota_bytes = DEFAULT_DOWNLOAD_QUOTA_GIB * GIB
        self.history_writable = True

        # Danh sách peer online: key = "ip:tcp_port", value = Peer
        self.peer_list: Dict[str, Peer] = {}
        self.peer_instances: Dict[str, str] = {}
        self.peer_endpoints: Dict[str, Tuple[str, float]] = {}
        self.listed_peer_keys: tuple[str, ...] = ()

        # Khóa của partner hiện đang chat cùng (hoặc None nếu đang ở màn hình ngoài)
        self.current_partner: Optional[str] = None
        self.reconnect_partner: Optional[str] = None

        # Lock bảo vệ peer_list, chat_history, unread_counts
        self.lock: threading.Lock = threading.Lock()

        # Lịch sử chat theo từng peer_key
        self.chat_history: Dict[str, List[str]] = {}

        # Số tin nhắn chưa đọc
        self.unread_counts: Dict[str, int] = {}
        self.first_unread: Dict[str, int] = {}

        # Event báo hiệu dừng toàn bộ các luồng khi /quit
        self.stop_event: threading.Event = threading.Event()

        # Quản lý kết nối TCP (giữ kết nối để tái sử dụng)
        # conn_lock tách rời để tránh contention/deadlock với data lock
        self.connections: Dict[str, ChatConnection] = {}
        self.pending_chat_dials: Dict[str, threading.Event] = {}
        self.dialing_chat_sockets: set[socket.socket] = set()
        self.tcp_workers: Dict[socket.socket, Tuple[threading.Thread, bool]] = {}
        self.conn_lock: threading.Lock = threading.Lock()

    # --- Các hàm thao tác thread-safe ---

    @staticmethod
    def get_peer_key(ip: str, tcp_port: int) -> str:
        """Tạo khóa định danh duy nhất cho peer."""
        return f"{ip}:{tcp_port}"

    def resolve_peer_key(self, ip: str, tcp_port: int) -> str:
        endpoint = self.get_peer_key(ip, tcp_port)
        with self.lock:
            key, _ = self.peer_endpoints.get(endpoint, (endpoint, 0.0))
            return key

    def upsert_peer(self, name: str, ip: str, tcp_port: int, status: str, instance_id: str | None = None) -> bool:
        """
        Cập nhật hoặc thêm mới peer.
        Trả về True nếu đây là peer mới tham gia mạng.
        """
        endpoint = self.get_peer_key(ip, tcp_port)
        with self.lock:
            now = time.monotonic()
            if instance_id is not None:
                key = self.peer_instances.get(instance_id, endpoint)
                previous = self.peer_list.get(key)
                if previous and previous.instance_id not in (None, instance_id):
                    self.peer_instances.pop(previous.instance_id, None)
                    self.peer_endpoints = {
                        addr: seen for addr, seen in self.peer_endpoints.items() if seen[0] != key
                    }
                self.peer_instances[instance_id] = key
                self.peer_endpoints[endpoint] = (key, now)
                legacy = self.peer_list.get(endpoint)
                if key != endpoint and legacy is not None and legacy.instance_id is None:
                    # Keep its history on disk under the original endpoint key.
                    del self.peer_list[endpoint]
            else:
                key, _ = self.peer_endpoints.get(endpoint, (endpoint, now))
                if endpoint in self.peer_endpoints:
                    self.peer_endpoints[endpoint] = (key, now)

            is_new = key not in self.peer_list
            if is_new:
                self.peer_list[key] = Peer(
                    name=name, ip=ip, tcp_port=tcp_port, status=status,
                    last_seen=now, endpoint_seen=now, instance_id=instance_id,
                )
            else:
                peer = self.peer_list[key]
                if instance_id is not None:
                    peer.instance_id = instance_id
                peer.name = name
                peer.status = status
                peer.last_seen = now
                if (peer.ip, peer.tcp_port) == (ip, tcp_port) or now - peer.endpoint_seen >= PEER_TIMEOUT:
                    peer.ip = ip
                    peer.tcp_port = tcp_port
                    peer.endpoint_seen = now
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
                    if peer.instance_id is not None:
                        self.peer_instances.pop(peer.instance_id, None)
                # Đưa về dấu nhắc chính nếu partner bị timeout
                if self.current_partner == key:
                    self.current_partner = None
                if self.reconnect_partner == key:
                    self.reconnect_partner = None
            now = time.monotonic()
            self.peer_endpoints = {
                addr: (key, last_seen) for addr, (key, last_seen) in self.peer_endpoints.items()
                if key in self.peer_list and now - last_seen < PEER_TIMEOUT
            }
        return removed

    def get_peer_list_snapshot(self) -> Dict[str, Peer]:
        """Lấy bản sao nhất quán của các peer để đọc ngoài lock."""
        with self.lock:
            return {key: replace(peer) for key, peer in self.peer_list.items()}

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
