"""
protocol.py - Định nghĩa cấu trúc khung gói tin (Message Framing & Protocol)

Format giao thức:
- UDP Discovery:
    HELLO|<name>|<tcp_port>|<status>
- TCP Handshake:
    HELLO|<name>|<tcp_port>|<conn_type>|
- TCP Chat Message (Length-Prefixed):
    MSG|<length>|<content_utf8>
- TCP File Transfer:
    FILE|<filename>|<filesize>|<raw_binary_bytes>
"""

import os
from typing import Optional, Dict, Any

# ---------------------------------------------------------------------------
# Hằng số Protocol
# ---------------------------------------------------------------------------
UDP_PORT = 9999
UDP_BROADCAST_ADDR = "<broadcast>"
UDP_LOOPBACK_ADDR = "127.255.255.255"
UDP_BROADCAST_INTERVAL = 3.0
PEER_TIMEOUT = 10.0

DELIMITER = "|"

# Giới hạn an toàn chống tấn công DoS/Malformed data
MAX_FIELD_LEN = 4096            # Tối đa 4KB cho 1 trường header
MAX_MSG_LEN = 10 * 1024 * 1024  # Tối đa 10MB cho 1 tin nhắn văn bản

# Định danh loại tin nhắn
MSG_TYPE_HELLO = "HELLO"
MSG_TYPE_MSG   = "MSG"
MSG_TYPE_FILE  = "FILE"
MSG_TYPE_BYE   = "BYE"

__all__ = [
    "UDP_PORT",
    "UDP_BROADCAST_ADDR",
    "UDP_LOOPBACK_ADDR",
    "UDP_BROADCAST_INTERVAL",
    "PEER_TIMEOUT",
    "DELIMITER",
    "MAX_FIELD_LEN",
    "MAX_MSG_LEN",
    "MSG_TYPE_HELLO",
    "MSG_TYPE_MSG",
    "MSG_TYPE_FILE",
    "MSG_TYPE_BYE",
    "encode_hello",
    "decode_hello",
    "encode_msg",
    "encode_file_header",
    "encode_bye",
]


# ---------------------------------------------------------------------------
# UDP Packet Helpers
# ---------------------------------------------------------------------------

def encode_hello(name: str, tcp_port: int, status: str) -> bytes:
    """Tạo gói tin broadcast UDP HELLO."""
    # Làm sạch các ký tự phân tách nếu có trong name
    clean_name = name.replace(DELIMITER, "_").strip() or "Anonymous"
    packet = f"{MSG_TYPE_HELLO}{DELIMITER}{clean_name}{DELIMITER}{tcp_port}{DELIMITER}{status}"
    return packet.encode("utf-8", errors="replace")


def decode_hello(data: bytes) -> Optional[Dict[str, Any]]:
    """
    Phân tích gói tin UDP HELLO.
    Trả về None nếu gói tin rác hoặc sai cấu trúc.
    """
    try:
        text = data.decode("utf-8", errors="ignore").strip()
        parts = text.split(DELIMITER)
        if len(parts) != 4 or parts[0] != MSG_TYPE_HELLO:
            return None

        tcp_port = int(parts[2])
        if not (1 <= tcp_port <= 65535):
            return None

        status = parts[3].strip().lower()
        if status not in ("available", "busy", "away"):
            status = "available"

        return {
            "name": parts[1].strip() or "Unknown",
            "tcp_port": tcp_port,
            "status": status,
        }
    except (ValueError, IndexError):
        return None


# ---------------------------------------------------------------------------
# TCP Framing Helpers
# ---------------------------------------------------------------------------

def encode_msg(content: str) -> bytes:
    """
    Đóng khung tin nhắn văn bản TCP:
    MSG|<length>|<content_utf8>
    """
    content_bytes = content.encode("utf-8", errors="replace")
    header = f"{MSG_TYPE_MSG}{DELIMITER}{len(content_bytes)}{DELIMITER}".encode("utf-8")
    return header + content_bytes


def encode_file_header(filename: str, filesize: int) -> bytes:
    """
    Tạo header cho gói tin truyền file TCP:
    FILE|<clean_filename>|<filesize>|
    """
    # Lấy tên file gốc an toàn, loại bỏ directory traversal
    clean_filename = os.path.basename(filename).replace(DELIMITER, "_").strip()
    if not clean_filename:
        clean_filename = "unnamed_file"
    header = f"{MSG_TYPE_FILE}{DELIMITER}{clean_filename}{DELIMITER}{filesize}{DELIMITER}"
    return header.encode("utf-8")


def encode_bye() -> bytes:
    """Tạo gói tin BYE thông báo ngắt kết nối chủ động."""
    return f"{MSG_TYPE_BYE}{DELIMITER}".encode("utf-8")
