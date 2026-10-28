"""
protocol.py - Định nghĩa định dạng gói tin (Protocol / Message Framing)

Tại sao cần "Framing" (đóng khung)?
- UDP: Mỗi lần sendto() là 1 datagram độc lập, receiver nhận nguyên vẹn.
  → Framing đơn giản: encode chuỗi thành bytes.
- TCP: Là luồng byte liên tục (stream). Nhiều lần send() có thể được ghép
  thành 1 lần recv(), hoặc 1 lần send() bị cắt thành nhiều recv().
  → PHẢI có header chứa độ dài để receiver biết điểm kết thúc của 1 message.

Format gói tin:
  UDP: "HELLO|<name>|<tcp_port>|<status>"   ← text đơn giản, dùng | làm delimiter
  TCP: "MSG|<length>|<content>"              ← length-prefixed framing (Bước 2)
       "FILE|<filename>|<size>|<bytes>"      ← file transfer (Bước 2)
"""

# ---------------------------------------------------------------------------
# Hằng số Protocol
# ---------------------------------------------------------------------------

UDP_PORT = 9999          # Port cố định cho UDP broadcast discovery
UDP_BROADCAST_ADDR = "255.255.255.255"  # Địa chỉ broadcast toàn mạng LAN
UDP_BROADCAST_INTERVAL = 3.0            # Gửi HELLO mỗi 3 giây
PEER_TIMEOUT = 10.0      # Peer bị coi là offline sau 10 giây không thấy HELLO

DELIMITER = "|"          # Ký tự phân tách trường trong gói tin

# Các loại message (type identifiers)
MSG_TYPE_HELLO = "HELLO"
MSG_TYPE_MSG   = "MSG"
MSG_TYPE_FILE  = "FILE"
MSG_TYPE_BYE   = "BYE"


# ---------------------------------------------------------------------------
# UDP Packet: HELLO (Discovery)
# ---------------------------------------------------------------------------

def encode_hello(name: str, tcp_port: int, status: str) -> bytes:
    """
    Tạo gói HELLO để broadcast UDP.

    Format: "HELLO|<name>|<tcp_port>|<status>"
    Ví dụ:  "HELLO|Quan|5001|available"

    encode("utf-8") chuyển string → bytes (vì socket chỉ truyền bytes).
    """
    packet = f"{MSG_TYPE_HELLO}{DELIMITER}{name}{DELIMITER}{tcp_port}{DELIMITER}{status}"
    return packet.encode("utf-8")


def decode_hello(data: bytes) -> dict | None:
    """
    Phân tích gói HELLO nhận được từ UDP.

    Trả về dict {"name", "tcp_port", "status"} hoặc None nếu gói không hợp lệ.
    Luôn dùng try/except khi parse dữ liệu mạng — không tin tưởng input ngoài.
    """
    try:
        text = data.decode("utf-8").strip()
        parts = text.split(DELIMITER)

        # Kiểm tra: phải có đúng 4 trường và type phải là "HELLO"
        if len(parts) != 4 or parts[0] != MSG_TYPE_HELLO:
            return None

        return {
            "name":     parts[1],
            "tcp_port": int(parts[2]),
            "status":   parts[3],
        }
    except (ValueError, UnicodeDecodeError):
        # int() raise ValueError nếu tcp_port không phải số
        # decode() raise UnicodeDecodeError nếu bytes không hợp lệ
        return None


# ---------------------------------------------------------------------------
# TCP Framing (sẽ dùng đầy đủ ở Bước 2, định nghĩa sớm để nhất quán)
# ---------------------------------------------------------------------------

def encode_msg(content: str) -> bytes:
    """
    Đóng khung tin nhắn TCP theo format length-prefixed.

    Format: "MSG|<length>|<content>"
    Ví dụ:  "MSG|11|Hello world"

    Tại sao cần length?
    - TCP là stream: recv(4096) có thể nhận được "MSG|11|Hello worldMSG|5|Hi"
      (2 message ghép lại). Receiver dùng <length> để biết cắt ở đâu.
    """
    content_bytes = content.encode("utf-8")
    header = f"{MSG_TYPE_MSG}{DELIMITER}{len(content_bytes)}{DELIMITER}"
    return header.encode("utf-8") + content_bytes


def encode_file_header(filename: str, file_size: int) -> bytes:
    """
    Tạo header cho gói tin gửi file.

    Format: "FILE|<filename>|<size>|"
    Sau header sẽ là <size> bytes dữ liệu raw của file.
    """
    header = f"{MSG_TYPE_FILE}{DELIMITER}{filename}{DELIMITER}{file_size}{DELIMITER}"
    return header.encode("utf-8")


def decode_tcp_header(header_str: str) -> dict | None:
    """
    Parse header của TCP frame.
    Trả về dict với 'type' và các trường tương ứng, hoặc None nếu lỗi.
    """
    try:
        parts = header_str.split(DELIMITER)
        msg_type = parts[0]

        if msg_type == MSG_TYPE_MSG and len(parts) >= 3:
            return {"type": MSG_TYPE_MSG, "length": int(parts[1])}

        elif msg_type == MSG_TYPE_FILE and len(parts) >= 4:
            return {
                "type":     MSG_TYPE_FILE,
                "filename": parts[1],
                "size":     int(parts[2]),
            }

        elif msg_type == MSG_TYPE_BYE:
            return {"type": MSG_TYPE_BYE}

        return None
    except (ValueError, IndexError):
        return None

