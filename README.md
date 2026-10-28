# 🖥️ LAN Chat & File Sharing P2P

Ứng dụng **chat và chia sẻ file ngang hàng (P2P)** trong mạng LAN, chạy hoàn toàn trên **giao diện console**. Không cần server trung tâm — các máy tự tìm thấy nhau qua UDP broadcast.

```
╔══════════════════════════════════════════════════╗
║          LAN Chat & File Sharing P2P             ║
╚══════════════════════════════════════════════════╝

[Quan|available]> /chat Minh
[OK]  Đang chat với Minh.

[Quan|available → Minh]> Chào Minh!
[Quan (bạn)] Chào Minh!

*** [Minh] Chào Quan! ***
```

---

## ✨ Tính năng

| Tính năng | Mô tả |
|---|---|
| 🔍 **UDP Discovery** | Tự động tìm thấy các máy khác trong LAN sau ~3 giây, không cần nhập IP thủ công |
| 💬 **Chat 1-1** | Nhắn tin real-time với bất kỳ peer nào đang online |
| 📢 **Broadcast** | Gửi thông báo đến toàn bộ peer online cùng lúc |
| 📁 **Gửi file** | Chuyển file qua kết nối TCP riêng biệt (không làm gián đoạn chat) |
| 📊 **File progress** | Thanh tiến độ `[████░░░░] 50%` khi gửi file lớn |
| 💾 **Persistent history** | Lịch sử chat được lưu tự động vào `chat_history.json`, đọc lại sau khi restart |
| 🔄 **Auto-reconnect** | Tự động kết nối lại với người đang chat khi mất kết nối tạm thời |
| ⌨️ **Tab Autocomplete** | Nhấn Tab để gợi ý lệnh, tên peer, đường dẫn file |
| 🎨 **Colored UI** | Giao diện màu sắc, input không bị đè bởi tin nhắn đến |
| 🔌 **Multi-instance** | Test nhiều người trên cùng 1 máy với các `--port` khác nhau |

---

## 🏗️ Kiến trúc

```
┌─────────────────────────────────────────────────────┐
│                     main.py                         │
│         Entry point · argparse · command loop       │
└──────┬──────────┬──────────┬──────────┬─────────────┘
       │          │          │          │
  udp_handler  tcp_handler  history   ui_handler
  ─────────── ─────────── ──────── ───────────
  Broadcaster Acceptor    Load /   prompt_toolkit
  Listener    Receiver    Save     LanChatCompleter
  Discovery   File xfer   JSON     patch_stdout
  Timeout     Reconnect
       │          │          │          │
       └──────────┴──────────┴──────────┘
                        │
               ┌────────┴────────┐
             models.py      protocol.py
             SharedState    UDP HELLO
             Peer           TCP Framing
             Locks          MSG / FILE
```

**Protocol tự thiết kế:**
```
UDP:  HELLO|<name>|<tcp_port>|<status>
TCP:  HELLO|<name>|<tcp_port>|<CHAT|FILE>|
      MSG|<length>|<content bytes>
      FILE|<filename>|<size>|<data bytes>
```

---

## 📋 Yêu cầu

- **Python** 3.10 trở lên (dùng tính năng `match`, `X | Y` type hint)
- **prompt_toolkit** ≥ 3.0.0
- **Kết nối mạng LAN** (hoặc loopback để test trên 1 máy)

---

## 🚀 Cài đặt & Chạy

### 🐧 Linux

```bash
# Clone hoặc copy thư mục dự án
cd lan_chat

# Cài thư viện (Arch Linux / Manjaro)
pip install prompt_toolkit --break-system-packages

# Các distro khác (Ubuntu, Fedora...)
pip3 install prompt_toolkit
# Hoặc dùng venv:
python3 -m venv .venv && source .venv/bin/activate && pip install prompt_toolkit

# Chạy
python3 main.py --name Quan --port 5001
```

> **Lưu ý Linux:** Cần quyền gửi UDP broadcast. Thông thường không cần `sudo`.  
> Nếu bị `Permission denied`, chạy: `sudo python3 main.py --name Quan --port 5001`

---

### 🍎 macOS

```bash
cd lan_chat

# Cài thư viện
pip3 install prompt_toolkit
# Hoặc nếu dùng Homebrew Python:
python3 -m pip install prompt_toolkit

# Chạy
python3 main.py --name Quan --port 5001
```

> **Lưu ý macOS:** `SO_REUSEPORT` được hỗ trợ đầy đủ. Test nhiều instance trên cùng máy hoạt động bình thường.

---

### 🪟 Windows

```powershell
# Mở PowerShell hoặc Command Prompt
cd lan_chat

# Cài thư viện
pip install prompt_toolkit

# Chạy
python main.py --name Quan --port 5001
```

> **⚠️ Lưu ý quan trọng trên Windows:**
>
> - `SO_REUSEPORT` không có sẵn trên Windows → code tự bỏ qua (dùng `hasattr` check), không bị crash.
> - Test **nhiều instance trên cùng 1 máy** có thể không hoạt động do Windows không cho phép 2 socket cùng bind `0.0.0.0:9999`. Hãy test trên **2 máy thật** trong cùng mạng LAN.
> - Nếu Windows Firewall chặn → cho phép Python qua Firewall khi được hỏi.
> - Dùng `python` thay vì `python3` nếu lệnh `python3` không nhận.

---

### Test nhiều người trên cùng 1 máy (Linux / macOS)

```bash
# Terminal 1
python3 main.py --name Quan --port 5001

# Terminal 2
python3 main.py --name Minh --port 5002

# Terminal 3 (tùy chọn)
python3 main.py --name An --port 5003
```

---

## 📖 Hướng dẫn sử dụng

### Tham số khởi động

```
python3 main.py --name <tên> --port <port> [--debug]
```

| Tham số | Mô tả | Ví dụ |
|---|---|---|
| `--name` | Tên hiển thị của bạn | `--name Quan` |
| `--port` | TCP port lắng nghe (1024–65535) | `--port 5001` |
| `--debug` | Bật log chi tiết (socket, thread) | `--debug` |

---

### Danh sách lệnh

```
┌─ Discovery ───────────────────────────────────────────┐
│  /list              Hiển thị peer đang online          │
│  /status <s>        Đổi trạng thái (available|busy)   │
├─ Chat 1-1 ────────────────────────────────────────────┤
│  /chat <tên>        Kết nối và bắt đầu chat            │
│  /send <tin nhắn>   Gửi tin (hoặc gõ thẳng không /send)│
│  /history           Xem lại lịch sử chat               │
│  /leave             Rời chat, giữ kết nối TCP          │
├─ Gửi File ────────────────────────────────────────────┤
│  /file <đường dẫn>  Gửi file đến peer hiện tại        │
├─ Broadcast ───────────────────────────────────────────┤
│  /broadcast <msg>   Gửi tin đến TẤT CẢ peer online    │
├─ Chung ───────────────────────────────────────────────┤
│  /help              Hiển thị trợ giúp                  │
│  /quit              Thoát (lưu lịch sử trước)          │
└───────────────────────────────────────────────────────┘
```

### Tab Autocomplete

Nhấn `Tab` sau khi gõ một phần lệnh:

| Gõ | Tab → | Kết quả |
|---|---|---|
| `/ch` | Tab | `/chat` |
| `/chat ` | Tab | Danh sách tên tất cả peer online |
| `/chat Mi` | Tab | `Minh` (peer tên bắt đầu bằng Mi) |
| `/status ` | Tab | `available` / `busy` |
| `/file ./` | Tab | Danh sách file trong thư mục hiện tại |
| `/br` | Tab | `/broadcast` |

Dùng `↑` `↓` để duyệt lịch sử lệnh đã gõ.

---

## 📂 Cấu trúc file

```
lan_chat/
├── main.py            # Entry point, xử lý lệnh
├── models.py          # SharedState, Peer, threading Locks
├── protocol.py        # UDP/TCP framing format
├── udp_handler.py     # UDP Discovery (broadcast + listener)
├── tcp_handler.py     # TCP chat engine (acceptor, receiver, file)
├── history.py         # Lưu/đọc lịch sử JSON, autosave
├── ui_handler.py      # prompt_toolkit UI, Tab Autocomplete
├── requirements.txt   # prompt_toolkit>=3.0.0
│
├── chat_history.json  # Tự tạo: lịch sử chat (lưu khi thoát)
└── downloads/         # Tự tạo: file nhận được
```

---

## 🔧 Xử lý sự cố thường gặp

### ❌ `ModuleNotFoundError: No module named 'prompt_toolkit'`

```bash
# Linux (Arch)
pip install prompt_toolkit --break-system-packages

# Tất cả OS — dùng venv
python3 -m venv .venv
source .venv/bin/activate        # Linux/macOS
.venv\Scripts\activate           # Windows
pip install prompt_toolkit
python3 main.py --name Quan --port 5001
```

### ❌ Không thấy peer sau khi `/list`

1. Đảm bảo cả 2 terminal đã chạy ít nhất 3–5 giây (UDP broadcast interval).
2. Kiểm tra tường lửa (firewall) không chặn UDP port 9999.
3. Trên **Windows**: test trên 2 máy thật thay vì 2 terminal cùng máy.
4. Chạy với `--debug` để xem log UDP:
   ```
   python3 main.py --name Quan --port 5001 --debug
   ```

### ❌ `[Errno 98] Address already in use`

Port TCP đang bị chiếm. Dùng port khác:
```bash
python3 main.py --name Quan --port 5010
```

### ❌ Gửi tin thất bại sau khi peer restart

Auto-reconnect sẽ tự phục hồi trong vòng **15 giây**. Hoặc gõ `/chat <tên>` lại thủ công.

### ❌ File không nhận được

- Kiểm tra thư mục `downloads/` trong thư mục chạy lệnh.
- Đảm bảo đã `/chat <tên>` trước khi `/file`.
- Trên Windows: cho phép Python qua Windows Defender Firewall.

---

## 🧠 Concepts học được

Dự án này minh họa các khái niệm lập trình mạng cốt lõi:

```
Socket Programming
├── UDP (SOCK_DGRAM)
│   ├── SO_BROADCAST — gửi đến 255.255.255.255
│   ├── SO_REUSEADDR / SO_REUSEPORT — nhiều process cùng bind 1 port
│   └── 127.255.255.255 — broadcast qua loopback (Linux)
│
└── TCP (SOCK_STREAM)
    ├── 3-way handshake (connect / accept)
    ├── Length-prefix framing — tránh TCP stream ambiguity
    ├── recv_exact() loop — recv(n) không đảm bảo trả đủ n bytes
    └── sendall() — đảm bảo gửi hết dữ liệu

Threading
├── threading.Lock (Mutex) — bảo vệ shared state
├── threading.Event — stop signal giữa các thread
├── daemon=True thread — tự dừng khi main thread thoát
└── stop_event.wait(timeout) — sleepable loop có thể dừng nhanh

Python
├── @dataclass — cấu trúc dữ liệu gọn gàng
├── Singleton pattern — SharedState dùng chung toàn app
├── Closure — _make_file_progress trả về callback có state
└── prompt_toolkit — Completer, patch_stdout, PromptSession
```

---

## 📜 License

MIT License — tự do học tập, chỉnh sửa và chia sẻ.

