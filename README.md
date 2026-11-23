# 🖥️ LAN Chat & File Sharing P2P

Ứng dụng **chat và chia sẻ file ngang hàng (P2P)** trong mạng LAN, chạy hoàn toàn trên **giao diện console**. Không cần server trung tâm — các máy tự tìm thấy nhau qua UDP broadcast.

```
╔══════════════════════════════════════════════════╗
║          LAN Chat & File Sharing P2P             ║
╚══════════════════════════════════════════════════╝

[Quan|online]> /chat Minh
[OK]  Đang chat với Minh.

[Quan|online → Minh]> Chào Minh!
[Quan (bạn)] Chào Minh!

*** [Minh] Chào Quan! ***
```

---

## ✨ Tính năng

| Tính năng | Mô tả |
|---|---|
| 🔍 **UDP Discovery** | Tự động tìm thấy các máy khác trong LAN sau ~3 giây, không cần nhập IP thủ công |
| 💬 **Chat 1-1** | Nhắn tin real-time với bất kỳ peer nào đang online |
| 📢 **Broadcast** | Gửi thông báo đến toàn bộ peer online |
| 📁 **Gửi file** | Chuyển file qua TCP connection riêng trên background worker. Có thể tiếp tục chat trong lúc gửi. |
| 📊 **File progress** | Thanh tiến độ `[████░░░░] 50%` khi gửi file lớn |
| 💾 **Persistent history** | Lịch sử chat được lưu tự động vào `chat_history.json`, đọc lại sau khi restart |
| 🔄 **Auto-reconnect** | Thử kết nối lại với peer đã chọn khi peer vẫn còn trong UDP peer table. |
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
      HELLO2|<name>|<tcp_port>|<status>|<instance_id>
TCP:  HELLO|<name>|<tcp_port>|<CHAT hoặc FILE>|
      MSG|<length>|<content bytes>
      FILE|<filename>|<size>|<data bytes>
      FILE_ACK|1|<OK hoặc ERROR>|
```

---

## 📋 Yêu cầu

- **Python** 3.10 trở lên
- **prompt_toolkit** ≥ 3.0.0
- **Kết nối mạng LAN** (hoặc loopback để test trên 1 máy)

---

## 🚀 Cài đặt & Chạy

### 🐧 Linux

```bash
# Clone hoặc copy thư mục dự án
cd lan_chat

python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt

# Chạy
python3 main.py --name Quan --port 5001
```

> Nếu discovery lỗi, kiểm tra UDP 9999, TCP port đã chọn và cấu hình firewall của mạng LAN.

---

### 🍎 macOS

```bash
cd lan_chat

python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt

# Chạy
python3 main.py --name Quan --port 5001
```

> Nếu nhiều instance trên một máy không nhận được UDP broadcast, thử trên hai máy trong cùng LAN.

---

### 🪟 Windows

```powershell
# Mở PowerShell hoặc Command Prompt
cd lan_chat

python -m venv .venv
.\.venv\Scripts\python -m pip install -r requirements.txt

# Chạy
.\.venv\Scripts\python main.py --name Quan --port 5001
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
python3 main.py --name Quan --port 5001 --data-dir ./data-quan

# Terminal 2
python3 main.py --name Minh --port 5002 --data-dir ./data-minh

# Terminal 3 (tùy chọn)
python3 main.py --name An --port 5003 --data-dir ./data-an
```

---

## 📖 Hướng dẫn sử dụng

### Tham số khởi động

```
python3 main.py --name <tên> --port <port> [--data-dir <thư mục>] [--download-quota-gib <GiB>] [--debug]
```

| Tham số | Mô tả | Ví dụ |
|---|---|---|
| `--name` | Tên hiển thị của bạn | `--name Quan` |
| `--port` | TCP port lắng nghe (1–65535) | `--port 5001` |
| `--data-dir` | Thư mục history và downloads, mặc định là working directory | `--data-dir ./data-quan` |
| `--download-quota-gib` | Quota file nhận tính bằng GiB, mặc định 8 | `--download-quota-gib 16` |
| `--debug` | Bật log chi tiết (socket, thread) | `--debug` |

---

### Danh sách lệnh

```
┌─ Discovery ───────────────────────────────────────────┐
│  /list              Hiển thị peer đang online          │
│  /status <s>        Đổi trạng thái (online|away)      │
├─ Chat 1-1 ────────────────────────────────────────────┤
│  /chat <tên|số>     Kết nối và bắt đầu chat            │
│  Nhập nội dung      Gửi tin đến peer hiện tại         │
│  /history           Xem lại lịch sử chat               │
│  /leave             Rời chat, giữ kết nối TCP          │
├─ Gửi File ────────────────────────────────────────────┤
│  /send <đường dẫn>  Gửi file (/file là alias)          │
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
| `/status ` | Tab | `online` / `away` |
| `/send ./` | Tab | Danh sách file trong thư mục hiện tại |
| `/br` | Tab | `/broadcast` |

Dùng `↑` `↓` để duyệt lịch sử lệnh đã gõ.

`/chat <số>` dùng số trong lần `/list` gần nhất. Trạng thái `online` được gửi trên UDP với giá trị `available` để peer cũ đọc được. Peer mới gửi cả `HELLO` và `HELLO2` trong mỗi chu kỳ discovery. `HELLO2` chứa UUID để gộp các địa chỉ cùng peer.

Mỗi instance gửi một file tại một thời điểm, tối đa 2 GiB mỗi file. Receiver lưu vào `downloads/` trong data directory và dùng hard link để publish cùng filesystem. Nếu peer đóng connection khi chưa gửi ACK được hỗ trợ, sender báo chưa xác nhận lưu file.

Quota tính file thường hiện có trực tiếp trong `downloads/` và kích thước đã đặt trước cho các lượt nhận cùng instance. Khi vượt quota hoặc thiếu dung lượng trống, ứng dụng từ chối lượt nhận mới. Dùng `--data-dir` riêng cho từng instance để mỗi instance quản lý reservation riêng.

TCP chat có deadline ghi 10 giây cho mỗi frame. `/broadcast` chạy trên queue riêng, tối đa 8 lệnh đang chờ. Khi thoát, ứng dụng chờ tối đa 2 giây cho lần lưu history cuối, rồi cảnh báo nếu chưa nhận được kết quả.

## Kiểm thử

```bash
python3 -B -m unittest discover -s tests -v
python3 -B run_auto_test.py
```

Unit tests bao gồm socket ownership, giới hạn framing, timeout, hủy transfer, storage, command parsing và prompt redraw. Smoke test chạy hai subprocess trên IPv4 loopback với cổng động và thư mục tạm riêng. Các kịch bản có broadcast, kiểm tra SHA-256, từ chối khi vượt quota và lưu history sau `/quit`. Script trả exit code khác 0 khi có lỗi.

Để demo trên hai máy, chạy hai instance cùng LAN, thử discovery, trạng thái, tin nhắn và gửi file khi đang chat. Trong Wireshark, lọc `udp.port == 9999` rồi chọn TCP stream để xác nhận SYN, SYN-ACK, ACK và application HELLO.

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
python3 -m venv .venv
source .venv/bin/activate        # Linux/macOS
python3 -m pip install -r requirements.txt
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

Worker thử lại theo chu kỳ 15 giây khi peer đã chọn vẫn còn online. Nếu peer đã hết UDP timeout, đợi peer xuất hiện lại trong `/list` rồi gõ `/chat <tên|số>`. `/leave` hủy retry và giữ TCP connection đã mở.

### ❌ File không nhận được

- Kiểm tra `downloads/` trong data directory và thông báo xác nhận phía gửi.
- Dùng `/chat <tên|số>` trước khi `/send <tệp>`.
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

