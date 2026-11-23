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

Regression tests bao gồm socket ownership, framing, timeout, storage, command parsing và redraw qua pipe input và POSIX PTY. PTY test kiểm tra ANSI output, draft, cursor và thông báo unread.

Integration harness chạy ba subprocess trên IPv4 loopback với cổng động và data directory tạm riêng. Subprocess nhận lệnh từng dòng để kiểm tra CLI và network; regression PTY kiểm tra phần render. Harness dùng gate để buộc hai peer cùng dial trước khi gửi HELLO và giữ upload sau chunk đầu tiên trong khi kiểm tra chat. Fault injection chỉ hoạt động trong test subprocess. Script trả exit code khác 0 khi có lỗi.

### Kiểm tra thủ công trên hai máy cùng LAN

**UNVERIFIED.** Đang chờ đồng đội có máy thứ hai để chạy các bước dưới đây trên mạng LAN thực tế.

Chạy từ thư mục repository tương ứng trên mỗi máy, với Python và dependency đã sẵn có. Dùng Wi-Fi hoặc Ethernet thuộc cùng IPv4 subnet. Nếu hai máy cùng SSID nhưng không thấy nhau, kiểm tra guest network, AP isolation và firewall. Cho phép nhận UDP **9999** và TCP port của tiến trình trên interface LAN, trong phạm vi hai máy thử nghiệm. `--port` cấu hình TCP listener. UDP discovery gửi broadcast mỗi khoảng 3 giây và peer hết hạn sau khoảng 10 giây không nhận HELLO.

**Máy A**

```bash
lan_a_data=$(mktemp -d -t lan-chat-A.XXXXXX)
dd if=/dev/urandom of="$lan_a_data/qa-file.bin" bs=1M count=5 status=none
printf 'File thử nghiệm: %s\n' "$lan_a_data/qa-file.bin"
sha256sum "$lan_a_data/qa-file.bin"
python3 -B main.py --name QA-A --port 5001 --data-dir "$lan_a_data" --debug
```

**Máy B**

```bash
lan_b_data=$(mktemp -d -t lan-chat-B.XXXXXX)
printf 'Data directory: %s\n' "$lan_b_data"
python3 -B main.py --name QA-B --port 5002 --data-dir "$lan_b_data" --debug
```

1. Trên cả hai máy, dùng `/list` và kiểm tra tên, IP LAN và TCP port của máy kia. Đổi `/status away` rồi `/status online` trên B. Kiểm tra A thấy mỗi lần đổi sau một chu kỳ broadcast.
2. Gần như đồng thời, dùng `/chat QA-B` trên A và `/chat QA-A` trên B. Gửi hai tin khác nhau, ví dụ `A → B | kiểm thử UTF-8` và `B → A | kiểm thử UTF-8`. Xác nhận mỗi tin xuất hiện một lần ở đúng đầu nhận.
3. Trên A, dùng `/send <đường dẫn tuyệt đối tới qa-file.bin>`. Xác nhận thông báo của A ghi B đã lưu file và so SHA-256 trên B bằng `sha256sum "$lan_b_data/downloads/qa-file.bin"` sau khi quay lại shell. Khi cần kiểm tra chat đồng thời bằng tay, dùng file tổng hợp đủ lớn để kịp gửi tin hai chiều trước ACK.
4. Trên B, dùng `/quit` khi chat đang mở. A phải báo ngắt TCP, rồi hiển thị B offline trong `/list` sau khoảng 10–12 giây kể từ HELLO cuối. Sau `/quit` ở A, kiểm tra `chat_history.json` ở hai data directory đã in.
5. Khi discovery thất bại, ghi nhận IP và interface LAN của hai máy, ví dụ bằng `ip -br -4 addr` trên Linux. Kiểm tra listener bằng `ss -ltn` và kiểm tra capture ở bước dưới để xác định UDP broadcast có tới máy nhận và TCP handshake có hoàn tất hay không.

### Checklist Kitty

Chạy hai lệnh trên từ Kitty để kiểm tra redraw trực tiếp. Có thể thêm peer thử `QA-C` trên Linux với port `5003` và một data directory tạm riêng để kiểm tra tin từ hội thoại khác.

1. Dùng `/list`, `/status away`, `/status online` và `/chat <tên hoặc số>`. Kiểm tra prompt hiển thị đúng trạng thái và tên peer sau mỗi thao tác.
2. Trên A, đang chat với B, gõ `abcd` và nhấn ← hai lần. Giữ nguyên draft. B gửi `kitty-active`. Xác nhận tin xuất hiện phía trên prompt, draft vẫn là `abcd` và cursor vẫn ở giữa `b` và `c`.
3. Khi draft vẫn đang mở, C dùng `/chat QA-A` rồi gửi `kitty-unread`. Xác nhận A thấy thông báo từ C và vẫn giữ draft. Chèn `X` rồi Enter. B phải nhận chính xác `abXcd`. Dùng `/list` để xem `1 chưa đọc` ở C, sau đó `/chat QA-C` và `/list` để xác nhận lịch sử hiện tin và badge biến mất.
4. Nếu chỉ có hai peer, hoàn tất bước gửi `abXcd` trước. Trên A dùng `/leave` và bắt đầu gõ `/chat QA-B` mà chưa Enter. B gửi `kitty-unread` trên TCP connection còn mở. Kiểm tra draft tại A, nhấn Enter để đọc tin và dùng `/list` để xác nhận badge unread biến mất. Trường hợp ba peer vẫn được kiểm tra riêng trong integration harness.
5. Dùng `/leave`, chọn lại B, rồi `/send <file tổng hợp>`. Trong lúc hiển thị progress, gõ draft khác và kiểm tra redraw. Dùng `/quit` để xác nhận prompt thoát sạch.

**Kết quả Phase 3 · PASS trên Kitty thật**, với hai cửa sổ trên cùng máy Arch Linux/Hyprland. Người kiểm thử xác nhận discovery, đổi trạng thái, chat UTF-8 hai chiều, giữ draft và cursor khi nhận tin hoặc tiến độ file, unread sau `/leave`, và thoát sạch. Lượt gửi file 16 MiB đã đạt progress 100%, có `FILE_ACK` và SHA-256 khớp.

### Wireshark

**UNVERIFIED.** Chưa thực hiện capture Wireshark thật. Khi có lượt kiểm thử LAN, ghi lại số frame TCP handshake và số stream cho chat và file.

1. Trên một trong hai máy, chọn interface IPv4 LAN đang dùng và bắt đầu capture trước khi mở app. Capture filter cho cổng trong ví dụ là `udp port 9999 or tcp port 5001 or tcp port 5002`. Dùng dữ liệu tổng hợp và lưu `.pcapng` ngoài repository. [Capture filters](https://wiki.wireshark.org/CaptureFilters)
2. Áp display filter `udp.dstport == 9999`. Mở UDP payload và ghi lại địa chỉ nguồn, đích broadcast, cặp `HELLO`/`HELLO2`, advertised TCP port, presence và UUID. Để đo chu kỳ, so thời gian giữa hai gói cùng loại từ cùng nguồn trên một interface. `available` là giá trị wire cho trạng thái `online`.
3. Dùng `(tcp.port == 5001 || tcp.port == 5002) && tcp.flags.syn == 1` để tìm kết nối. Chọn một packet, dùng **Analyze → Follow → TCP Stream**, rồi dùng filter theo stream mà Wireshark tạo để xem cả TCP SYN, SYN-ACK và ACK cuối, kể cả packet không có cờ SYN. Chú ý client dùng source port tạm. [Following protocol streams](https://www.wireshark.org/docs/wsug_html_chunked/ChAdvFollowStreamSection.html)
4. Trong stream chat, tìm `HELLO|<name>|<port>|CHAT|` và `MSG|<length>|<UTF-8 bytes>`. Kiểm tra length theo byte UTF-8 của nội dung. Một frame có thể trải qua nhiều packet. Trong stream file riêng, tìm `HELLO|<name>|<port>|FILE|`, `FILE|<filename>|<size>|`, payload và `FILE_ACK|1|OK|` theo chiều ngược lại. `FILE_ACK` là dữ liệu ứng dụng trong TCP payload.
5. Ghi số frame cho handshake, số stream cho chat và file, và SHA-256 hai đầu. Nếu xem trên loopback, chọn interface loopback và dùng port thực tế của lần chạy. Capture trên loopback không đại diện cho broadcast trên Ethernet hoặc Wi-Fi.

### Ma trận xác minh Phase 3

| Yêu cầu | Trạng thái | Bằng chứng hoặc giới hạn |
|---|---|---|
| A · P2P console, UDP discovery, TCP chat và file | PASS | Ba process trên IPv4 loopback và hai instance Kitty trên cùng máy. |
| B · Discovery, online/away và peer expiry | PASS | Đã xác nhận trong integration và thử thủ công trên cùng máy. UDP discovery dùng cổng 9999 khi chạy app trực tiếp. |
| C · Chat hai chiều, simultaneous dial, đa peer, unread và `/leave` | PASS | Integration buộc simultaneous dial và đối chiếu TCP stream. Thử thủ công xác nhận chat UTF-8 hai chiều, unread và giữ TCP sau `/leave`. |
| D · File, progress, chat đồng thời, hủy và dọn file | PASS | Thử thủ công với file 16 MiB xác nhận progress 100%, `FILE_ACK` và SHA-256. Integration giữ upload đang chạy để kiểm tra chat đồng thời và hủy bằng `/quit`. |
| E · ANSI output, draft và cursor | PASS | Regression chạy qua POSIX PTY. |
| E · Render và thao tác trực tiếp trên Kitty | PASS | Thử thủ công trên Arch Linux/Hyprland xác nhận draft, cursor và thông báo unread khi có output xen vào. |
| F · Wire protocol, UTF-8 và FILE_ACK | PASS | Regression và integration kiểm tra framing hợp lệ, header bị từ chối, ACK thành công và lỗi quota. |
| G · Reconnect, disconnect, quota và shutdown | PASS | Người kiểm thử xác nhận reconnect, disconnect, expiry và history sau `/quit`. Integration kiểm tra thêm từ chối trước payload khi vượt quota. |
| H · README, hướng dẫn thử nghiệm và hướng dẫn capture | PASS | Các bước và tiêu chí ghi kết quả nằm trong mục này. |
| H · Capture Wireshark thật | UNVERIFIED | Chưa có capture cho UDP discovery, TCP handshake và các stream chat/file. |
| H · Báo cáo 5–10 trang | UNVERIFIED | Tài liệu nộp riêng ngoài phạm vi xác minh Phase 3 này. |
| I · Multi-instance trên cùng máy | PASS | Ba process loopback và hai cửa sổ Kitty đã chạy thành công. |
| I · Demo hai máy vật lý trên LAN | UNVERIFIED | Chờ đồng đội có máy thứ hai. |

Các quyết định còn mở gồm quota khi nhiều process dùng chung `--data-dir`, giới hạn tổng byte của hàng đợi chat và chính sách giữ history trong RAM. Hành vi firewall, broadcast qua Wi-Fi và hard link trên filesystem khác cần được đánh giá trong môi trường triển khai.

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

