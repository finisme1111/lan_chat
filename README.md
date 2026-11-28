# LAN Chat & File Sharing P2P

Ứng dụng console để chat và gửi file giữa các peer trên LAN IPv4. Mỗi peer chạy cùng một chương trình, tìm nhau qua UDP broadcast và dùng TCP cho chat và file transfer. Bài tập minh họa socket programming, framing trên TCP byte stream, đồng bộ giữa các worker và redraw terminal khi tin đến trong lúc người dùng đang gõ.

- **Discovery:** tự cập nhật danh sách peer, trạng thái online/away và thông báo online/offline.
- **Chat:** chọn peer theo tên hoặc số, nhắn tin UTF-8 và nhận tin từ nhiều peer. Thông báo tin từ ngoài hội thoại đang chọn kèm số unread.
- **File:** dùng kết nối TCP riêng, có tiến độ và phản hồi lưu file. Có thể tiếp tục chat trong lúc gửi.
- **Console:** giữ draft và cursor khi có output nền, hỗ trợ Tab completion, lịch sử lệnh và lịch sử chat lưu trên đĩa.

Phạm vi hỗ trợ là **LAN IPv4 tin cậy với một endpoint ổn định được quan sát cho mỗi peer**. Ứng dụng truyền plaintext và tin vào danh tính do peer công bố. Chỉ cho phép UDP discovery và TCP listener qua firewall trong phạm vi mạng thử nghiệm cần thiết. Xem [mô hình bảo mật và giới hạn](docs/SECURITY.md).

## Cài đặt

Cần Python **3.10 trở lên** và `prompt_toolkit>=3.0.0` theo [requirements.txt](requirements.txt). Bản kiểm thử gói sạch dùng Python 3.14.10. Mở terminal trong thư mục chứa `main.py`. Tạo venv riêng cho checkout hoặc gói source đang dùng.

### Arch Linux

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m pip check
```

### Windows native

Mở Windows Terminal hoặc PowerShell có console tương tác. Gọi cùng interpreter của venv để cài dependency, chạy ứng dụng và chạy test.

```powershell
python -m venv .venv
$Python = Join-Path (Get-Location).Path '.venv\Scripts\python.exe'
& $Python -m pip install -r .\requirements.txt
& $Python -m pip check
```

Trên Windows, chạy trực tiếp trong console, với stdin và stdout gắn với terminal. Nếu chuyển hướng một trong hai stream hoặc Windows console không khởi tạo được, ứng dụng báo lý do trên stderr rồi thoát với mã `2` trước khi mở data directory hoặc socket.

## Chạy hai peer

Trên Arch Linux, mở hai terminal ở thư mục dự án. Dùng TCP port khác nhau khi hai peer cùng chạy trên một máy. Các ví dụ tạo data directory tạm riêng ngoài repository.

```bash
# Terminal A
lan_a_data=$(mktemp -d -t lan-chat-A.XXXXXX)
printf 'Data A: %s\n' "$lan_a_data"
.venv/bin/python -B main.py --name QA-A --port 5001 --data-dir "$lan_a_data"

# Terminal B
lan_b_data=$(mktemp -d -t lan-chat-B.XXXXXX)
printf 'Data B: %s\n' "$lan_b_data"
.venv/bin/python -B main.py --name QA-B --port 5002 --data-dir "$lan_b_data"
```

Trên Windows, dùng lệnh dưới đây cho máy B nếu máy A đang chạy lệnh Arch ở trên. Nếu cả hai máy chạy Windows, trên máy A dùng `--name QA-A --port 5001` và tạo thư mục tạm riêng. Mỗi máy dùng source và venv của chính nó.

```powershell
$Python = Join-Path (Get-Location).Path '.venv\Scripts\python.exe'
$LanData = Join-Path ([System.IO.Path]::GetTempPath()) ("lan-chat-B-" + [guid]::NewGuid().ToString("N"))
Write-Host "Data B: $LanData"
& $Python -B .\main.py --name QA-B --port 5002 --data-dir "$LanData"
```

1. Dùng `/list` ở cả hai bên và chờ thấy peer còn lại. Trên A, nhập `/chat QA-B`; trên B, nhập `/chat QA-A`. Gõ một dòng văn bản và nhấn Enter.
2. Dùng `/status away` rồi `/status online`. Peer còn lại thấy trạng thái qua chu kỳ discovery.
3. Dùng `/send <đường dẫn file tổng hợp>`. Bên nhận kiểm tra `downloads/` trong data directory của mình. Sau đó dùng `/leave` để rời hội thoại hoặc `/quit` để thoát.

Ứng dụng production gửi **UDP broadcast trên cổng 9999** và quảng bá TCP port đã chọn. Cấu hình broadcast/unicast loopback của integration harness chỉ dùng cho test. Trên Windows, bind cùng UDP port giữa nhiều instance và khả năng nhận loopback broadcast tùy hệ điều hành. Kiểm thử unicast harness và lần chạy trên hai máy thật là hai hạng mục riêng. Xem [quy trình kiểm thử](docs/TESTING.md).

## Tham số và lệnh

```text
main.py --name <tên> --port <cổng> [--data-dir <thư mục>] [--download-quota-gib <GiB>] [--debug]
main.py -h
```

| Tham số | Hành vi |
|---|---|
| `--name` | Bắt buộc. Tên được chuẩn hóa trước khi kiểm tra cả hai gói discovery theo giới hạn 2.048 byte UTF-8, gồm phần overhead của gói. |
| `--port` | Bắt buộc. TCP listener từ `1` đến `65535`. |
| `--data-dir` | Thư mục chứa lock, history và downloads. Mặc định là working directory. Dùng thư mục riêng cho từng instance. |
| `--download-quota-gib` | Số nguyên dương, mặc định **8 GiB**. Áp dụng cho dung lượng file nhận trong `downloads/`. |
| `--debug` | Bật log chi tiết. |
| `-h`, `--help` | Hiển thị trợ giúp tham số và thoát. |

| Lệnh trong console | Hành vi |
|---|---|
| `/list` | Hiện peer online, số thứ tự, endpoint, trạng thái và badge connection/unread khi có. |
| `/chat <tên hoặc số>` | Chọn peer theo tên hoặc số từ lần `/list` gần nhất. Nếu chọn thất bại, lệnh gửi liên quan bị chặn cho đến khi chọn lại thành công hoặc `/leave`. |
| Văn bản thường | Gửi một tin tới peer đang chọn. Dòng bắt đầu bằng `/` được xử lý như lệnh. |
| `/send <file>`, `/file <file>` | Gửi file thường tới peer đang chọn; `/file` là alias. Dùng dấu nháy nếu muốn ghi rõ đường dẫn có khoảng trắng. |
| `/history` | Hiện lịch sử gần đây và phần chưa đọc của hội thoại đang chọn. |
| `/leave` | Bỏ selection và hủy reconnect cho hội thoại đó. TCP connection đang mở được giữ để dùng lại. |
| `/status <online hoặc away>` | Đổi trạng thái. Dispatcher cũng chấp nhận `available` và `busy`. |
| `/broadcast <nội dung>` | Gửi nội dung qua TCP tới các peer trong snapshot danh sách online. |
| `/help` | Hiển thị trợ giúp lệnh trong console. |
| `/quit` | Dừng chương trình, dọn worker và yêu cầu lưu history cuối. |

`Tab` hỗ trợ hoàn thành tên lệnh, tên peer khi `/chat` và đường dẫn khi gửi file. Mã thoát `0` áp dụng cho `/quit` và `Ctrl+C` tại prompt. TCP listener không khởi động được trả `1`; tham số sai, lỗi mở hoặc khóa data directory và console Windows không được hỗ trợ trả `2`.

## File, history và unread

Mỗi instance gửi một file tại một thời điểm, tối đa **2 GiB/file**. Tiến độ được cập nhật trong lúc chat vẫn hoạt động. Receiver ghi file tạm rồi lưu trong `<data-dir>/downloads/` bằng hard link cùng filesystem. Khi trùng tên, receiver chọn tên có hậu tố để giữ file sẵn có. Sender phân biệt receiver báo đã lưu, báo thất bại và chưa xác nhận. Chi tiết nằm trong [giao thức FILE và FILE_ACK](docs/PROTOCOL.md).

Tin đến từ peer khác được lưu kèm unread counter của peer đó. Khi chọn hội thoại, lịch sử hiện phần gần đây và các tin chưa đọc, sau đó cập nhật counter. `chat_history.json` lưu các dòng chat trong data directory. Unread là trạng thái trong RAM. App tự lưu định kỳ và yêu cầu lưu lần cuối khi thoát; xem [history và shutdown](docs/ARCHITECTURE.md).

## Xử lý sự cố

| Triệu chứng | Kiểm tra |
|---|---|
| `ModuleNotFoundError: prompt_toolkit` | Cài `requirements.txt`, chạy `pip check` và chạy app bằng cùng interpreter của venv. |
| `/list` trống | Chờ vài nhịp discovery. Kiểm tra cùng IPv4 subnet, guest network/AP isolation và firewall cho UDP `9999` cùng TCP port của các peer trên interface thử nghiệm. |
| TCP port đã được dùng hoặc data directory bị khóa | Chọn TCP port rảnh hoặc đóng instance sử dụng thư mục đó và chạy lại. File `.lan-chat.lock` tồn tại sau khi thoát là bình thường. Không xóa hoặc thay thế file lock khi app đang chạy. |
| Peer vừa restart hoặc selection lỗi | Dùng `/list` mới và `/chat` lại. Auto-reconnect chỉ áp dụng khi selection vẫn còn và peer vẫn có presence hợp lệ. |
| File không được lưu | Kiểm tra file nguồn, tiến độ, ACK, quota, dung lượng trống và filesystem đích có hỗ trợ hard link. Khi sender báo chưa xác nhận, kiểm tra file ở receiver. |
| Console Windows không được hỗ trợ | Mở Windows Terminal hoặc PowerShell tương tác và chạy trực tiếp, không dùng pipe hay redirect stdin/stdout. |

## Cấu trúc và tài liệu

| Thành phần | Trách nhiệm |
|---|---|
| `main.py`, `ui_handler.py` | CLI, presentation và command workers. |
| `models.py`, `protocol.py` | Shared state và wire format. |
| `udp_handler.py`, `tcp_handler.py` | Discovery, chat, file transfer và reconnect. |
| `history.py`, `data_dir_lock.py` | History JSON và quyền sở hữu data directory giữa các process. |
| `tests/`, `run_auto_test.py` | Regression, POSIX PTY và loopback integration. |

- [Kiến trúc và luồng thực thi](docs/ARCHITECTURE.md)
- [Giao thức UDP/TCP và framing](docs/PROTOCOL.md)
- [Kiểm thử và các cổng xác minh](docs/TESTING.md)
- [Bảo mật, tài nguyên và quyền riêng tư](docs/SECURITY.md)
- [Báo cáo kỹ thuật](docs/REPORT.md)
- [Runbook demo 10 phút](docs/DEMO.md)

## Trạng thái xác minh

**Phase 6H.2A** là baseline đã xác minh của source handoff. Gói bàn giao giữ nguyên production source, test suite và dependency của baseline, đồng thời cập nhật tài liệu theo kết luận Final QA **ACCEPT WITH DOCUMENTED LIMITATIONS**. Development và functional testing đã đóng. Feature freeze và production code freeze vẫn có hiệu lực. Completion commit và công bố trên GitHub chưa thực hiện.

- **Arch Linux:** regression **112/112 PASS**. Native POSIX terminal và lifecycle **PASS**. Candidate 6H.1 có production source giống 6H.2A đạt broadcast loopback **15/15 PASS**, unicast loopback **15/15 PASS** và mixed-version với Phase 5.1 **10/10 PASS**.
- **Windows native, máy A và máy B:** mỗi máy xác minh cùng candidate 6H.2A, đạt **109 PASS**, **3 SKIP theo nền tảng**, **0 FAIL**, **0 ERROR** và integration **15/15 PASS**.
- **Phase 3E và Wireshark:** chủ dự án đã chấp nhận LAN vật lý giữa hai máy Windows và Wireshark dựa trên bằng chứng đã nộp và xác nhận của tester. [Phạm vi bằng chứng lịch sử](docs/TESTING.md)

Phạm vi bàn giao là đồ án LAN IPv4 tin cậy, ưu tiên tin nhắn và file văn bản hoặc hình ảnh có tên thông thường. **6D05-F01: RISK ACCEPTED — NOT FIXED. 6D-05: CLOSED WITH DOCUMENTED LIMITATIONS — NOT FULL PASS. 6A-03: DEFERRED.** [Giới hạn và rủi ro còn lại](docs/SECURITY.md)

## Dữ liệu local và lịch sử Git

QA index đang stage việc bỏ theo dõi 7 file bytecode, `chat_history.json` và 2 file trong `downloads/`, cùng việc thêm `.gitignore` cho các đường dẫn runtime mặc định. Gói source sạch loại các artifact runtime này. HEAD hiện tại và các commit cũ vẫn có thể chứa dữ liệu từng được track. Chủ dự án cần quyết định phạm vi chia sẻ lịch sử trước khi publish hoặc merge. **Sao lưu history và downloads trước khi cập nhật clone đang có dữ liệu**, vì checkout mới có thể loại các bản từng được track.

## License

MIT License — tự do học tập, chỉnh sửa và chia sẻ.
