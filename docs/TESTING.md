# Kiểm thử và phạm vi bằng chứng

[README](../README.md) · [Kiến trúc](ARCHITECTURE.md) · [Giao thức](PROTOCOL.md) · [Bảo mật](SECURITY.md)

## Kết quả QA hiện hành

Final QA cho baseline **Phase 6H.2A** kết luận **ACCEPT WITH DOCUMENTED LIMITATIONS**. Lần xác minh candidate gốc đối chiếu **27/27 file** theo đường dẫn, kích thước, SHA-256 và POSIX mode. Gói source bàn giao cập nhật tài liệu và dùng manifest mới. Production source, test suite, dependency và `.gitignore` giữ nguyên. Development và functional testing đã đóng. Nội dung dưới đây là bằng chứng lịch sử.

| Môi trường và snapshot | Kết quả đã chấp nhận | Phạm vi |
|---|---|---|
| Arch Linux, baseline 6H.2A | Regression **112/112 PASS**. Native POSIX terminal và lifecycle **PASS** trong hồ sơ QA Arch. | POSIX PTY, startup/shutdown, khóa data directory và history. |
| Arch Linux, 6H.1 | Broadcast loopback **15/15 PASS**, unicast loopback **15/15 PASS**, mixed-version với Phase 5.1 **10/10 PASS**. | 6H.1 có production source giống 6H.2A. Hai manifest chỉ khác kích thước và SHA-256 của `tests/test_data_dir_lock.py`. |
| Windows native, máy A, 6H.2A | **109 PASS**, **3 SKIP**, **0 FAIL**, **0 ERROR**. Integration **15/15 PASS**. | Regression và UDP unicast loopback trên cùng candidate như máy B. |
| Windows native, máy B, 6H.2A | **109 PASS**, **3 SKIP**, **0 FAIL**, **0 ERROR**. Integration **15/15 PASS**. | Regression và UDP unicast loopback trên cùng candidate như máy A. |
| Phase 3E và Wireshark | **ACCEPTED** bởi chủ dự án. | Hai máy Windows trên LAN vật lý. Kết luận dựa trên bằng chứng đã nộp và xác nhận tester. Một số ca mở rộng không có minh chứng riêng để kiểm tra độc lập. |

## Bằng chứng từ các snapshot trước

| Snapshot và môi trường | Kết quả | Phạm vi |
|---|---|---|
| **6C.3**, gói source sạch trên Arch Linux | **112/112 regression PASS**, 0 fail, error hoặc skip; 3,009 giây, exit `0` | Python **3.14.10**, pip **26.2.2**, `prompt_toolkit` **3.0.53**, `wcwidth` **0.9.5**. Fresh install từ requirements và `pip check` đều PASS. |
| **6C.3**, Arch IPv4 broadcast loopback | **15/15 PASS**, exit `0` | Ba peer riêng, UDP broadcast trên loopback và TCP loopback. |
| **6C.3**, Arch IPv4 unicast loopback | **15/15 PASS**, exit `0` | Ba peer riêng, UDP unicast do harness điều phối và TCP loopback. |
| **6C.3**, POSIX PTY tương tác | **PASS** | Chạy CLI trực tiếp với terminal thật, tên Unicode, help/list/status, chat tổng hợp, lock trùng, thoát, lưu và đọc lại history. Regression PTY kiểm tra thêm draft, cursor và redraw. |
| **6C.3**, gói source | **21/21 file PASS** | Dựng đúng baseline cộng patch, cài và chạy từ gói bằng venv mới. Hash/mode khớp trước/sau, ZIP giải nén và xác minh lại thành công. Đây là gói trung gian trước tài liệu 6E. |
| **6B.1**, mixed-version trên Arch loopback | **10/10 PASS** với Phase 5.1 | HELLO và HELLO2 được kiểm tra riêng; chọn peer, chat/file Unicode hai chiều, FILE_ACK, SHA‑256, reconnect, EOF, expiry và đọc chéo JSON history. |
| **Phase 3**, Kitty trên Arch/Hyprland | **PASS** trên hai terminal cùng máy | Xác nhận draft/cursor, unread, chat, file 16 MiB có tiến độ, ACK và SHA‑256. Bằng chứng thủ công lịch sử. |
| **Phase 5.1**, Windows native | **87 phát hiện, 85 PASS, 2 skip POSIX**; **15/15 IPv4 unicast loopback PASS** | Windows 11 với Python **3.14.3**, `prompt_toolkit` **3.0.53** và kiểm tra console tương tác. Bằng chứng lịch sử của Phase 5.1. |

## Lệnh test tham khảo

Các lệnh sau ghi lại cách chạy bộ test theo từng nền tảng. Handoff hiện tại chỉ đối chiếu tài liệu và toàn vẹn source. Xem cách chuẩn bị venv trong [README](../README.md).

**Arch Linux**

```bash
.venv/bin/python -B -m unittest discover -s tests -v
.venv/bin/python -B run_auto_test.py
.venv/bin/python -B -c 'from run_auto_test import run_tests; raise SystemExit(run_tests(use_udp_unicast=True))'
```

Lệnh thứ hai dùng broadcast loopback mặc định trên Arch. Lệnh thứ ba chọn unicast **chỉ trong harness**. Các cấu hình này không thêm tham số production vào `main.py`.

**Windows native**

```powershell
$Python = Join-Path (Get-Location).Path '.venv\Scripts\python.exe'
& $Python -B -m unittest discover -s tests -v
& $Python -B .\run_auto_test.py
```

Harness mặc định unicast trên Windows. Ghi exit code sau từng lệnh và kết quả chi tiết. Các skip hợp lệ gồm `POSIX FIFO required`, `POSIX PTY required` và `POSIX byte filenames required`. Ca tên file theo byte cũng có thể skip nếu filesystem decoder không tạo surrogate cho fixture. Báo riêng số skip và lý do thực tế, không tính skip vào số PASS.

### Phạm vi và cách ly

- **Regression:** parser và giới hạn độ dài, partial reads/writes, deadline, peer expiry, socket ownership, simultaneous dial, failed selection, unread, quota, cleanup, khóa thư mục và save history.
- **Integration:** ba subprocess với dữ liệu tổng hợp, port động và data directory tạm. Harness kiểm tra presence, chat đa peer, status, file, ACK, SHA‑256, graceful shutdown, reconnect và expiry. Broadcast test bind/gửi trên IPv4 loopback. Unicast test gửi datagram thực tới từng listener riêng ở `127.0.0.1` và giữ nguyên HELLO/HELLO2 payload. Fault injection, gate cho concurrent upload/dial và đầu vào theo dòng chỉ thuộc harness.
- **PTY:** kiểm tra prompt và output bằng terminal POSIX, gồm giữ draft/cursor khi nhận thông báo unread. Khi chạy CLI trực tiếp, dùng terminal tương tác riêng và data directory mới. Trên Windows harness dùng backend dành cho test; CLI production yêu cầu stdin/stdout gắn với console.
- **Gói sạch:** 6C.3 dùng venv mới, tắt user-site và loại `PYTHONPATH`/`PYTHONHOME`, xác minh source được import từ gói và dependencies từ venv. Startup chạy trong network namespace chỉ có loopback. Log, runtime và venv đều ở ngoài gói. Kết quả thuộc snapshot 6C.3.

## Phạm vi xác minh Windows

Hai máy xác minh cùng 6H.2A bằng đường dẫn, kích thước và SHA-256. Mỗi máy có ba SKIP hợp lệ cho POSIX FIFO, POSIX byte filenames và POSIX PTY. **6H2-01** đã được xử lý trong driver của ca mô phỏng TCP bind failure ở `tests/test_data_dir_lock.py`. Regression trên cả hai máy xác nhận ca này PASS. Kết quả của driver mô phỏng và các quan sát từ console tương tác được ghi riêng.

**6D05-F01: RISK ACCEPTED — NOT FIXED. 6D-05: CLOSED WITH DOCUMENTED LIMITATIONS — NOT FULL PASS.** Hồ sơ probe native cục bộ trên máy B và giới hạn tên file được ghi riêng với regression và integration. Quyết định đóng áp dụng cho bản phát hành đồ án này. [Giới hạn tên file Windows](SECURITY.md#6d-05-giới-hạn-tên-file-windows)

## LAN hai máy vật lý

**Phase 3E: ACCEPTED.** Báo cáo tester mô tả hai máy Windows chạy trên LAN thật, gồm discovery hai chiều, chat UTF-8 và truyền file. Chủ dự án chấp nhận dựa trên hồ sơ đã nộp và xác nhận tester. Kết quả chỉ ghi nhận các ca trong phạm vi bằng chứng tương ứng. Buổi demo học thuật 10 phút là deliverable riêng đang chờ.

## Wireshark

**ACCEPTED** theo bằng chứng đã nộp và xác nhận tester. Đề yêu cầu ảnh UDP broadcast và TCP three-way handshake kèm giải thích ngắn. Dùng ảnh đã được duyệt cho báo cáo học thuật, giữ raw capture ngoài repository và gói source.

- Capture filter ví dụ cho TCP port 5001/5002 là `udp port 9999 or tcp port 5001 or tcp port 5002`.
- Display filter `udp.dstport == 9999` để thấy đích broadcast và payload HELLO/HELLO2. Đo khoảng cách giữa các gói cùng loại từ cùng nguồn. Ghi status wire và TCP port quảng bá.
- Dùng `(tcp.port == 5001 || tcp.port == 5002) && tcp.flags.syn == 1` để tìm lúc bắt đầu. Sau đó dùng **Follow → TCP Stream** hoặc filter theo số stream để thấy đủ SYN, SYN‑ACK và ACK cuối. Application HELLO chỉ xuất hiện sau TCP handshake.
- Khi chú thích ảnh, ghi các trường và số frame thấy được trong bằng chứng đã duyệt. Giữ nhãn môi trường LAN vật lý hoặc loopback đúng theo nguồn.
