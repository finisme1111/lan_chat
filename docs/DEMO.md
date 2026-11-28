# Runbook demo LAN Chat & File Sharing P2P

**Thời lượng 10 phút trên ít nhất hai máy.** Cấu hình demo học thuật đề xuất là máy A chạy Arch Linux, Hyprland và Kitty, máy B chạy Windows native trong console tương tác. Người trình bày A điều phối và theo dõi giờ. Người trình bày B thao tác ở đầu kia, đọc kết quả discovery, phản hồi chat, xác minh file độc lập và thực hiện ngắt kết nối.

Runbook bám sát đề gốc *LAN Chat & File Sharing* (§3–5 về chức năng và kỹ thuật, §7 về deliverables, §8 về rubric). Hai máy dùng UDP broadcast production trên IPv4 LAN và cùng một bản source đã xác minh, với một endpoint ổn định quan sát được cho mỗi peer.

**Trạng thái chuẩn bị.** Demo học thuật vẫn đang chờ. Phase 3E trên hai máy Windows native và Wireshark đã được chủ dự án chấp nhận theo bằng chứng đã nộp và xác nhận tester. Windows native đã xác minh trên cùng candidate 6H.2A ở cả hai máy. Timeline bên dưới mô tả kết quả kỳ vọng của buổi trình bày Arch–Windows. **6D05-F01: RISK ACCEPTED — NOT FIXED. 6D-05: CLOSED WITH DOCUMENTED LIMITATIONS — NOT FULL PASS. 6A-03: DEFERRED.** [Kiểm thử](TESTING.md) · [Bảo mật](SECURITY.md)

## 1. Chuẩn bị trước khi bấm giờ

- [ ] Có hai máy vật lý cùng IPv4 LAN. Xác định địa chỉ IPv4 và interface thật trên hai máy. Kiểm tra AP/client isolation, guest VLAN và khả năng hai peer nhìn thấy nhau. Cùng SSID chưa đủ để xác nhận reachability.
- [ ] Giải nén cùng gói source bàn giao dựa trên 6H.2A trên cả hai máy. Dùng manifest **27 file** và SHA-256 tương ứng của gói bàn giao qua kênh đã thống nhất.
- [ ] Chuẩn bị Python 3.10 trở lên và venv riêng theo `requirements.txt`. Ghi phiên bản Python, pip, `prompt_toolkit` và `wcwidth`; `pip check` phải thành công. A dùng Kitty. B dùng Windows Terminal hoặc PowerShell có console tương tác, với stdin/stdout gắn vào console.
- [ ] Tạo data directory mới riêng cho mỗi máy ngoài thư mục source. Chuẩn bị file tổng hợp và cửa sổ shell phụ trên B để tính SHA-256 trong khi app vẫn mở. Kiểm tra TCP 5001 ở A, 5002 ở B và quyền dùng UDP 9999.
- [ ] Nếu cần thay đổi firewall, nhờ người quản trị cho phép UDP inbound 9999 và TCP inbound đúng cổng listener trên từng máy, giới hạn ở interface, profile và subnet LAN được phép. Kiểm tra policy outbound nếu mạng có hạn chế.
- [ ] Chuẩn bị báo cáo PDF, gói source bàn giao, ảnh Wireshark đã được duyệt, bộ đếm giờ và tài liệu dự phòng. Tắt thông báo desktop chứa thông tin riêng tư trước khi chia sẻ màn hình.

### Máy A — Bash trong Kitty

Thay các đường dẫn trong dấu nháy bằng đường dẫn thật. Đặt manifest bên ngoài thư mục source. Lấy SHA-256 của manifest từ người chuẩn bị gói, rồi dừng nếu giá trị hiển thị khác giá trị đã chốt.

```bash
cd '/duong/dan/toi/lan_chat'
qa_manifest='/duong/dan/toi/lan_chat-final-source.manifest.json'
sha256sum -- "$qa_manifest"

python3 -m venv .venv
qa_python="$PWD/.venv/bin/python"
"$qa_python" -m pip install -r requirements.txt
"$qa_python" -m pip check
"$qa_python" --version
"$qa_python" -m pip --version
"$qa_python" -m pip show prompt_toolkit wcwidth

ip -br -4 addr
read -r -p 'IPv4 thật của máy B: ' qa_peer_ip
ip -4 route get "$qa_peer_ip"
ss -ltn 'sport = :5001'

qa_data_a=$(mktemp -d -t lan-chat-demo-A.XXXXXXXX) || exit 1
qa_file_a="$qa_data_a/demo-A.bin"
"$qa_python" -B -c 'import os, sys; from pathlib import Path; Path(sys.argv[1]).write_bytes(os.urandom(16 * 1024**2))' "$qa_file_a"
printf 'Data A: %s\nFile A: %s\n' "$qa_data_a" "$qa_file_a"
sha256sum -- "$qa_file_a"
```

Kết quả `ip -4 route get` cho biết interface `dev` và địa chỉ `src` mà A sẽ dùng đến B. Nếu `ss` có dòng listener ở cổng 5001, xác định tiến trình trước khi chọn cổng khác. Cập nhật đồng bộ cổng CLI, firewall và filter Wireshark nếu đổi.

### Máy B — PowerShell trong Windows Terminal

Đối chiếu SHA-256 của manifest như trên A. Giữ lại giá trị `Data B` để dán vào shell phụ ở bước nhận file. `Get-NetTCPConnection` không có bản ghi nghĩa là không thấy TCP listener trên cổng 5002 tại thời điểm kiểm tra.

```powershell
Set-Location -LiteralPath 'C:\duong-dan\toi\lan_chat'
$QaManifest = 'C:\duong-dan\toi\lan_chat-final-source.manifest.json'
Get-FileHash -LiteralPath $QaManifest -Algorithm SHA256

python -m venv .venv
$QaPython = Join-Path (Get-Location).Path '.venv\Scripts\python.exe'
& $QaPython -m pip install -r .\requirements.txt
& $QaPython -m pip check
& $QaPython --version
& $QaPython -m pip --version
& $QaPython -m pip show prompt_toolkit wcwidth

Get-NetIPConfiguration
Get-NetConnectionProfile
Get-NetTCPConnection -State Listen -LocalPort 5002 -ErrorAction SilentlyContinue

$QaDataB = Join-Path ([System.IO.Path]::GetTempPath()) ('lan-chat-demo-B-' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $QaDataB -ErrorAction Stop | Out-Null
$QaFileB = Join-Path $QaDataB 'demo-B.bin'
& $QaPython -B -c 'import os, sys; from pathlib import Path; Path(sys.argv[1]).write_bytes(os.urandom(16 * 1024**2))' $QaFileB
"Data B: $QaDataB"
"File B: $QaFileB"
Get-FileHash -LiteralPath $QaFileB -Algorithm SHA256
```

### Kiểm tra source trên cả hai máy

Chỉ dùng manifest đã đối chiếu SHA-256 ở trên. Chạy trong thư mục gốc source. Kỳ vọng thấy `Verified files: 27`. Lệnh kiểm tra kích thước và SHA-256 từng file. POSIX mode được đối chiếu riêng ở bước đóng gói trên Linux. Nếu hash sai trên Windows, dùng bản giải nén đã xác minh và kiểm tra line ending trước khi chạy.

```bash
"$qa_python" -B -c 'import hashlib, json, sys; from pathlib import Path; rows = json.loads(Path(sys.argv[1]).read_bytes()); files = [(r, Path(r["path"]).read_bytes()) for r in rows]; bad = [r["path"] for r, data in files if len(data) != r["size"] or hashlib.sha256(data).hexdigest() != r["sha256"]]; print("Verified files:", len(files) - len(bad)); sys.exit("Mismatch: " + ", ".join(bad) if bad else 0)' "$qa_manifest"
```

```powershell
$QaVerify = @'
import hashlib, json, sys
from pathlib import Path
rows = json.loads(Path(sys.argv[1]).read_bytes())
bad = []
for row in rows:
    data = Path(row['path']).read_bytes()
    if len(data) != row['size'] or hashlib.sha256(data).hexdigest() != row['sha256']:
        bad.append(row['path'])
print('Verified files:', len(rows) - len(bad))
sys.exit('Mismatch: ' + ', '.join(bad) if bad else 0)
'@
& $QaPython -B -c $QaVerify $QaManifest
```

Dừng trước khi chạy app nếu version, `pip check`, source manifest hoặc cổng listener cần điều tra. Mở sẵn ảnh Wireshark đã duyệt cho phần giải thích.

### Khởi động và thoát

```bash
# A, trong cùng shell đã chuẩn bị
"$qa_python" -B main.py --name QA-A --port 5001 --data-dir "$qa_data_a"
printf 'Exit = %s\n' "$?"
```

```powershell
# B, trong cùng PowerShell tương tác đã chuẩn bị
& $QaPython -B .\main.py --name QA-B --port 5002 --data-dir $QaDataB
"Exit = $LASTEXITCODE"
```

Gõ `/quit` tại prompt ứng dụng. Dòng `Exit` chạy sau khi app trở về shell. Kỳ vọng exit `0` khi thoát bình thường. Có thể dùng Ctrl+C tại prompt nếu cần dừng phiên tương tác. Giữ lại thư mục tạm tổng hợp nếu cần kiểm tra history hoặc receiver file sau demo. [Cách chạy và lệnh CLI](../README.md)

## 2. Kịch bản trực tiếp — 10:00

A ra hiệu cho B chuyển bước. Chuẩn bị môi trường, file tổng hợp và ảnh Wireshark đã duyệt trước khi bắt đầu đồng hồ. Chọn peer theo tên để giữ lệnh ổn định.

| Thời gian | Máy và thao tác | Kỳ vọng và tiêu chí đạt | Lời dẫn của A và tiêu chí đề | Khi bước lỗi |
| --- | --- | --- | --- | --- |
| 00:00–01:00 | A và B chạy lệnh startup ở mục 1. A gõ `/help` và chỉ các lệnh chính. | Hai máy lên prompt, báo TCP listener và UDP discovery sẵn sàng. Help có `/list`, `/chat`, `/send`, `/leave`, `/status`, `/help` và `/quit`. | “Mỗi máy là một peer; cùng chương trình vừa lắng nghe vừa mở kết nối. Dữ liệu demo nằm ngoài source.” §2, §4, §5.1. | Kiểm tra lỗi bind, console và venv. Dùng bảng xử lý lỗi; ghi rõ nếu startup chưa thành công. |
| 01:00–03:00 | A và B gõ `/list`. B gõ `/status away`; A chờ cập nhật rồi `/list`. B gõ `/status online`; A kiểm tra lại. | Hai máy thấy peer kia với tên, IPv4, TCP port và trạng thái; A thấy B chuyển away rồi online. Quan sát thông báo xuất hiện nếu còn trên màn hình. | “UDP broadcast thông báo presence mỗi 3 giây. Danh sách dùng địa chỉ IPv4 quan sát được và TCP port trong payload.” §3.1–3.2. | Chờ vài nhịp; đối chiếu interface, firewall và AP isolation. Nếu peer không xuất hiện, đánh dấu bước chưa đạt và trình bày bằng chứng có sẵn theo đúng phạm vi. |
| 03:00–06:00 | A gõ `/chat QA-B` rồi `Chào máy B — mạng IPv4!`. B gõ `/chat QA-A` rồi `Máy B đã nhận được.`. A gõ `abcd`, nhấn mũi tên trái hai lần và chờ. B gửi `Tin đến khi A đang gõ.`. A chèn `X` rồi Enter. A gõ `/leave`. B gửi `Tin chưa đọc thứ nhất.`; A gõ `/list`. A gõ `/chat QA-B` nhưng chưa Enter, B gửi `Tin chưa đọc thứ hai.`, sau đó A Enter và gõ `/history` rồi `/list`. | Chat Unicode hai chiều. Tin đến khi gõ xuất hiện phía trên prompt; B nhận `abXcd` để xác nhận cursor đúng. Sau `/leave`, A thấy 1 rồi 2 chưa đọc, giữ draft lệnh đang gõ. Chọn lại B mở lịch sử và xóa unread vừa hiển thị. | “TCP CHAT truyền hai chiều trên một stream. `/leave` bỏ selection và mục tiêu reconnect local, giữ socket để nhận tin và tái sử dụng. Khi chưa chọn hội thoại, tin đến được lưu và báo số unread.” §3.3, §4.1, §4.3, §5.2. | Kiểm tra tên qua `/list`; đợi kết quả chọn peer trước khi gửi. Nếu redraw hoặc unread khác kỳ vọng, ghi riêng hiện tượng và tiếp tục phần có thể thực hiện. |
| 06:00–08:00 | A gõ `/send "<đường dẫn File A đã in>"`. B chuẩn bị sẵn và gửi `B vẫn chat được.` khi file đang truyền. A quan sát progress và xác nhận lưu. B tính SHA-256 file nhận ở shell phụ theo mục 3. | B có file trong `downloads/` của Data B. A có progress và thông báo xác nhận lưu. Nội dung chat đến khi transfer đang diễn ra nếu kịp. Hai SHA-256 trùng. | “File dùng kết nối TCP riêng. Receiver publish file rồi phản hồi FILE_ACK. Hai đầu tự tính SHA-256 để kiểm tra nội dung.” §3.4, §5.1–5.2. | Nếu file xong trước khi B gửi tin, dùng bằng chứng QA lịch sử để giải thích chat đồng thời. Nếu ACK thiếu, kiểm tra receiver và hash trước khi gửi lại. |
| 08:00–09:00 | A trình bày ảnh UDP broadcast và TCP handshake đã được duyệt. | Chỉ địa chỉ IPv4 đích broadcast, UDP 9999, HELLO/HELLO2 và thứ tự SYN, SYN-ACK, ACK. Giải thích HELLO ứng dụng đi sau handshake. | “UDP gửi datagram. TCP lập stream qua ba bước, rồi ứng dụng mới trao đổi HELLO và dùng framing.” §5.2, §7.3. | Dùng bản ảnh offline hoặc sơ đồ để giải thích. Ghi đúng nguồn và thời điểm của ảnh. |
| 09:00–10:00 | B gõ `/quit` và đọc exit code. A quan sát disconnect, đợi khoảng 10 giây tính từ presence cuối rồi gõ `/list`; A gõ `/quit` và kiểm tra exit code. | A thấy thông báo từ kết nối đang dùng rồi thấy B rời danh sách khi presence hết hạn; hai app về shell với exit `0`. Thời điểm expiry có thể lệch theo nhịp dọn danh sách. | “Đóng TCP và hết hạn UDP presence là hai sự kiện riêng. Demo minh họa discovery, chat, truyền file và thu hồi tài nguyên.” §3.1, §4.1, §5.3. | Nếu chưa thấy expiry, báo chưa đạt trong cửa sổ quan sát, giữ log tổng hợp và kiểm tra sau. Thoát riêng hai app bằng `/quit`. |

B đọc kết quả ngay khi có ở ba mốc chính: trạng thái peer lúc discovery, `abXcd` và câu trả lời lúc chat, SHA-256 receiver và exit code ở cuối. A giữ phần giải thích giao thức trong một phút, dành thời gian thao tác cho B.

## 3. Truyền file và kiểm tra độc lập

Lượt chính **A → B** dùng `demo-A.bin` tổng hợp, **16 MiB** (16.777.216 byte) tạo ở mục 1. Gõ đường dẫn thật thay vì tên biến shell trong app. Giữ dấu nháy kép nếu đường dẫn có khoảng trắng. File `demo-B.bin` dành cho phần minh họa chiều ngược nếu có thời gian.

- Sender hiển thị phần trăm dựa trên số byte đã gửi. Tốc độ LAN và việc cập nhật UI quyết định số lần nhìn thấy progress.
- TCP FILE riêng để CHAT tiếp tục. Receiver ghi file tạm rồi publish dưới `downloads/`. Nếu đích có tên trùng, dùng tên thực tế do receiver báo khi tính hash.
- Khi sender nhận `FILE_ACK|1|OK|`, nó báo peer đã xác nhận lưu. `UNCONFIRMED` nghĩa là sender chưa có xác nhận lưu, dù đã gửi dữ liệu. FILE_ACK không chứa checksum hay chữ ký. [FILE và FILE_ACK](PROTOCOL.md#file-và-file_ack)

Tính lại hash sender trên A nếu cần. Trên B mở PowerShell phụ và thay bằng đường dẫn Data B đã in ra và tên file thực tế. Đối chiếu 64 ký tự hex, bỏ qua khác biệt chữ hoa/thường.

```bash
sha256sum -- "$qa_file_a"
```

```powershell
$QaReceiverData = 'C:\duong-dan-tam\lan-chat-demo-B-...'
$QaReceived = Join-Path $QaReceiverData 'downloads\demo-A.bin'
Get-Item -LiteralPath $QaReceived | Select-Object Name, Length
Get-FileHash -LiteralPath $QaReceived -Algorithm SHA256
```

Ở lượt B → A, hash nguồn là `Get-FileHash -LiteralPath $QaFileB -Algorithm SHA256`. Hash đích điển hình là `sha256sum -- "$qa_data_a/downloads/demo-B.bin"` trong shell A có biến `qa_data_a`; thay tên nếu receiver báo tên khác. Nếu hash lệch, ghi nhận lượt truyền và chọn lại file thực tế trước khi thử mới.

## 4. Wireshark — trình bày bằng chứng đã duyệt

Chuẩn bị ảnh đã duyệt từ LAN vật lý Windows–Windows. Ghi nhãn môi trường khi trình bày. Các display filter dưới đây dùng để giải thích những frame đã chọn.

| Bằng chứng | Display filter và thao tác | Trường cần giải thích |
| --- | --- | --- |
| UDP broadcast | `udp.dstport == 9999`. Chọn datagram của peer QA và mở payload. | Source/destination IPv4, địa chỉ đích broadcast thực tế, cổng nguồn và đích UDP, HELLO hoặc HELLO2 và TCP port được quảng bá. Nếu đo chu kỳ, so các gói cùng loại từ cùng nguồn. UI `online` tương ứng wire `available`; `away` giữ nguyên. |
| TCP three-way handshake | `(tcp.port == 5001 \|\| tcp.port == 5002) && tcp.flags.syn == 1` để tìm SYN và SYN-ACK. Sau đó bỏ filter SYN và dùng **Follow → TCP Stream** từ một gói thuộc stream, hoặc nhập `tcp.stream == N` với số stream thật. | Xác định SYN, SYN-ACK và ACK cuối trên cùng stream. Chỉ hai endpoint, port, sequence/acknowledgement theo giá trị Wireshark hiển thị. Sau bước TCP ACK, tìm HELLO ứng dụng trong byte stream, ví dụ định dạng `HELLO\|QA-A\|5001\|CHAT\|`. |

Trong filter nhập vào Wireshark, dùng dấu `||` trực tiếp. Khi cần lọc nhanh cả hai port và tìm SYN, cũng có thể dùng:

```text
(tcp.port == 5001 || tcp.port == 5002) && tcp.flags.syn == 1
```

Chuẩn bị chú thích ngắn theo §7.3 của đề. Giữ raw capture ngoài repository và che dữ liệu nhận dạng không cần thiết trong ảnh. [Phạm vi bằng chứng](TESTING.md#wireshark) · [Mục phân tích trong báo cáo](REPORT.md#7-phân-tích-lưu-lượng-wireshark) · [Định dạng giao thức](PROTOCOL.md)

## 5. Bằng chứng bổ trợ khi giải thích

Nếu được hỏi về simultaneous dial, failed selection, lock, transfer gián đoạn, history hoặc reconnect, tham chiếu hồ sơ QA đã chấp nhận và ghi đúng môi trường quan sát. Functional testing đã đóng. Phần chuẩn bị demo chỉ cần phối hợp người trình bày, thời lượng và tài liệu. [Phạm vi kiểm thử](TESTING.md#phạm-vi-và-cách-ly) · [Kiến trúc](ARCHITECTURE.md) · [BYE và EOF](PROTOCOL.md#bye-eof-và-đóng-kết-nối)

## 6. Xử lý sự cố và phương án dự phòng

| Triệu chứng | Thao tác an toàn |
| --- | --- |
| Discovery trống | Chờ vài nhịp discovery rồi `/list` ở hai đầu. So địa chỉ IPv4, subnet và cổng UDP 9999; xem log startup. |
| Firewall hoặc isolation | Đối chiếu quyền UDP/TCP trên đúng profile và subnet. Hỏi người quản trị về client isolation hoặc guest VLAN, hoặc chuyển sang LAN thử nghiệm được phép. |
| Interface sai | So địa chỉ và route của hai máy với adapter LAN dự kiến. |
| TCP port đã dùng | Xem listener bằng lệnh mục 1; nếu cần, chọn cổng còn trống và cập nhật startup, firewall và capture filter. |
| Thiếu dependency | Dùng đúng Python của venv; kiểm tra `pip show prompt_toolkit`, cài bằng `requirements.txt` và chạy `pip check`. |
| Console Windows không hỗ trợ | Mở Windows Terminal/PowerShell tương tác và gọi trực tiếp Python của venv. Kiểm tra stdin/stdout gắn console. Exit `2` là tình huống từ chối console theo chính sách hỗ trợ. |
| Data-directory lock conflict | Giữ phiên đang sở hữu thư mục. Dùng thư mục tạm khác cho peer mới hoặc thoát phiên QA đang giữ lock bằng `/quit`. |
| Chọn chat lỗi | Xem `/list`, kiểm tra endpoint và cổng người nhận; chọn lại peer hợp lệ và đợi kết quả. Tin hoặc file đã bị từ chối cần người dùng chủ động gửi lại. |
| Truyền file lỗi | Kiểm tra đường dẫn nguồn, data directory receiver, dung lượng, quota và thông báo lỗi. Chỉ dùng file và thư mục tổng hợp của phiên QA khi thử lại. |
| Thiếu FILE_ACK | Ghi `UNCONFIRMED` nếu sender báo vậy. Kiểm tra receiver đã publish chưa, tính SHA-256 và xem log trước khi gửi lại; lần gửi mới có thể tạo thêm một file nếu tên trùng. |
| Ảnh handshake không mở được | Dùng bản ảnh offline đã chuẩn bị hoặc giải thích bằng sơ đồ và nêu nguồn bằng chứng đã được duyệt. |
| Peer bất ngờ ngắt | Để app dọn socket và quan sát thông báo. Xem `/list`; đợi reconnect nếu selection/presence còn hợp lệ, nếu không chọn lại peer khi peer online. |

Khi live demo gặp lỗi, ghi rõ bước chưa đạt, giải thích bằng capture LAN thật đã chuẩn bị nếu có, rồi thực hiện các bước còn khả thi. Bằng chứng test local và capture đã lưu được trình bày với nhãn môi trường và thời điểm tương ứng.

## 7. Đối chiếu rubric §8

| Nhóm chấm | Điểm tối đa | Bằng chứng live dự kiến | Bằng chứng bổ trợ |
| --- | ---: | --- | --- |
| UDP discovery, online list, status | 15 | Hai chiều discovery, `/list`, away/online và expiry. | Capture broadcast thật, regression và loopback có nhãn môi trường. |
| TCP chat, gửi/nhận bất kỳ lúc nào, nhiều peer | 20 | Hai peer chat Unicode hai chiều, unread trong phiên. | Kiểm thử tích hợp ba peer, simultaneous dial và mixed-version. Hai máy trình bày một cặp peer. |
| File transfer, progress, lưu đúng | 15 | File tổng hợp, progress, CHAT song song khi quan sát được, ACK và hai hash độc lập. | Hồ sơ QA lịch sử về file transfer và trao đổi CHAT đồng thời. |
| Console UI | 15 | `/help`, giữ draft và cursor, `abXcd`, hai thông báo unread rồi chọn lại. | Bằng chứng POSIX PTY và console native từ hồ sơ QA Windows. |
| Protocol design và framing | 10 | Giải thích capture UDP và TCP, liên hệ HELLO, MSG và FILE_ACK. | Tài liệu giao thức và sơ đồ trong báo cáo. |
| Error handling và stability | 10 | `/quit`, disconnect và peer expiry. | Bằng chứng lịch sử về chọn lỗi, lock, transfer gián đoạn, deadline, cleanup và reconnect. |
| Report | 10 | Nêu vị trí báo cáo nếu được hỏi. | PDF 5–10 trang theo đề và ảnh capture thật có giải thích; đang chờ hoàn tất. |
| Demo và code quality | 5 | Hai máy vật lý, phân vai, chạy đúng 10 phút. | Source và README, manifest gói bàn giao và hồ sơ QA lịch sử. |

Điểm tối đa là trọng số của đề. Kết quả live và chất lượng deliverables phụ thuộc lần trình bày và đánh giá thực tế. [Báo cáo kỹ thuật](REPORT.md) · [Phạm vi bằng chứng kiểm thử](TESTING.md)

## 8. Checklist chuẩn bị demo và nộp bài

- [ ] Hai máy vật lý sẵn sàng, operator xác nhận console, route, firewall, adapter và cổng.
- [ ] Đã đối chiếu cùng source manifest và xác nhận Python, dependencies, `pip check` trên từng máy.
- [ ] Chuẩn bị ảnh Wireshark đã duyệt và ghi nhãn hồ sơ Phase 3E Windows–Windows.
- [ ] Chốt lượt đổi màn hình, phần giải thích và thời lượng 10 phút của buổi trình bày.
- [ ] Hoàn thiện metadata, chèn ảnh và kiểm tra PDF cuối 5–10 trang ở giai đoạn Final Technical Report.
- [ ] Đặt gói source bàn giao, manifest và các tài liệu học thuật cuối ở vị trí nộp đã thống nhất.
- [ ] Có ảnh và tài liệu offline dự phòng. Ghi rõ phần nào đã thực hiện trong buổi demo.
