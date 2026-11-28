# Bảo mật, tài nguyên và quyền riêng tư

[README](../README.md) · [Kiến trúc](ARCHITECTURE.md) · [Giao thức](PROTOCOL.md) · [Kiểm thử](TESTING.md)

## Threat model

Đây là ứng dụng P2P giáo dục dùng trong **LAN IPv4 tin cậy**, với **một endpoint ổn định được quan sát cho mỗi peer** và data directory local do người dùng kiểm soát. Người có thể truy cập mạng LAN vẫn có thể quan sát lưu lượng hoặc gửi thông báo sai. UDP discovery, TCP chat và FILE là plaintext. Tên, endpoint và UUID do peer quảng bá hoặc được suy ra từ socket; không có cơ chế xác thực danh tính ngang hàng hay bảo mật nội dung.

UDP listener bind wildcard IPv4 ở cổng **9999**. TCP listener bind wildcard IPv4 ở `--port`. Listener có thể tiếp cận qua các interface được routing/firewall cho phép. Giới hạn firewall vào đúng interface và phạm vi IPv4 LAN cần thử. Không mở cổng ra Internet. Receiver có thể bắt đầu tải file khi nhận FILE hợp lệ, gồm dạng tương thích không có HELLO; việc chọn hội thoại ở local không phải quyền cấp cho peer gửi đến. Dùng payload tổng hợp khi kiểm thử và capture.

## Dữ liệu local và toàn vẹn

`chat_history.json` lưu các dòng chat dưới dạng JSON UTF‑8. File nhận nằm trong `downloads/` thuộc data directory. Unread chỉ giữ trong RAM. Trên Arch POSIX, probe 6D với dữ liệu tổng hợp xác nhận file history mới và file download mới có mode **0600**. File lock được tạo với mode `0o600` trên POSIX và mở theo `O_NOFOLLOW` khi hệ điều hành cung cấp. Điều này không thay đổi quyền của thư mục hoặc file có sẵn. Quyền ACL trên Windows nằm ngoài phạm vi đã xác minh. Dùng thư mục local riêng, tránh thư mục cùng ghi bởi người dùng khác, và nhớ rằng khóa data directory chỉ điều phối các instance hợp tác.

Tên file nhận được lấy basename, đổi `|` thành `_` và bỏ khoảng trắng đầu/cuối; receiver dùng `.part` riêng và thử tên có hậu tố khi trùng. Publication dùng hard link cùng filesystem. History dùng temp file cùng thư mục và `os.replace`. Các thao tác này cho phép tên đích xuất hiện nguyên tử nếu filesystem hỗ trợ, nhưng không có `fsync` cho file/thư mục để bảo đảm sau mất điện. Một process bị dừng cưỡng bức hoặc filesystem lỗi có thể để lại temp file hoặc làm mất dữ liệu mới chưa bền. Nếu dọn `.part` thất bại, ứng dụng ghi cảnh báo; lượt quét quota tiếp theo vẫn có thể tính file đó.

`FILE_ACK` là kết quả do receiver báo sau bước publish. Giao thức không có checksum cấp ứng dụng, xác thực hay chữ ký. SHA‑256 trong [kiểm thử](TESTING.md) và demo là phép đối chiếu độc lập ở hai đầu.

## Giới hạn tài nguyên

| Phạm vi | Giới hạn thực tế và nơi thực thi |
|---|---|
| UDP discovery | **2.048 byte/datagram** ở decoder; broadcast khoảng **3 giây**; presence hết hạn sau **10 giây** tại lượt quét. Không có cap số peer còn hạn. [protocol.py](../protocol.py) · [udp_handler.py](../udp_handler.py) · [models.py](../models.py) |
| TCP header và payload CHAT | **4.096 byte/trường header**; **10 MiB UTF‑8/payload MSG**. Receiver kiểm tra số byte trước khi ghi history. [protocol.py](../protocol.py) · [tcp_handler.py](../tcp_handler.py) |
| Chat queue | **64 lệnh chờ**, **32 MiB payload** kể cả job đang chạy. |
| Broadcast queue | **8 lệnh chờ**, **16 MiB payload** kể cả job đang chạy và prefix `[Broadcast] ` theo phép tính kích thước. |
| TCP workers | Tối đa **32 inbound workers** tính chung handshake, CHAT và FILE. Listen backlog cấu hình **15**. Outbound CHAT receiver không tính vào cap inbound và không có cap tổng số socket của process. [tcp_handler.py](../tcp_handler.py) |
| File transfer | **2 GiB/file**, chunk **64 KiB** và tối đa **một file sender/process**. Quota downloads mặc định **8 GiB** từ `--download-quota-gib`. Receiver tính file thường trực tiếp trong downloads và bytes đã reserve, kiểm tra free space trước khi tạo temp, rồi giải phóng reservation sau cleanup. Quota tính byte, không tính giới hạn số file và không áp dụng cho history. [models.py](../models.py) · [tcp_handler.py](../tcp_handler.py) |
| Ownership của data directory | Khóa độc quyền nonblocking trước load history và khởi động mạng. POSIX dùng `flock`, Windows dùng `msvcrt.locking`. Descriptor giữ đến lúc process kết thúc. [data_dir_lock.py](../data_dir_lock.py) |

`_CommandQueue` từ chối lệnh mới khi thiếu slot hoặc budget. Budget tính payload, còn RAM thực tế gồm draft, history, peer table, socket buffers và Python overhead. History lưu không giới hạn retention. `/history` chỉ sao chép cửa sổ đang hiển thị, nhưng có thể kéo dài tới toàn bộ phần unread cũ; autosave vẫn chụp toàn bộ history. TTL dọn presence khi peer hết hạn nhưng không đóng idle socket hoặc xóa history. [Queue](../main.py) · [History](../history.py) · [UI](../ui_handler.py)

| Loại timeout hoặc ngân sách | Điều được giới hạn |
|---|---|
| TCP handshake nhận | **10 giây** cho toàn bộ phần header mà worker đọc trước khi chuyển vào CHAT/FILE receiver. |
| CHAT connect và ghi frame | Connect **5 giây**. Ghi MSG **10 giây** tính từ lúc bắt đầu chờ write lock và dùng chung cho các partial writes. |
| CHAT header và payload | **10 giây** cho header từ byte đầu; payload tối đa **10 giây không tiến triển** và **60 giây tổng thời gian đọc**. Chờ byte đầu của frame kế tiếp có thể idle. |
| FILE socket I/O | Connect **10 giây**; các thao tác socket FILE dùng timeout **10 giây**. Transfer còn tiến triển có thể giữ tài nguyên lâu hơn vì không có deadline tổng cho payload FILE. |
| FILE_ACK | **10 giây** cho toàn bộ ACK khi bên gửi bắt đầu chờ ACK. |
| Shutdown | Ngân sách chờ worker **2 giây**, sau đó chờ tối đa **2 giây** cho final history save. Các ngưỡng chờ không tạo hard deadline cấp tiến trình nếu filesystem, output hoặc thao tác đồng bộ chậm. |

Các timeout network được thực thi tại [tcp_handler.py](../tcp_handler.py). Điều phối shutdown ở [main.py](../main.py) và [history.py](../history.py).

## 6A-03 endpoint alias

**6A-03: DEFERRED.** Điều kiện kích hoạt là cùng một peer được quan sát qua nhiều endpoint trong khi alias cho một endpoint còn thiếu, hết hạn hoặc đổi giữa lúc lookup và publish socket. Ví dụ, thứ tự HELLO/HELLO2 ở receiver có thể khiến socket được đăng ký theo key endpoint cũ trước khi alias có hiệu lực. Có nhiều interface trên máy chưa đủ để gây lỗi; routing và discovery phải thực sự tạo ra các quan sát khác endpoint.

Trong điều kiện trên có thể xuất hiện TCP connection trùng, selection lệch key, history/unread khó truy cập qua danh sách mới, reconnect mất target hoặc gửi file thất bại vì endpoint cũ. Phạm vi hiện tại vẫn là một endpoint IPv4 được quan sát ổn định. [Endpoint state](../models.py) · [TCP ownership](../tcp_handler.py)

## 6D-05 giới hạn tên file Windows

**6D-05: CLOSED WITH DOCUMENTED LIMITATIONS — NOT FULL PASS.** Trong probe cục bộ được ghi nhận trên Windows máy B, tester không quan sát ghi ngoài thư mục đích. Bằng chứng đó giới hạn ở cấu hình và các ca đã thử. Hành vi containment, ACL và publication trên các phiên bản Windows hoặc filesystem khác chưa được xác nhận tổng quát.

**6D05-F01: RISK ACCEPTED — NOT FIXED.** Một số tên đặc biệt do peer gửi có thể được lưu dưới dạng khó mở hoặc xóa bằng công cụ thông thường, hoặc tên sau khi lưu có thể khác tên kỳ vọng. Rủi ro được chấp nhận trong phạm vi đồ án với peer tin cậy và tên file thông thường.

Symlink/junction hoặc chuyển hướng filesystem do người dùng cấu hình local là điều kiện riêng với tên do peer gửi. Dùng data directory do chính người dùng kiểm soát. [Phạm vi bằng chứng Windows](TESTING.md#phạm-vi-xác-minh-windows)

## Findings 6D

| ID | Disposition | Nội dung |
|---|---|---|
| 6D-01 | **DOCUMENT AS KNOWN LIMITATION** | Plaintext, claimed identity và wildcard listener. Firewall và LAN tin cậy là điều kiện vận hành. |
| 6D-02 | **DOCUMENT AS KNOWN LIMITATION** | Cap 32 áp dụng cho inbound worker. Idle CHAT có thể giữ slot và FD. |
| 6D-03 | **DOCUMENT AS KNOWN LIMITATION** | Peer table, unread metadata và history chưa có retention/cap toàn cục. |
| 6D-04 | **DOCUMENT AS KNOWN LIMITATION** | FILE timeout theo thao tác; quota byte không giới hạn số file hay chi phí scan. |
| 6D-05 | **CLOSED WITH DOCUMENTED LIMITATIONS — NOT FULL PASS** | Phạm vi probe native trên máy B và giới hạn đã ghi ở trên. |
| 6D05-F01 | **RISK ACCEPTED — NOT FIXED** | Một số tên file đặc biệt trên Windows vẫn gây khó khăn khi thao tác hoặc đổi tên lúc lưu. |
| 6D-06 | **ACCEPT WITH JUSTIFICATION** | History do app tự tạo thuộc phạm vi hỗ trợ. History import bất thường có thể load thành công nhưng làm lần lưu sau thất bại do Unicode; probe giữ được file cũ. Lỗi parser ngoài các loại đã bắt có thể làm startup ngừng. |
| 6D-07 | **DEFER AS OPTIONAL IMPROVEMENT** | Một số log chèn trường peer trực tiếp. Cải thiện escape dữ liệu log vẫn hoãn. |
| 6D-08 | **DOCUMENT AS KNOWN LIMITATION** | Atomic publication không bảo đảm crash durability; final save trong ngân sách chờ hữu hạn. |
| 6D-09 | **DOCUMENT AS KNOWN LIMITATION** | Git history vẫn có thể chứa runtime đã từng track. Untracking chỉ áp dụng cho snapshot mới. |

## Quyền riêng tư trong Git

QA index đang stage việc bỏ theo dõi 10 file gồm 7 bytecode, history và hai download. `.gitignore` giữ các đường dẫn runtime mặc định, cache, venv và các raw capture thường gặp ngoài những lần thêm file sau này. Với `--data-dir` tùy ý, ưu tiên thư mục ngoài repo. Baseline 6H.2A và gói source bàn giao 27 file loại các artifact runtime này.

HEAD hiện tại, các commit cũ và clone đã nhận chúng vẫn có thể chứa runtime từng được track. Chủ dự án quyết định phạm vi chia sẻ lịch sử trước khi publish hoặc merge. Khi cập nhật tới snapshot cleanup đã commit trong tương lai, Git có thể xóa những bản tracked khỏi checkout khác. Dừng app và sao lưu history/downloads cần giữ trước khi cập nhật clone. Không đưa file log, history hoặc raw capture riêng tư vào báo cáo hay bài nộp.
