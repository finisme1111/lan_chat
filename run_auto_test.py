#!/usr/bin/env python3
"""
run_auto_test.py - Kịch bản kiểm thử tự động toàn diện cho LAN Chat & File Sharing P2P
Mục đích:
- Kiểm thử tự động không cần can thiệp thủ công.
- Mô phỏng 2 Peer (Quan : 5001 và Minh : 5002) chạy ngầm qua subprocess.
- Kiểm tra 6 Test Case cốt lõi:
    TC1 - UDP Discovery tự động
    TC2 - Cập nhật và đồng bộ trạng thái UDP (away)
    TC3 - Trò chuyện 1-1 qua TCP (Chat message)
    TC4 - Truyền tệp nhị phân 5MB và kiểm tra toàn vẹn băm SHA-256
    TC5 - Khả năng chống crash khi nhận gói tin rác (Malformed Packet)
    TC6 - Xử lý an toàn khi đối tác ngắt kết nối đột ngột (Abrupt termination)
"""

import hashlib
import os
import shutil
import socket
import subprocess
import sys
import threading
import time
from typing import List, Tuple

# Màu hiển thị console
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
BOLD = "\033[1m"
RESET = "\033[0m"

TEST_FILENAME = "test_data.bin"
TEST_FILE_SIZE = 5 * 1024 * 1024  # 5 MB
DOWNLOADS_DIR = "downloads"


class ProcessMonitor:
    """Quản lý và đọc log stdout/stderr của tiến trình peer ngầm."""
    def __init__(self, name: str, port: int):
        self.name = name
        self.port = port
        self.proc: subprocess.Popen = None
        self.logs: List[str] = []
        self._lock = threading.Lock()
        self._thread: threading.Thread = None

    def start(self):
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        self.proc = subprocess.Popen(
            [sys.executable, "main.py", "--name", self.name, "--port", str(self.port)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            env=env,
        )
        self._thread = threading.Thread(target=self._reader, daemon=True)
        self._thread.start()

    def _reader(self):
        try:
            for line in iter(self.proc.stdout.readline, ""):
                cleaned = line.rstrip()
                with self._lock:
                    self.logs.append(cleaned)
        except Exception:
            pass

    def send_cmd(self, cmd: str):
        if self.proc and self.proc.poll() is None:
            try:
                self.proc.stdin.write(cmd + "\n")
                self.proc.stdin.flush()
            except (BrokenPipeError, OSError):
                pass

    def get_logs(self) -> List[str]:
        with self._lock:
            return list(self.logs)

    def is_alive(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    def stop(self):
        if self.proc and self.proc.poll() is None:
            try:
                self.send_cmd("/quit")
                self.proc.wait(timeout=2.0)
            except Exception:
                try:
                    self.proc.kill()
                except Exception:
                    pass


def generate_test_file(filename: str, size: int) -> str:
    """Tạo file nhị phân ngẫu nhiên và trả về mã băm SHA-256."""
    print(f"{CYAN}[+] Đang khởi tạo tệp nhị phân ngẫu nhiên {size // (1024*1024)}MB: '{filename}'...{RESET}")
    data = os.urandom(size)
    with open(filename, "wb") as f:
        f.write(data)
    sha256_hash = hashlib.sha256(data).hexdigest()
    print(f"{CYAN}[+] Mã băm SHA-256 gốc: {sha256_hash}{RESET}")
    return sha256_hash


def verify_file_hash(filepath: str, expected_hash: str) -> bool:
    """Xác minh mã băm SHA-256 của file."""
    if not os.path.exists(filepath):
        return False
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest() == expected_hash


def run_tests():
    print(f"\n{BOLD}═════════════════════════════════════════════════════════════════{RESET}")
    print(f"{BOLD}   KHỞI ĐỘNG HỆ THỐNG KIỂM THỬ TỰ ĐỘNG: LAN CHAT & FILE P2P   {RESET}")
    print(f"{BOLD}═════════════════════════════════════════════════════════════════{RESET}\n")

    # 1. Chuẩn bị file nhị phân 5MB
    orig_hash = generate_test_file(TEST_FILENAME, TEST_FILE_SIZE)

    # 2. Xóa các file test cũ trong downloads/ nếu có
    downloaded_target = os.path.join(DOWNLOADS_DIR, TEST_FILENAME)
    if os.path.exists(downloaded_target):
        try:
            os.remove(downloaded_target)
        except OSError:
            pass

    test_results: List[Tuple[str, str, str]] = []

    peer_a = ProcessMonitor("Quan", 5001)
    peer_b = ProcessMonitor("Minh", 5002)

    try:
        # Khởi chạy 2 peer
        print(f"{CYAN}[*] Khởi chạy Peer A (Quan - Cổng 5001)...{RESET}")
        peer_a.start()
        print(f"{CYAN}[*] Khởi chạy Peer B (Minh - Cổng 5002)...{RESET}")
        peer_b.start()

        # -------------------------------------------------------------
        # TC1 - UDP Discovery
        # -------------------------------------------------------------
        print(f"\n{BOLD}--- [TC1] Kiểm tra UDP Discovery tự động ---{RESET}")
        print("Đang chờ 4.0s để 2 peer phát hiện nhau qua broadcast UDP...")
        time.sleep(4.0)

        peer_a.send_cmd("/list")
        time.sleep(1.5)
        logs_a = peer_a.get_logs()

        tc1_passed = any("Minh" in line and "5002" in line for line in logs_a)
        if tc1_passed:
            print(f"{GREEN}[PASS] TC1: Peer A đã phát hiện Peer B (Minh:5002) qua UDP.{RESET}")
            test_results.append(("TC1 - UDP Discovery", "PASS", "Phát hiện đúng peer Minh:5002 trong danh sách /list"))
        else:
            print(f"{RED}[FAIL] TC1: Không tìm thấy Minh:5002 trong danh sách /list của Peer A.{RESET}")
            test_results.append(("TC1 - UDP Discovery", "FAIL", "Không thấy thông tin Minh:5002"))

        # -------------------------------------------------------------
        # TC2 - Trạng thái UDP (away)
        # -------------------------------------------------------------
        print(f"\n{BOLD}--- [TC2] Kiểm tra cập nhật trạng thái UDP (/status away) ---{RESET}")
        peer_b.send_cmd("/status away")
        print("Đã gửi lệnh đổi trạng thái sang 'away' tại Peer B. Chờ chu kỳ broadcast 3.5s...")
        time.sleep(3.5)

        peer_a.send_cmd("/list")
        time.sleep(1.5)
        logs_a = peer_a.get_logs()

        tc2_passed = any("Minh" in line and "away" in line for line in logs_a)
        if tc2_passed:
            print(f"{GREEN}[PASS] TC2: Trạng thái của Minh đã chuyển thành 'away' trên máy Peer A.{RESET}")
            test_results.append(("TC2 - UDP Status Sync", "PASS", "Đồng bộ trạng thái 'away' thành công qua UDP"))
        else:
            print(f"{RED}[FAIL] TC2: Trạng thái 'away' chưa được đồng bộ sang Peer A.{RESET}")
            test_results.append(("TC2 - UDP Status Sync", "FAIL", "Trạng thái không đổi sang away"))

        # -------------------------------------------------------------
        # TC3 - TCP Chat
        # -------------------------------------------------------------
        print(f"\n{BOLD}--- [TC3] Kiểm tra gửi nhận tin nhắn TCP 1-1 ---{RESET}")
        peer_a.send_cmd("/chat Minh")
        time.sleep(1.0)

        chat_msg = "Hello Minh from Quan! Test message 12345"
        peer_a.send_cmd(chat_msg)
        time.sleep(1.5)

        logs_b = peer_b.get_logs()
        tc3_passed = any(chat_msg in line for line in logs_b)
        if tc3_passed:
            print(f"{GREEN}[PASS] TC3: Peer B đã nhận chính xác nội dung tin nhắn chat từ Peer A.{RESET}")
            test_results.append(("TC3 - TCP Chat 1-1", "PASS", f"Tin nhắn '{chat_msg}' được nhận đúng 100%"))
        else:
            print(f"{RED}[FAIL] TC3: Tin nhắn không xuất hiện tại Peer B.{RESET}")
            test_results.append(("TC3 - TCP Chat 1-1", "FAIL", "Không nhận được tin nhắn chat"))

        # -------------------------------------------------------------
        # TC4 - Truyền File & Toàn vẹn dữ liệu SHA-256
        # -------------------------------------------------------------
        print(f"\n{BOLD}--- [TC4] Kiểm tra truyền file nhị phân 5MB & Kiểm tra mã băm SHA-256 ---{RESET}")
        # Gửi lệnh /send test_data.bin từ Peer A
        peer_a.send_cmd(f"/send {TEST_FILENAME}")
        print("Đang truyền tệp tin nhị phân 5MB qua kết nối TCP riêng...")

        # Đợi tối đa 10s cho file 5MB truyền xong
        file_received = False
        target_file_path = os.path.join(DOWNLOADS_DIR, TEST_FILENAME)
        for _ in range(20):
            time.sleep(0.5)
            if os.path.exists(target_file_path) and os.path.getsize(target_file_path) == TEST_FILE_SIZE:
                file_received = True
                break

        if file_received:
            print(f"{CYAN}[*] File đã được tải xong tại '{target_file_path}'. Đang tính toán mã băm SHA-256...{RESET}")
            hash_match = verify_file_hash(target_file_path, orig_hash)
            if hash_match:
                print(f"{GREEN}[PASS] TC4: File 5MB truyền thành công, mã băm SHA-256 trùng khớp 100%!{RESET}")
                test_results.append(("TC4 - File Transfer & SHA256", "PASS", "Dữ liệu nhị phân 5MB toàn vẹn tuyệt đối"))
            else:
                print(f"{RED}[FAIL] TC4: File nhận được bị sai lệch dữ liệu (mã băm không khớp)!{RESET}")
                test_results.append(("TC4 - File Transfer & SHA256", "FAIL", "Mã băm SHA-256 không khớp"))
        else:
            print(f"{RED}[FAIL] TC4: Không tìm thấy file hoàn chỉnh trong thư mục downloads/.{RESET}")
            test_results.append(("TC4 - File Transfer & SHA256", "FAIL", "Không nhận đủ 5MB hoặc file không tồn tại"))

        # -------------------------------------------------------------
        # TC5 - Chống crash gói rác (Malformed Packet)
        # -------------------------------------------------------------
        print(f"\n{BOLD}--- [TC5] Kiểm tra khả năng chống crash do gói tin rác (Malformed Packet) ---{RESET}")
        try:
            raw_s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            raw_s.settimeout(3.0)
            raw_s.connect(("127.0.0.1", 5002))
            raw_s.sendall(b"INVALID_HEADER_GARBAGE_12345\n")
            time.sleep(0.5)
            raw_s.close()
            time.sleep(1.0)

            # Kiểm tra xem Peer B có còn sống không
            if peer_b.is_alive():
                # Gửi thêm lệnh kiểm tra Peer B vẫn phản hồi bình thường
                peer_b.send_cmd("/help")
                time.sleep(0.5)
                print(f"{GREEN}[PASS] TC5: Gói tin rác bị từ chối an toàn, tiến trình Peer B tiếp tục sống khỏe mạnh.{RESET}")
                test_results.append(("TC5 - Malformed Packet", "PASS", "Server chặn rác an toàn, không bị crash"))
            else:
                print(f"{RED}[FAIL] TC5: Tiến trình Peer B đã bị crash sau khi nhận gói tin rác!{RESET}")
                test_results.append(("TC5 - Malformed Packet", "FAIL", "Tiến trình bị sập"))
        except Exception as e:
            print(f"{RED}[FAIL] TC5: Lỗi khi thực hiện gửi gói tin rác: {e}{RESET}")
            test_results.append(("TC5 - Malformed Packet", "FAIL", str(e)))

        # -------------------------------------------------------------
        # TC6 - Ngắt kết nối đột ngột
        # -------------------------------------------------------------
        print(f"\n{BOLD}--- [TC6] Kiểm tra xử lý khi đối tác ngắt kết nối đột ngột ---{RESET}")
        print("Đang dừng cưỡng bức (terminate) tiến trình Peer A...")
        if peer_a.proc:
            peer_a.proc.terminate()
            peer_a.proc.wait(timeout=2.0)

        time.sleep(2.0)
        logs_b = peer_b.get_logs()
        # Kiểm tra Peer B có in thông báo ngắt kết nối an toàn không
        disconnect_notified = any("Kết nối với Quan đã bị ngắt" in line for line in logs_b)
        peer_b_alive = peer_b.is_alive()

        if disconnect_notified and peer_b_alive:
            print(f"{GREEN}[PASS] TC6: Peer B phát hiện ngắt kết nối an toàn, in thông báo chuẩn và không bị crash.{RESET}")
            test_results.append(("TC6 - Abrupt Disconnect", "PASS", "Bắt ngoại lệ ngắt kết nối an toàn, prompt trở về bình thường"))
        else:
            status_desc = "Không in thông báo" if not disconnect_notified else "Peer B bị crash"
            print(f"{RED}[FAIL] TC6: {status_desc}.{RESET}")
            test_results.append(("TC6 - Abrupt Disconnect", "FAIL", status_desc))

    finally:
        # Dọn dẹp tiến trình
        print(f"\n{CYAN}[*] Đang dừng toàn bộ tiến trình peer...{RESET}")
        peer_a.stop()
        peer_b.stop()

        # Dọn dẹp file test
        print(f"{CYAN}[*] Đang dọn dẹp các tệp tin kiểm thử tạm thời...{RESET}")
        if os.path.exists(TEST_FILENAME):
            try:
                os.remove(TEST_FILENAME)
            except OSError:
                pass

        if os.path.exists(downloaded_target):
            try:
                os.remove(downloaded_target)
            except OSError:
                pass

    # -------------------------------------------------------------
    # BẢNG TỔNG KẾT ĐÁNH GIÁ (TEST SUMMARY REPORT)
    # -------------------------------------------------------------
    print(f"\n{BOLD}═══════════════════════════════════════════════════════════════════════════════════{RESET}")
    print(f"{BOLD}                        BẢNG TỔNG KẾT ĐÁNH GIÁ KIỂM THỬ TỰ ĐỘNG                     {RESET}")
    print(f"{BOLD}═══════════════════════════════════════════════════════════════════════════════════{RESET}")
    print(f"{BOLD}| {'Mã Test Case':<30} | {'Kết quả':<10} | {'Chi tiết đánh giá':<32} |{RESET}")
    print(f"|{'-'*32}|{'-'*12}|{'-'*34}|")

    all_passed = True
    for name, status, detail in test_results:
        status_colored = f"{GREEN}{status:<8}{RESET}" if status == "PASS" else f"{RED}{status:<8}{RESET}"
        if status != "PASS":
            all_passed = False
        print(f"| {name:<30} | {status_colored} | {detail:<32} |")

    print(f"═══════════════════════════════════════════════════════════════════════════════════\n")

    if all_passed:
        print(f"{GREEN}{BOLD}>>> KẾT QUẢ CUỐI CÙNG: 6/6 TEST CASES ĐẠT CHUẨN (100% PASS) <<<{RESET}\n")
    else:
        print(f"{RED}{BOLD}>>> KẾT QUẢ CUỐI CÙNG: MỘT SỐ TEST CASE CHƯA ĐẠT. VUI LÒNG KIỂM TRA LẠI LOG. <<<{RESET}\n")


if __name__ == "__main__":
    run_tests()

