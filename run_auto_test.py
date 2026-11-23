#!/usr/bin/env python3
"""Integration test các peer thật trên loopback, với storage tạm riêng."""
import hashlib
import json
import os
import re
import socket
import subprocess
import sys
import threading
import time
from contextlib import ExitStack, nullcontext
from pathlib import Path
from tempfile import TemporaryDirectory

TEST_FILE_SIZE = 5 * 1024 * 1024
TEST_QUOTA_GIB = 1
SCRIPT = Path(__file__).resolve()


class ProcessMonitor:
    def __init__(self, name, port, udp_port, directory):
        self.name = name
        self.port = port
        self.udp_port = udp_port
        self.directory = directory
        self.proc = None
        self.logs = []
        self.changed = threading.Condition()
        self.reader = None

    def start(self):
        self.proc = subprocess.Popen(
            [
                sys.executable, "-B", str(SCRIPT), "--peer", str(self.udp_port),
                "--name", self.name, "--port", str(self.port), "--data-dir", str(self.directory),
                "--download-quota-gib", str(TEST_QUOTA_GIB),
            ],
            cwd=self.directory,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            env={**os.environ, "PYTHONIOENCODING": "utf-8", "PYTHONUNBUFFERED": "1"},
        )
        self.reader = threading.Thread(target=self._read_output, daemon=True)
        self.reader.start()

    def _read_output(self):
        try:
            for line in self.proc.stdout:
                with self.changed:
                    self.logs.append(line.rstrip())
                    self.changed.notify_all()
        finally:
            with self.changed:
                self.changed.notify_all()

    def checkpoint(self):
        with self.changed:
            return len(self.logs)

    def wait_for(self, predicate, *, start=0, timeout=6):
        with self.changed:
            return self.changed.wait_for(lambda: any(predicate(line) for line in self.logs[start:]), timeout)

    def expect(self, text, *, start=0, timeout=6):
        if not self.wait_for(lambda line: text in line, start=start, timeout=timeout):
            raise AssertionError(f"{self.name} không hiển thị {text!r}")

    def expect_line(self, predicate, *, start=0, timeout=6):
        if not self.wait_for(predicate, start=start, timeout=timeout):
            raise AssertionError(f"{self.name} không hiển thị dòng mong đợi")
        with self.changed:
            return next(line for line in self.logs[start:] if predicate(line))

    def send(self, command):
        self.proc.stdin.write(command + "\n")
        self.proc.stdin.flush()

    def stop(self):
        if self.proc is None:
            return
        if self.proc.poll() is None:
            try:
                self.send("/quit")
                self.proc.wait(timeout=4)
            except (OSError, subprocess.TimeoutExpired):
                self.proc.kill()
                self.proc.wait()
        if self.reader is not None:
            self.reader.join(timeout=2)
        self.proc.stdin.close()
        self.proc.stdout.close()


def _test_ports(count):
    with ExitStack() as stack:
        sockets = [
            stack.enter_context(socket.socket(socket.AF_INET, kind))
            for kind in [socket.SOCK_STREAM] * count + [socket.SOCK_DGRAM]
        ]
        for sock in sockets:
            sock.bind(("127.0.0.1", 0))
        return tuple(sock.getsockname()[1] for sock in sockets)


def _wait_until(predicate, timeout):
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= deadline:
            return False
        time.sleep(0.02)
    return True


def _install_controls(main, tcp):
    """Gates và fault injection chỉ có trong subprocess của smoke harness."""
    directory = Path.cwd()
    connect = tcp._connect_socket
    start_file_send = main.start_file_send
    handle_command = main.handle_command

    def controlled_connect(sock, endpoint, timeout):
        connect(sock, endpoint, timeout)
        if (directory / ".qa-dial-armed").exists():
            (directory / ".qa-dial-ready").touch()
            if not _wait_until((directory.parent / ".qa-dial-release").exists, 5):
                raise TimeoutError("QA dial gate timed out")

    def controlled_file_send(peer_key, filepath, progress, done):
        def gated_progress(sent, total):
            progress(sent, total)
            if sent < total and (directory / ".qa-file-armed").exists():
                (directory / ".qa-file-ready").touch()
                if not _wait_until(
                    lambda: (directory / ".qa-file-release").exists() or tcp.state.stop_event.is_set(), 8,
                ):
                    raise TimeoutError("QA file gate timed out")

        return start_file_send(peer_key, filepath, gated_progress, done)

    def controlled_command(command, *submit):
        query, _, port = command.partition(" ")
        if query not in ("@qa-links", "@qa-drop"):
            return handle_command(command, *submit)

        key = tcp.state.resolve_peer_key("127.0.0.1", int(port))
        with tcp.state.conn_lock:
            connection = tcp.state.connections.get(key)
            if query == "@qa-links":
                endpoints = [connection.sock.getsockname(), connection.sock.getpeername()] if connection else []
                print(f"QA_LINKS {json.dumps(endpoints)}", flush=True)
            elif connection is not None:
                tcp._shutdown_socket(connection.sock)
        return False

    tcp._connect_socket = controlled_connect
    main.start_file_send = controlled_file_send
    main.handle_command = controlled_command


def _run_loopback_peer(udp_port):
    """Giới hạn socket của test peer, giữ nguyên cấu hình của ứng dụng khi chạy bình thường."""
    import main
    import tcp_handler
    import udp_handler

    class LineInputSession:
        """Use line input for network assertions; ANSI redraw has its own PTY test."""

        @property
        def app(self):
            return self

        def invalidate(self):
            pass

        def prompt(self, _message):
            return input()

    class LoopbackSocket(socket.socket):
        def bind(self, address):
            if self.family == socket.AF_INET:
                host, port = address
                if not host and self.type == socket.SOCK_DGRAM and port != 0:
                    host = "127.255.255.255"
                address = (host or "127.0.0.1", port)
                if not address[0].startswith("127."):
                    raise ValueError("Test chỉ cho phép bind IPv4 loopback.")
            return super().bind(address)

        def connect(self, address):
            if self.family == socket.AF_INET and not address[0].startswith("127."):
                raise ValueError("Test chỉ cho phép kết nối IPv4 loopback.")
            return super().connect(address)

        def connect_ex(self, address):
            if self.family == socket.AF_INET and not address[0].startswith("127."):
                raise ValueError("Test chỉ cho phép kết nối IPv4 loopback.")
            return super().connect_ex(address)

        def sendto(self, data, address):
            if self.family == socket.AF_INET and not address[0].startswith("127."):
                raise ValueError("Test chỉ cho phép gửi UDP tới IPv4 loopback.")
            return super().sendto(data, address)

    socket.socket = LoopbackSocket
    udp_handler.UDP_PORT = udp_port
    udp_handler.UDP_BROADCAST_ADDR = udp_handler.UDP_LOOPBACK_ADDR
    main.UDP_PORT = udp_port
    main.get_local_ip = lambda: "127.0.0.1"
    main.create_prompt_session = LineInputSession
    main.patch_stdout = nullcontext
    _install_controls(main, tcp_handler)
    main.main()


def run_tests():
    with TemporaryDirectory(prefix="lan-chat-test-") as directory:
        root = Path(directory)
        quan_port, minh_port, an_port, udp_port = _test_ports(3)
        quan = ProcessMonitor("Quan", quan_port, udp_port, root / "quan")
        minh = ProcessMonitor("Minh", minh_port, udp_port, root / "minh")
        an = ProcessMonitor("An", an_port, udp_port, root / "an")
        peers = (quan, minh, an)
        for peer in peers:
            peer.directory.mkdir()

        source = quan.directory / "test_data.bin"
        source.write_bytes(os.urandom(TEST_FILE_SIZE))
        sha256 = hashlib.sha256(source.read_bytes()).hexdigest()

        def list_row(observer, subject):
            match = re.compile(rf"^\s*(\d+)\. {subject.name} \(127\.0\.0\.1:{subject.port}\)")
            start = observer.checkpoint()
            observer.send("/list")
            line = observer.expect_line(match.search, start=start)
            return match.search(line)[1], line

        def discovery():
            for observer in peers:
                for subject in peers:
                    if observer is not subject:
                        observer.expect(f"{subject.name} (127.0.0.1:{subject.port}) đã online", timeout=10)
                        list_row(observer, subject)

        def links(observer, subject):
            start = observer.checkpoint()
            observer.send(f"@qa-links {subject.port}")
            line = observer.expect_line(lambda line: line.lstrip().startswith("QA_LINKS "), start=start)
            return json.loads(line.lstrip().removeprefix("QA_LINKS "))

        def simultaneous_chat():
            points = {peer: peer.checkpoint() for peer in (quan, minh)}
            for peer in points:
                (peer.directory / ".qa-dial-armed").touch()
            try:
                quan.send("/chat Minh")
                minh.send("/chat Quan")
                if not _wait_until(lambda: all((p.directory / ".qa-dial-ready").exists() for p in points), 4):
                    raise AssertionError("Hai đầu chưa cùng mở TCP trước khi gửi HELLO")
            finally:
                (root / ".qa-dial-release").touch()

            quan.expect("Đã kết nối với Minh", start=points[quan])
            minh.expect("Đã kết nối với Quan", start=points[minh])
            for _ in range(20):
                quan_link, minh_link = links(quan, minh), links(minh, quan)
                if quan_link and quan_link == minh_link[::-1]:
                    break
                time.sleep(0.05)
            else:
                raise AssertionError(f"Hai đầu giữ TCP connection khác nhau: {quan_link=} {minh_link=}")

            for sender, receiver in ((quan, minh), (minh, quan)):
                start = receiver.checkpoint()
                sender.send("Tin ngay sau simultaneous dial")
                receiver.expect(f"[{sender.name}] Tin ngay sau simultaneous dial", start=start)

        def status():
            for presence in ("away", "online"):
                start = minh.checkpoint()
                minh.send(f"/status {presence}")
                minh.expect(f"Đã cập nhật trạng thái cá nhân: '{presence}'", start=start)
                for _ in range(10):
                    start = quan.checkpoint()
                    quan.send("/list")
                    if quan.wait_for(
                        lambda line: f"Minh (127.0.0.1:{minh_port})" in line and presence in line,
                        start=start, timeout=0.5,
                    ):
                        break
                else:
                    raise AssertionError(f"Quan chưa thấy trạng thái {presence} của Minh")

        def chat():
            number, _ = list_row(quan, minh)
            start = quan.checkpoint()
            quan.send(f"/chat {number}")
            quan.expect("Đã kết nối với Minh", start=start)
            start = minh.checkpoint()
            quan.send("Chào Minh từ Quan | UTF-8")
            minh.expect("Chào Minh từ Quan | UTF-8", start=start)
            start = quan.checkpoint()
            sender_start = minh.checkpoint()
            minh.send("Minh trả lời Quan | UTF-8")
            minh.expect("(bạn)] Minh trả lời Quan | UTF-8", start=sender_start)
            quan.expect("Minh trả lời Quan | UTF-8", start=start)

        def multi_peer_unread():
            start = an.checkpoint()
            an.send("/chat Quan")
            an.expect("Đã kết nối với Quan", start=start)
            start = quan.checkpoint()
            an.send("Tin chưa đọc từ An")
            quan.expect("Tin nhắn mới từ An: Tin chưa đọc từ An", start=start)

            number, row = list_row(quan, an)
            if "1 chưa đọc" not in row:
                raise AssertionError(f"Sai unread counter: {row}")
            start = quan.checkpoint()
            quan.send(f"/chat {number}")
            quan.expect("Lịch sử chat với An", start=start)
            quan.expect("Tin chưa đọc từ An", start=start)
            _, row = list_row(quan, an)
            if "chưa đọc" in row:
                raise AssertionError(f"Unread counter chưa được xóa: {row}")

            start = quan.checkpoint()
            quan.send("/leave")
            quan.expect("Đã rời cuộc trò chuyện với An", start=start)
            if not links(quan, an):
                raise AssertionError("/leave đã đóng TCP connection")
            quan.send("/chat Minh")
            quan.expect("Đã kết nối với Minh", start=start)

        def broadcast():
            start = minh.checkpoint()
            quan.send("/broadcast kiểm tra worker")
            minh.expect("[Broadcast] kiểm tra worker", start=start)

        def file_transfer():
            start_sender = quan.checkpoint()
            start_receiver = minh.checkpoint()
            (quan.directory / ".qa-file-armed").touch()
            quan.send(f"/send {source}")
            target = minh.directory / "downloads" / source.name
            try:
                if not _wait_until((quan.directory / ".qa-file-ready").exists, 3):
                    raise AssertionError("Sender chưa gửi chunk đầu tiên")
                minh.expect("Đang nhận file 'test_data.bin'", start=start_receiver)
                for peer, start, marker in ((quan, start_sender, "[Gửi file]"), (minh, start_receiver, "[Tiến độ file]")):
                    peer.expect_line(lambda line: marker in line and "0%" in line and source.name in line, start=start)
                if target.exists():
                    raise AssertionError("File đã publish trước khi mở gate")
                quan.send("Tin nhắn trong lúc gửi file")
                minh.expect("Tin nhắn trong lúc gửi file", start=start_receiver)
                minh.send("Trả lời trong lúc nhận file")
                quan.expect("Trả lời trong lúc nhận file", start=start_sender)
                if target.exists():
                    raise AssertionError("Transfer đã kết thúc khi kiểm tra chat")
            finally:
                (quan.directory / ".qa-file-release").touch()
            quan.expect("Minh đã xác nhận lưu file 'test_data.bin'", start=start_sender, timeout=15)
            for peer, start, marker in ((quan, start_sender, "[Gửi file]"), (minh, start_receiver, "[Tiến độ file]")):
                peer.expect_line(lambda line: marker in line and "100%" in line and source.name in line, start=start)
            if hashlib.sha256(target.read_bytes()).hexdigest() != sha256:
                raise AssertionError("SHA-256 của file nhận không khớp")

        expiration_start = None

        def cancel_upload():
            nonlocal expiration_start
            cancelled = an.directory / "cancelled.bin"
            cancelled.write_bytes(source.read_bytes())
            start = an.checkpoint()
            an.send("/chat Minh")
            an.expect("Đã kết nối với Minh", start=start)
            (an.directory / ".qa-file-armed").touch()
            receive_start = minh.checkpoint()
            an.send(f"/send {cancelled}")
            if not _wait_until((an.directory / ".qa-file-ready").exists, 3):
                raise AssertionError("Sender chưa gửi chunk đầu tiên trước /quit")
            minh.expect("Đang nhận file 'cancelled.bin'", start=receive_start)
            expiration_start = quan.checkpoint()
            an.send("/quit")
            if an.proc.wait(timeout=8) != 0:
                raise AssertionError("Sender thoát lỗi khi hủy upload")
            minh.expect("Nhận file 'cancelled.bin' thất bại", start=receive_start)
            downloads = minh.directory / "downloads"
            if not _wait_until(lambda: not list(downloads.glob("*.part")), 3):
                raise AssertionError("File tạm vẫn tồn tại sau hủy")
            if (downloads / cancelled.name).exists():
                raise AssertionError("File bị hủy đã xuất hiện trong downloads")

        def reconnect():
            start = quan.checkpoint()
            old_socket = links(quan, minh)
            if not old_socket:
                raise AssertionError("Thiếu chat connection trước khi mô phỏng gián đoạn")
            quan.send(f"@qa-drop {minh.port}")
            quan.expect("Kết nối với Minh đã bị ngắt", start=start)
            quan.expect("Đã tự động kết nối lại thành công với Minh", start=start, timeout=18)
            new_socket = links(quan, minh)
            if not new_socket or new_socket == old_socket:
                raise AssertionError("Reconnect chưa mở một TCP stream mới")
            start = minh.checkpoint()
            quan.send("Chat sau reconnect")
            minh.expect("Chat sau reconnect", start=start)
            start = quan.checkpoint()
            minh.send("Trả lời sau reconnect")
            quan.expect("Trả lời sau reconnect", start=start)

        def expire_peer():
            if expiration_start is None:
                raise AssertionError("Thiếu lần dừng peer để kiểm tra expiry")
            quan.expect("An đã offline (hết thời gian chờ)", start=expiration_start, timeout=12)
            start = quan.checkpoint()
            quan.send("/list")
            quan.expect("Danh sách Peer Online (1 người)", start=start)
            quan.expect(f"Minh (127.0.0.1:{minh_port})", start=start)

        def quota():
            filename = "quota.bin"
            declared_size = TEST_QUOTA_GIB * 1024**3 + 1
            expected = b"FILE_ACK|1|ERROR|"
            with socket.create_connection(("127.0.0.1", minh.port), timeout=3) as conn:
                conn.sendall(f"FILE|{filename}|{declared_size}|".encode("ascii"))
                with conn.makefile("rb") as reply:
                    if reply.read(len(expected)) != expected:
                        raise AssertionError("Receiver chưa từ chối file vượt quota trước khi đọc payload")
            downloads = minh.directory / "downloads"
            if (downloads / filename).exists() or list(downloads.glob("*.part")):
                raise AssertionError("Lượt nhận bị từ chối để lại file")

        def invalid_header():
            with socket.create_connection(("127.0.0.1", minh.port), timeout=3) as conn:
                conn.sendall(b"INVALID_HEADER|")
            start = minh.checkpoint()
            minh.send("/help")
            minh.expect("Lệnh Discovery", start=start)

        def disconnect():
            start = minh.checkpoint()
            quan.proc.terminate()
            quan.proc.wait(timeout=3)
            minh.expect("Kết nối với Quan đã bị ngắt", start=start)

        def graceful_shutdown():
            minh.send("/quit")
            if minh.proc.wait(timeout=8) != 0:
                raise AssertionError("Peer thoát với exit code khác 0")
            saved = json.loads((minh.directory / "chat_history.json").read_text(encoding="utf-8"))
            if not any("Chào Minh từ Quan | UTF-8" in entry for messages in saved.values() for entry in messages):
                raise AssertionError("History sau /quit thiếu tin nhắn đã nhận")

        tests = (
            ("UDP discovery và /list", discovery),
            ("Cập nhật UDP away và online", status),
            ("Hai peer cùng mở TCP và hội tụ một stream", simultaneous_chat),
            ("TCP chat hai chiều theo số và UTF-8", chat),
            ("Ba peer, unread counter, history và /leave", multi_peer_unread),
            ("Broadcast qua worker riêng", broadcast),
            ("Truyền file, ACK và SHA-256", file_transfer),
            ("Quota downloads từ chối trước payload", quota),
            ("Từ chối header không hợp lệ", invalid_header),
            ("Hủy upload bằng /quit và dọn file tạm", cancel_upload),
            ("Tự reconnect khi UDP presence còn hoạt động", reconnect),
            ("Peer timeout và danh sách sau expiry", expire_peer),
            ("Abrupt disconnect", disconnect),
            ("Graceful shutdown và history", graceful_shutdown),
        )
        failed = False
        try:
            for peer in peers:
                peer.start()
                peer.expect("Hệ thống UDP Discovery đã sẵn sàng", timeout=10)
            for title, test in tests:
                checkpoints = {peer: peer.checkpoint() for peer in peers}
                try:
                    test()
                except (AssertionError, OSError, ValueError, subprocess.TimeoutExpired) as e:
                    failed = True
                    print(f"[FAIL] {title} — {e}")
                    for peer, start in checkpoints.items():
                        with peer.changed:
                            lines = [line for line in peer.logs[start:] if line.strip()]
                        if lines:
                            print(f"--- {peer.name} ---")
                            print("\n".join(lines[-12:]))
                else:
                    print(f"[PASS] {title}")
        except (AssertionError, OSError) as e:
            failed = True
            print(f"[FAIL] Khởi động test peers — {e}")
        finally:
            for peer in peers:
                peer.stop()
                if failed:
                    print(f"--- Log gần nhất của {peer.name} ---")
                    print("\n".join(peer.logs[-20:]))
        return int(failed)


if __name__ == "__main__":
    if sys.argv[1:2] == ["--peer"]:
        port = int(sys.argv[2])
        del sys.argv[1:3]
        _run_loopback_peer(port)
    else:
        raise SystemExit(run_tests())
