#!/usr/bin/env python3
"""Smoke test hai peer thật trên loopback, với storage tạm riêng."""
import hashlib
import json
import os
import socket
import subprocess
import sys
import threading
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


def _test_ports():
    with (
        socket.socket(socket.AF_INET, socket.SOCK_STREAM) as first,
        socket.socket(socket.AF_INET, socket.SOCK_STREAM) as second,
        socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as udp,
    ):
        for sock in (first, second, udp):
            sock.bind(("127.0.0.1", 0))
        return first.getsockname()[1], second.getsockname()[1], udp.getsockname()[1]


def _run_loopback_peer(udp_port):
    """Giới hạn socket của test peer, giữ nguyên cấu hình của ứng dụng khi chạy bình thường."""
    import main
    import udp_handler

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
    main.main()


def run_tests():
    with TemporaryDirectory(prefix="lan-chat-test-") as directory:
        root = Path(directory)
        quan_port, minh_port, udp_port = _test_ports()
        quan = ProcessMonitor("Quan", quan_port, udp_port, root / "quan")
        minh = ProcessMonitor("Minh", minh_port, udp_port, root / "minh")
        for peer in (quan, minh):
            peer.directory.mkdir()

        source = quan.directory / "test_data.bin"
        source.write_bytes(os.urandom(TEST_FILE_SIZE))
        sha256 = hashlib.sha256(source.read_bytes()).hexdigest()

        def discovery():
            quan.expect("Minh (127.0.0.1:", timeout=10)
            start = quan.checkpoint()
            quan.send("/list")
            quan.expect(f"1. Minh (127.0.0.1:{minh_port})", start=start)

        def status():
            start = minh.checkpoint()
            minh.send("/status away")
            minh.expect("Đã cập nhật trạng thái cá nhân: 'away'", start=start)
            for _ in range(10):
                start = quan.checkpoint()
                quan.send("/list")
                if quan.wait_for(
                    lambda line: f"Minh (127.0.0.1:{minh_port})" in line and "away" in line,
                    start=start, timeout=0.5,
                ):
                    return
            raise AssertionError("Quan chưa thấy trạng thái away của Minh")

        def chat():
            start = quan.checkpoint()
            quan.send("/chat 1")
            quan.expect("Đã kết nối với Minh", start=start)
            start = minh.checkpoint()
            quan.send("Chào Minh từ Quan | UTF-8")
            minh.expect("Chào Minh từ Quan | UTF-8", start=start)

        def broadcast():
            start = minh.checkpoint()
            quan.send("/broadcast kiểm tra worker")
            minh.expect("[Broadcast] kiểm tra worker", start=start)

        def file_transfer():
            start_sender = quan.checkpoint()
            start_receiver = minh.checkpoint()
            quan.send(f"/send {source}")
            quan.send("Tin nhắn trong lúc gửi file")
            minh.expect("Tin nhắn trong lúc gửi file", start=start_receiver)
            quan.expect("Minh đã xác nhận lưu file 'test_data.bin'", start=start_sender, timeout=15)
            target = minh.directory / "downloads" / source.name
            if hashlib.sha256(target.read_bytes()).hexdigest() != sha256:
                raise AssertionError("SHA-256 của file nhận không khớp")

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
            ("Cập nhật UDP away", status),
            ("TCP chat theo số và UTF-8", chat),
            ("Broadcast qua worker riêng", broadcast),
            ("Truyền file, ACK và SHA-256", file_transfer),
            ("Quota downloads từ chối trước payload", quota),
            ("Từ chối header không hợp lệ", invalid_header),
            ("Abrupt disconnect", disconnect),
            ("Graceful shutdown và history", graceful_shutdown),
        )
        failed = False
        try:
            for peer in (quan, minh):
                peer.start()
                peer.expect("Hệ thống UDP Discovery đã sẵn sàng", timeout=10)
            for title, test in tests:
                try:
                    test()
                except (AssertionError, OSError, ValueError, subprocess.TimeoutExpired) as e:
                    failed = True
                    print(f"[FAIL] {title} — {e}")
                else:
                    print(f"[PASS] {title}")
        except (AssertionError, OSError) as e:
            failed = True
            print(f"[FAIL] Khởi động test peers — {e}")
        finally:
            for peer in (quan, minh):
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
