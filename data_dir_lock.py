"""Exclusive ownership of a local data directory for the lifetime of the CLI."""

import errno
import os
import stat
from pathlib import Path


LOCK_FILENAME = ".lan-chat.lock"


def lock_data_directory(directory: Path) -> None:
    """Acquire a nonblocking OS lock on a persistent file in the resolved directory."""
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(directory / LOCK_FILENAME, flags, 0o600)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise OSError("Mục khóa phải là file thường.")
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
            elif os.name == "posix":
                import fcntl

                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            else:
                raise OSError("Hệ điều hành không hỗ trợ khóa --data-dir.")
        except ImportError as error:
            raise OSError("Thiếu cơ chế khóa --data-dir của hệ điều hành.") from error
        except OSError as error:
            if error.errno in (errno.EACCES, errno.EAGAIN, errno.EWOULDBLOCK):
                raise BlockingIOError(
                    "Một instance khác có thể đang dùng thư mục này. Hãy chọn --data-dir riêng."
                ) from error
            raise
    except BaseException:
        os.close(fd)
        raise

    # Keep this non-inheritable descriptor open until the OS ends the process.
    # A daemon writer may still be running after bounded shutdown or an exception.
    # The lock file must persist so all contenders open the same file.
