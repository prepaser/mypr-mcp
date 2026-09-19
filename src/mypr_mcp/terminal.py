"""Small POSIX terminal helpers used by the shell service."""

from __future__ import annotations

import fcntl
import os
import struct
import termios
from contextlib import suppress


def validate_size(rows: int, cols: int) -> tuple[int, int]:
    if (
        type(rows) is not int
        or type(cols) is not int
        or not 1 <= rows <= 65535
        or not 1 <= cols <= 65535
    ):
        raise ValueError("rows and cols must be integers between 1 and 65535")
    return rows, cols


def resize(fd: int, rows: int, cols: int) -> None:
    rows, cols = validate_size(rows, cols)
    fcntl.ioctl(fd, termios.TIOCSWINSZ, struct.pack("HHHH", rows, cols, 0, 0))


def eof_byte(fd: int) -> bytes:
    attributes = termios.tcgetattr(fd)
    value = attributes[6][termios.VEOF]
    return bytes((value if isinstance(value, int) else ord(value),))


def close(fd: int | None) -> None:
    if fd is None:
        return
    with suppress(OSError):
        os.close(fd)
