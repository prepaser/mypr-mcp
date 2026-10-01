"""Bounded reads for persisted regular files."""

import os
import stat
from pathlib import Path


class PersistedFileError(OSError):
    """The path is not a readable persisted regular file."""


def read_bytes(
    path: Path, *, max_bytes: int | None = None, follow_symlinks: bool = True
) -> bytes:
    """Read a regular file without allowing special files to block forever."""

    flags = os.O_RDONLY | getattr(os, "O_NONBLOCK", 0)
    if not follow_symlinks:
        flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise PersistedFileError(f"persisted path is not a regular file: {path}")
        if max_bytes is not None and metadata.st_size > max_bytes:
            raise ValueError(f"persisted file exceeds {max_bytes} bytes")
        with os.fdopen(descriptor, "rb") as stream:
            descriptor = -1
            data = stream.read(max_bytes + 1 if max_bytes is not None else -1)
            after = os.fstat(stream.fileno())
            if max_bytes is not None and after.st_size > max_bytes:
                raise ValueError(f"persisted file exceeds {max_bytes} bytes")
            if (
                metadata.st_dev, metadata.st_ino, metadata.st_size,
                metadata.st_mtime_ns, metadata.st_ctime_ns,
            ) != (
                after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns,
            ):
                raise PersistedFileError(f"persisted file changed while reading: {path}")
        if max_bytes is not None and len(data) > max_bytes:
            raise ValueError(f"persisted file exceeds {max_bytes} bytes")
        return data
    finally:
        if descriptor != -1:
            os.close(descriptor)


def open_regular(path: Path, mode: str = "rb"):
    """Open a persisted regular file after validating its type."""

    if mode not in {"rb", "ab", "r+b"}:
        raise ValueError("unsupported persisted file mode")
    flags = {
        "rb": os.O_RDONLY,
        "ab": os.O_WRONLY | os.O_APPEND | os.O_CREAT,
        "r+b": os.O_RDWR,
    }[mode] | getattr(os, "O_NONBLOCK", 0)
    descriptor = os.open(path, flags)
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise PersistedFileError(f"persisted path is not a regular file: {path}")
        return os.fdopen(descriptor, mode)
    except BaseException:
        os.close(descriptor)
        raise
