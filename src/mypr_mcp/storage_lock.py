"""Cross-process locking for workspace storage maintenance."""

from __future__ import annotations

import contextlib
import os
from pathlib import Path
from typing import IO

try:
    import fcntl
except ImportError:  # pragma: no cover - mypr-mcp currently targets Linux
    fcntl = None


class StorageLock:
    """An exclusive lock file shared by the manager and workspace kernel."""

    def __init__(self, path: str | os.PathLike[str]) -> None:
        self.path = Path(path)
        self._file: IO[str] | None = None

    def __enter__(self) -> StorageLock:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        flags = os.O_CREAT | os.O_RDWR | getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(self.path, flags, 0o600)
        self._file = os.fdopen(descriptor, "a+")
        if fcntl is not None:
            fcntl.flock(self._file.fileno(), fcntl.LOCK_EX)
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        file, self._file = self._file, None
        if file is None:
            return
        try:
            if fcntl is not None:
                with contextlib.suppress(OSError):
                    fcntl.flock(file.fileno(), fcntl.LOCK_UN)
        finally:
            file.close()
