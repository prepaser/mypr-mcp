"""JSONL event pages indexed by on-disk byte offsets, with a single writer."""

import fcntl
import json
import os
import struct
import tempfile
from pathlib import Path

_HEADER = struct.Struct("<8sQQQQ")
_OFFSET = struct.Struct("<Q")
_MAGIC = b"MYPRIDX1"


def _identity(stat):
    return stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns


def _count(index, stat):
    index.seek(0)
    header = index.read(_HEADER.size)
    if len(header) != _HEADER.size or _HEADER.unpack(header) != (_MAGIC, *_identity(stat)):
        return None
    size = os.fstat(index.fileno()).st_size - _HEADER.size
    if size < _OFFSET.size or size % _OFFSET.size:
        return None
    if index.read(_OFFSET.size) != _OFFSET.pack(0):
        return None
    index.seek(-_OFFSET.size, os.SEEK_END)
    if index.read(_OFFSET.size) != _OFFSET.pack(stat.st_size):
        return None
    return size // _OFFSET.size - 1


def _cached_index(sidecar, stat):
    try:
        index = sidecar.open("rb")
    except FileNotFoundError:
        index = None
    if index is not None:
        count = _count(index, stat)
        if count is not None:
            return index, count
        index.close()
    return None


def _open_index(path):
    sidecar = path.with_suffix(".idx")
    cached = _cached_index(sidecar, path.stat())
    if cached is not None:
        return cached
    with path.open("rb") as source:
        fcntl.flock(source.fileno(), fcntl.LOCK_EX)
        stat = os.fstat(source.fileno())
        cached = _cached_index(sidecar, stat)
        if cached is not None:
            return cached
        return _build_index(path, sidecar, source, stat)


def _build_index(path, sidecar, source, stat):
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            dir=path.parent, prefix=sidecar.name, delete=False
        ) as target:
            temporary = Path(target.name)
            target.write(_HEADER.pack(_MAGIC, *_identity(stat)))
            target.write(_OFFSET.pack(0))
            count = 0
            while source.readline():
                target.write(_OFFSET.pack(source.tell()))
                count += 1
            if _identity(path.stat()) != _identity(stat):
                raise RuntimeError("Output journal changed while indexing")
        temporary.replace(sidecar)
        return sidecar.open("rb"), count
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def append_events(path: Path, events: list[dict]) -> None:
    if not events:
        return
    if not path.exists():
        path.touch()
    index, _ = _open_index(path)
    index.close()
    with path.open("ab") as journal, path.with_suffix(".idx").open("r+b") as index:
        index.seek(0, os.SEEK_END)
        for event in events:
            journal.write((json.dumps(event) + "\n").encode())
            index.write(_OFFSET.pack(journal.tell()))
        journal.flush()
        index.seek(0)
        index.write(_HEADER.pack(_MAGIC, *_identity(path.stat())))


def decode_event(line: bytes, line_number: int) -> dict:
    try:
        event = json.loads(line)
        if isinstance(event, dict):
            return event
    except ValueError, UnicodeError:
        pass
    truncated = not line.endswith(b"\n")
    return {
        "type": "warning",
        "code": "journal_truncated" if truncated else "journal_corrupt",
        "text": (
            f"Output journal line {line_number} is "
            f"{'incomplete' if truncated else 'corrupt'}; its output could not be recovered."
        ),
        "line": line_number,
    }


def read_page(path: Path, cursor: int, budget: int, initial_size: int = 0):
    if type(cursor) is not int or cursor < 0:
        raise ValueError("Invalid output cursor")
    try:
        index, count = _open_index(path)
    except FileNotFoundError:
        if cursor:
            raise ValueError("Invalid output cursor") from None
        return [], 0
    with index:
        if cursor > count:
            raise ValueError("Invalid output cursor")
        index.seek(_HEADER.size + cursor * _OFFSET.size)
        offset = _OFFSET.unpack(index.read(_OFFSET.size))[0]
    output, size = [], initial_size
    with path.open("rb") as source:
        source.seek(offset)
        for position in range(cursor, count):
            event = decode_event(source.readline(), position + 1)
            length = len(json.dumps(event).encode())
            if output and size + length > budget:
                break
            output.append(event)
            size += length
    return output, count
