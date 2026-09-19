import asyncio
import hashlib
import os
import stat
from pathlib import Path

import pytest

from mypr_mcp.filesystem import Filesystem


def digest(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


async def test_write_read_and_revision(tmp_path: Path):
    fs = Filesystem(tmp_path)
    created = await fs.write("src/example.py", "one\ntwo\nthree\n", create_parents=True)
    assert created["created"]
    assert created["revision"] == digest("one\ntwo\nthree\n")

    result = await fs.read("src/example.py", start_line=2, end_line=2)
    assert result["text"] == "two\n"
    assert result["start_line"] == result["end_line"] == 2
    assert result["truncated"] is False
    assert result["next_line"] is None
    assert result["revision"] == created["revision"]

    with pytest.raises(FileExistsError):
        await fs.write("src/example.py", "new")
    changed = await fs.write("src/example.py", "new", expected_hash=created["revision"])
    assert changed["overwritten"]
    assert (tmp_path / "src/example.py").read_text() == "new"


async def test_read_long_utf8_line_returns_byte_cursor(tmp_path: Path):
    fs = Filesystem(tmp_path)
    content = "가나다😀" * 20 + "\n끝\n"
    await fs.write("long.txt", content)
    first = await fs.read("long.txt", max_bytes=17)
    assert first["truncated"]
    assert first["next_line"] == 1
    assert first["next_byte"] == len(first["text"].encode())
    assert first["text"].encode().decode() == first["text"]

    rest = await fs.read("long.txt", start_line=1, start_byte=first["next_byte"], max_bytes=4096)
    assert first["text"] + rest["text"] == content
    with pytest.raises(ValueError, match="encoding boundary"):
        await fs.read("long.txt", start_line=1, start_byte=first["next_byte"] - 1)
    with pytest.raises(ValueError, match="first UTF-8"):
        await fs.read("long.txt", max_bytes=1)
    with pytest.raises(ValueError, match="UTF-8 only"):
        await fs.read("long.txt", encoding="utf-16")


async def test_stream_reader_handles_chunk_boundary_and_invalid_utf8(tmp_path: Path):
    fs = Filesystem(tmp_path)
    text = "a" * (64 * 1024 - 2) + "😀\nsecond\n"
    await fs.write("boundary.txt", text)
    result = await fs.read("boundary.txt", start_line=1, end_line=1, max_bytes=64 * 1024 + 3)
    assert result["text"] == text[: 64 * 1024 - 2] + "😀\n"
    assert result["next_line"] is None

    (tmp_path / "invalid.txt").write_bytes(b"ok\n\xff\n")
    with pytest.raises(UnicodeDecodeError):
        await fs.read("invalid.txt")


async def test_patch_requires_unique_matches_and_is_atomic(tmp_path: Path):
    fs = Filesystem(tmp_path)
    await fs.write("file.txt", "a\na\n")
    with pytest.raises(ValueError, match="one match"):
        await fs.patch("file.txt", [{"old": "a", "new": "b"}])
    assert (tmp_path / "file.txt").read_text() == "a\na\n"

    preview = await fs.patch(
        "file.txt",
        [{"old": "a", "new": "b", "count": "all"}],
        dry_run=True,
    )
    assert preview["changed"] and preview["dry_run"]
    assert (tmp_path / "file.txt").read_text() == "a\na\n"
    result = await fs.patch("file.txt", [{"old": "a", "new": "b", "count": "all"}])
    assert result["changed"]
    assert (tmp_path / "file.txt").read_text() == "b\nb\n"


async def test_patch_all_requires_a_match_and_diff_marks_missing_newline(tmp_path: Path):
    fs = Filesystem(tmp_path)
    await fs.write("file.txt", "before")
    with pytest.raises(ValueError, match="at least one match"):
        await fs.patch("file.txt", [{"old": "missing", "new": "x", "count": "all"}])
    result = await fs.patch("file.txt", [{"old": "before", "new": "after\n"}])
    assert "No newline at end of file" in result["diff"]


async def test_patch_cas_and_concurrent_writes(tmp_path: Path):
    fs = Filesystem(tmp_path)
    await fs.write("file.txt", "before")
    revision = (await fs.read("file.txt"))["revision"]
    first, second = await asyncio.gather(
        fs.patch("file.txt", [{"old": "before", "new": "first"}], expected_hash=revision),
        fs.patch("file.txt", [{"old": "before", "new": "second"}], expected_hash=revision),
        return_exceptions=True,
    )
    assert sum(isinstance(item, dict) for item in (first, second)) == 1
    assert sum(isinstance(item, ValueError) for item in (first, second)) == 1
    assert (tmp_path / "file.txt").read_text() in {"first", "second"}


async def test_write_preserves_permissions_and_follows_symlink(tmp_path: Path):
    target = tmp_path / "target.txt"
    target.write_text("old")
    target.chmod(0o640)
    link = tmp_path / "link.txt"
    link.symlink_to(target)
    fs = Filesystem(tmp_path)
    await fs.write("link.txt", "new", overwrite=True)
    assert link.is_symlink()
    assert target.read_text() == "new"
    assert stat.S_IMODE(target.stat().st_mode) == 0o640


async def test_non_regular_files_fail_before_blocking(tmp_path: Path):
    fifo = tmp_path / "pipe"
    os.mkfifo(fifo)
    fs = Filesystem(tmp_path)
    with pytest.raises(ValueError, match="regular file"):
        await fs.read("pipe")
    with pytest.raises(ValueError, match="regular file"):
        await fs.write("pipe", "data", overwrite=True)
    with pytest.raises(ValueError, match="regular file"):
        await fs.patch("pipe", [{"old": "x", "new": "y"}])


async def test_cancelled_write_waits_for_worker_before_unlocking(tmp_path: Path, monkeypatch):
    fs = Filesystem(tmp_path)
    await fs.write("file.txt", "old")
    original = os.replace

    # The worker must finish before the caller can release the per-path lock.
    # Use a thread-safe event because the replacement runs in asyncio.to_thread.
    import threading

    thread_started = threading.Event()
    thread_release = threading.Event()

    def delayed_replace_thread(source, destination):
        thread_started.set()
        thread_release.wait(timeout=5)
        original(source, destination)

    monkeypatch.setattr(os, "replace", delayed_replace_thread)
    task = asyncio.create_task(fs.write("file.txt", "new", overwrite=True))
    await asyncio.to_thread(thread_started.wait, 5)
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    thread_release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert (tmp_path / "file.txt").read_text() == "new"
