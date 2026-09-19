import asyncio
import os
from pathlib import Path

import pytest

from mypr_mcp.filesystem import Filesystem


async def test_native_bare_and_named_hunks_with_eof_anchor(tmp_path):
    fs = Filesystem(tmp_path)
    path = tmp_path / "file.py"
    path.write_text("def one():\n    old\ndef two():\n    old\n")
    await fs.apply_patch("""*** Begin Patch
*** Update File: file.py
@@ def two():
-    old
+    new
*** End of File
*** End Patch""")
    assert path.read_text() == "def one():\n    old\ndef two():\n    new\n"
    await fs.apply_patch("""*** Begin Patch
*** Update File: file.py
@@
-def one():
+def first():
*** End Patch""")
    assert path.read_text().startswith("def first():\n")


async def test_named_heading_restricts_matching_without_eof(tmp_path):
    path = tmp_path / "text"
    path.write_text("first\nold\nsecond\nold\ntail\n")
    await Filesystem(tmp_path).apply_patch("""*** Begin Patch
*** Update File: text
@@ second
-old
+new
*** End Patch""")
    assert path.read_text() == "first\nold\nsecond\nnew\ntail\n"


async def test_numeric_insertion_uses_zero_length_range_position(tmp_path):
    path = tmp_path / "text"
    path.write_text("first\nlast\n")
    await Filesystem(tmp_path).apply_patch("""*** Begin Patch
*** Update File: text
@@ -1,0 +2,1 @@
+middle
*** End Patch""")
    assert path.read_text() == "first\nmiddle\nlast\n"


async def test_added_line_preserves_context_line_endings(tmp_path):
    path = tmp_path / "text"
    path.write_bytes(b"first\r\nlast\n")
    await Filesystem(tmp_path).apply_patch("""*** Begin Patch
*** Update File: text
@@
+new
 first
 last
*** End Patch""")
    assert path.read_bytes() == b"new\r\nfirst\r\nlast\n"


async def test_diff_limit_does_not_hide_changed_files(tmp_path):
    result = await Filesystem(tmp_path).apply_patch(
        """*** Begin Patch
*** Add File: one
+one
*** Add File: two
+two
*** End Patch""",
        max_diff_bytes=1,
    )
    assert result["diff_truncated"]
    assert len(result["changes"]) == 2
    assert (tmp_path / "one").exists() and (tmp_path / "two").exists()


async def test_move_without_hunks_preserves_binary_contents(tmp_path):
    content = b"\x00\xffbinary\r\n"
    (tmp_path / "old.bin").write_bytes(content)
    result = await Filesystem(tmp_path).apply_patch("""*** Begin Patch
*** Update File: old.bin
*** Move to: new.bin
*** End Patch""")
    assert result["changes"][0]["operation"] == "move"
    assert (tmp_path / "new.bin").read_bytes() == content
    assert not (tmp_path / "old.bin").exists()


async def test_cleanup_failure_reports_successful_commit_with_warning(monkeypatch, tmp_path):
    (tmp_path / "text").write_text("before\n")
    unlink = Path.unlink

    def fail_backup_cleanup(path, *args, **kwargs):
        if ".mypr-backup." in path.name:
            raise PermissionError("injected cleanup failure")
        return unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", fail_backup_cleanup)
    result = await Filesystem(tmp_path).apply_patch("""*** Begin Patch
*** Update File: text
@@
-before
+after
*** End Patch""")
    assert result["changes"][0]["changed"]
    assert "injected cleanup failure" in result["warnings"][0]
    assert (tmp_path / "text").read_text() == "after\n"
    assert len(list(tmp_path.glob(".text.mypr-backup.*"))) == 1


async def test_failed_commit_after_move_restores_original_paths(monkeypatch, tmp_path):
    (tmp_path / "source").write_text("before\n")
    (tmp_path / "other").write_text("old\n")
    replace = os.replace

    def fail_one(source, destination):
        if Path(destination) == tmp_path / "other":
            raise OSError("injected commit failure")
        return replace(source, destination)

    monkeypatch.setattr(os, "replace", fail_one)
    with pytest.raises(OSError, match="injected commit"):
        await Filesystem(tmp_path).apply_patch("""*** Begin Patch
*** Update File: source
*** Move to: destination
@@
-before
+after
*** Update File: other
@@
-old
+new
*** End Patch""")
    assert (tmp_path / "source").read_text() == "before\n"
    assert (tmp_path / "other").read_text() == "old\n"
    assert not (tmp_path / "destination").exists()
    assert sorted(p.name for p in tmp_path.iterdir()) == ["other", "source"]


async def test_cancelled_lock_acquisition_releases_previous_locks(tmp_path):
    fs = Filesystem(tmp_path)
    first = fs._lock(tmp_path / "a")
    second = fs._lock(tmp_path / "b")
    await second.acquire()
    task = asyncio.create_task(
        fs.apply_patch("""*** Begin Patch
*** Add File: a
+a
*** Add File: b
+b
*** End Patch""")
    )
    try:
        await asyncio.sleep(0)
        assert first.locked()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not first.locked()
        assert not list(tmp_path.iterdir())
    finally:
        if first.locked():
            first.release()
        second.release()
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def test_unrelated_unicode_and_line_endings_are_preserved(tmp_path):
    path = tmp_path / "mixed.txt"
    path.write_bytes("before\r\nkeep\u2028inside\nlast\r\n".encode())
    await Filesystem(tmp_path).apply_patch("""*** Begin Patch
*** Update File: mixed.txt
@@
-before
+after
*** End Patch""")
    assert path.read_bytes() == "after\r\nkeep\u2028inside\nlast\r\n".encode()
