from __future__ import annotations

import base64
from pathlib import Path

import pytest

from mypr_mcp.filesystem import Filesystem


@pytest.mark.asyncio
async def test_workspace_file_history_and_binary_lifecycle(tmp_path: Path):
    fs = Filesystem(tmp_path)
    first = await fs.write("value.txt", "one\n")
    second = await fs.write("value.txt", "two\n", expected_hash=first["revision"])

    history = await fs.history("value.txt")
    assert [item["revision"] for item in history["items"]] == [
        second["revision"],
        first["revision"],
        "absent",
    ]
    absent = await fs.read_revision("value.txt", "absent")
    assert absent["recorded"] is True
    assert absent["absent"] is True
    restored = await fs.restore(
        "value.txt", first["revision"], expected_hash=second["revision"]
    )
    assert restored["history_recorded"]
    assert (tmp_path / "value.txt").read_text() == "one\n"

    binary = await fs.write_bytes("value.bin", b"\x00\xffpayload")
    page = await fs.read_bytes("value.bin")
    assert base64.b64decode(page["data_base64"]) == b"\x00\xffpayload"
    revision = await fs.read_revision("value.bin", binary["revision"])
    assert revision["binary"]
    assert base64.b64decode(revision["data_base64"]) == b"\x00\xffpayload"

    await fs.copy("value.bin", "copy.bin", expected_hash=binary["revision"])
    await fs.move("copy.bin", "moved.bin", expected_hash=binary["revision"])
    deleted = await fs.delete("moved.bin", expected_hash=binary["revision"])
    assert deleted["deleted"]
    assert not (tmp_path / "moved.bin").exists()


@pytest.mark.asyncio
async def test_destructive_lifecycle_requires_cas_and_destination_overwrite_is_rejected(
    tmp_path: Path,
):
    fs = Filesystem(tmp_path)
    await fs.write("source.txt", "source")
    with pytest.raises(ValueError, match="delete requires expected_hash"):
        await fs.delete("source.txt")
    with pytest.raises(ValueError, match="move requires expected_hash"):
        await fs.move("source.txt", "destination.txt")
    with pytest.raises(ValueError, match="destination overwrite"):
        await fs.copy("source.txt", "destination.txt", overwrite=True)


@pytest.mark.asyncio
async def test_history_failure_rolls_back_single_file(tmp_path: Path, monkeypatch):
    fs = Filesystem(tmp_path)
    first = await fs.write("value.txt", "one\n")
    original = fs.write

    async def fail_index(path, text, **kwargs):
        if ".mypr/revisions/index/" in str(path):
            raise OSError("injected index failure")
        return await original(path, text, **kwargs)

    monkeypatch.setattr(fs, "write", fail_index)
    with pytest.raises(RuntimeError, match="file change was rolled back"):
        await fs.write("value.txt", "two\n", expected_hash=first["revision"])
    assert (tmp_path / "value.txt").read_text() == "one\n"


@pytest.mark.asyncio
async def test_history_can_be_explicitly_disabled_for_large_files(tmp_path: Path, monkeypatch):
    import mypr_mcp.revisions as revisions

    monkeypatch.setattr(revisions, "_MAX_BLOB_BYTES", 3)
    fs = Filesystem(tmp_path)
    with pytest.raises(ValueError, match="history=False"):
        await fs.write("value.txt", "four")
    result = await fs.write("value.txt", "four", history=False)
    assert result["revision"]

    outside = tmp_path.parent / "outside-value.txt"
    result = await fs.write(outside, "four", overwrite=True)
    assert result["revision"]


@pytest.mark.asyncio
async def test_file_history_prunes_without_renumbering_sequences(tmp_path: Path, monkeypatch):
    import mypr_mcp.revisions as revisions

    monkeypatch.setattr(revisions, "_MAX_HISTORY_RECORDS", 2)
    fs = Filesystem(tmp_path)
    previous = None
    for value in range(5):
        previous = await fs.write(
            "value.txt",
            str(value),
            expected_hash=previous["revision"] if previous else None,
            overwrite=previous is None,
        )
    history = await fs.history("value.txt")
    assert [item["sequence"] for item in history["items"]] == [6, 5]
    assert history["pruned_before"] == 4


@pytest.mark.asyncio
async def test_user_editable_mypr_sources_use_file_history(tmp_path: Path):
    fs = Filesystem(tmp_path)
    await fs.write(".mypr/skills/demo/SKILL.md", "one", create_parents=True)
    await fs.write(".mypr/lib/ws_lib/demo.py", "one", create_parents=True)
    await fs.write(".mypr/runtime.json", "one", create_parents=True)

    assert (await fs.history(".mypr/skills/demo/SKILL.md"))["items"]
    assert (await fs.history(".mypr/lib/ws_lib/demo.py"))["items"]
    await fs.apply_patch(
        """*** Begin Patch
*** Add File: .mypr/lib/ws_lib/from_patch.py
+one
*** End Patch"""
    )
    assert (await fs.history(".mypr/lib/ws_lib/from_patch.py"))["items"]
    with pytest.raises(ValueError, match="regular workspace file"):
        await fs.history(".mypr/runtime.json")


@pytest.mark.asyncio
async def test_file_history_records_absence_and_restores_it(tmp_path: Path):
    fs = Filesystem(tmp_path)
    created = await fs.write("value.txt", "one")
    await fs.delete("value.txt", expected_hash=created["revision"])
    history = await fs.history("value.txt")
    absent = next(item for item in history["items"] if item["revision"] == "absent")
    assert absent["absent"] is True
    absence = await fs.read_revision("value.txt", absent["revision"])
    assert absence["absent"] is True

    recreated = await fs.write("value.txt", "two")
    restored = await fs.restore(
        "value.txt", absent["revision"], expected_hash=recreated["revision"]
    )
    assert restored["changed"] is True
    assert not (tmp_path / "value.txt").exists()
    noop = await fs.restore("value.txt", absent["revision"])
    assert noop["changed"] is False
    with pytest.raises(ValueError, match="Revision mismatch"):
        await fs.restore("value.txt", absent["revision"], expected_hash=recreated["revision"])

    empty = await fs.write_bytes("empty.bin", b"")
    empty_history = await fs.history("empty.bin")
    assert {item["revision"] for item in empty_history["items"]} >= {
        "absent",
        empty["revision"],
    }
