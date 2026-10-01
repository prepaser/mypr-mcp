from __future__ import annotations

import stat
from pathlib import Path
from types import SimpleNamespace

import pytest

import mypr_mcp.patching as patching
from mypr_mcp.filesystem import Filesystem
from mypr_mcp.lsp_edits import PlannedOperation, sha256


def _plan(operations, preconditions=None):
    return SimpleNamespace(operations=operations, preconditions=preconditions or {})


@pytest.mark.asyncio
async def test_lsp_delete_create_and_create_delete_sequences_are_virtual(tmp_path: Path):
    path = tmp_path / "sample.py"
    path.write_bytes(b"old")
    path.chmod(0o640)
    fs = Filesystem(tmp_path)

    result = await fs._apply_lsp_plan(
        _plan(
            [
                PlannedOperation("delete", path, b"old", None, sha256(b"old")),
                PlannedOperation("create", path, None, b"new", None),
            ]
        )
    )

    assert path.read_bytes() == b"new"
    assert result["changes"][0]["operation"] == "update"
    assert result["changes"][0]["changed"] is True
    assert stat.S_IMODE(path.stat().st_mode) == 0o600

    same = tmp_path / "same.py"
    same.write_bytes(b"same")
    same.chmod(0o640)
    result = await fs._apply_lsp_plan(
        _plan(
            [
                PlannedOperation("delete", same, b"same", None, sha256(b"same")),
                PlannedOperation("create", same, None, b"same", None),
            ]
        )
    )

    assert result["changes"][0]["operation"] == "update"
    assert result["changes"][0]["changed"] is True
    assert stat.S_IMODE(same.stat().st_mode) == 0o600

    fresh = tmp_path / "fresh.py"
    result = await fs._apply_lsp_plan(
        _plan(
            [
                PlannedOperation("create", fresh, None, b"temporary", None),
                PlannedOperation("delete", fresh, b"temporary", None, sha256(b"temporary")),
            ]
        )
    )

    assert not fresh.exists()
    assert result["changes"] == []


@pytest.mark.asyncio
async def test_lsp_rechecks_paths_that_only_exist_in_intermediate_state(
    tmp_path: Path, monkeypatch
):
    path = tmp_path / "temporary.py"
    fs = Filesystem(tmp_path)
    store = fs._history_store()
    original_prepare = store.prepare_changes_sync

    def prepare(plans):
        result = original_prepare(plans)
        path.write_bytes(b"external")
        return result

    monkeypatch.setattr(store, "prepare_changes_sync", prepare)
    monkeypatch.setattr(fs, "_history_store", lambda: store)

    with pytest.raises(RuntimeError, match="changed while applying patch"):
        await fs._apply_lsp_plan(
            _plan(
                [
                    PlannedOperation("create", path, None, b"temporary", None),
                    PlannedOperation(
                        "delete", path, b"temporary", None, sha256(b"temporary")
                    ),
                ]
            )
        )

    assert path.read_bytes() == b"external"


@pytest.mark.asyncio
async def test_lsp_guard_precondition_rejects_changed_untouched_origin(tmp_path: Path):
    origin = tmp_path / "origin.py"
    target = tmp_path / "target.py"
    origin.write_bytes(b"origin")
    target.write_bytes(b"old")
    fs = Filesystem(tmp_path)
    origin.write_bytes(b"changed")

    with pytest.raises(ValueError, match="precondition is stale"):
        await fs._apply_lsp_plan(
            _plan(
                [PlannedOperation("update", target, b"old", b"new", sha256(b"old"))],
                {origin: sha256(b"origin")},
            )
        )

    assert target.read_bytes() == b"old"


@pytest.mark.asyncio
async def test_lsp_guard_precondition_rejects_created_absent_path(tmp_path: Path):
    guard = tmp_path / "missing.py"
    target = tmp_path / "target.py"
    target.write_bytes(b"old")
    fs = Filesystem(tmp_path)
    guard.write_bytes(b"created")

    with pytest.raises(ValueError, match="precondition is stale"):
        await fs._apply_lsp_plan(
            _plan(
                [PlannedOperation("update", target, b"old", b"new", sha256(b"old"))],
                {guard: None},
            )
        )

    assert target.read_bytes() == b"old"


@pytest.mark.asyncio
async def test_lsp_guard_precondition_rechecks_after_history_preflight(
    tmp_path: Path, monkeypatch
):
    origin = tmp_path / "origin.py"
    target = tmp_path / "target.py"
    origin.write_bytes(b"origin")
    target.write_bytes(b"old")
    fs = Filesystem(tmp_path)
    store = fs._history_store()
    original_prepare = store.prepare_changes_sync

    def prepare(plans):
        result = original_prepare(plans)
        origin.write_bytes(b"changed")
        return result

    monkeypatch.setattr(store, "prepare_changes_sync", prepare)
    monkeypatch.setattr(fs, "_history_store", lambda: store)
    with pytest.raises(RuntimeError, match="changed while applying patch"):
        await fs._apply_lsp_plan(
            _plan(
                [PlannedOperation("update", target, b"old", b"new", sha256(b"old"))],
                {origin: sha256(b"origin")},
            )
        )

    assert target.read_bytes() == b"old"


@pytest.mark.asyncio
async def test_lsp_guard_precondition_rechecks_after_staging(tmp_path: Path, monkeypatch):
    origin = tmp_path / "origin.py"
    target = tmp_path / "target.py"
    origin.write_bytes(b"origin")
    target.write_bytes(b"old")
    fs = Filesystem(tmp_path)
    original_mkstemp = patching.tempfile.mkstemp
    changed = False

    def mkstemp(*args, **kwargs):
        nonlocal changed
        result = original_mkstemp(*args, **kwargs)
        if not changed:
            changed = True
            origin.write_bytes(b"changed")
        return result

    monkeypatch.setattr(
        patching,
        "tempfile",
        SimpleNamespace(mkstemp=mkstemp),
    )
    with pytest.raises(RuntimeError, match="changed while applying patch"):
        await fs._apply_lsp_plan(
            _plan(
                [PlannedOperation("update", target, b"old", b"new", sha256(b"old"))],
                {origin: sha256(b"origin")},
            )
        )

    assert target.read_bytes() == b"old"


@pytest.mark.asyncio
async def test_lsp_guard_success_does_not_record_guard_history(tmp_path: Path):
    origin = tmp_path / "origin.py"
    target = tmp_path / "target.py"
    origin.write_bytes(b"origin")
    target.write_bytes(b"old")
    fs = Filesystem(tmp_path)
    store = fs._history_store()
    origin_index = tmp_path / store._index_path("origin.py")

    result = await fs._apply_lsp_plan(
        _plan(
            [PlannedOperation("update", target, b"old", b"new", sha256(b"old"))],
            {origin: sha256(b"origin")},
        )
    )

    assert result["history_recorded"] is True
    assert target.read_bytes() == b"new"
    assert not origin_index.exists()


@pytest.mark.asyncio
async def test_lsp_rename_update_preserves_source_mode_and_rename_delete_removes_source(
    tmp_path: Path,
):
    source = tmp_path / "source.py"
    destination = tmp_path / "destination.py"
    source.write_bytes(b"old")
    source.chmod(0o640)
    fs = Filesystem(tmp_path)

    result = await fs._apply_lsp_plan(
        _plan(
            [
                PlannedOperation(
                    "rename",
                    destination,
                    None,
                    b"old",
                    None,
                    source=source,
                    source_old=b"old",
                ),
                PlannedOperation("update", destination, b"old", b"new", sha256(b"old")),
            ]
        )
    )

    assert not source.exists()
    assert destination.read_bytes() == b"new"
    assert stat.S_IMODE(destination.stat().st_mode) == 0o640
    assert {change["operation"] for change in result["changes"]} == {"add", "delete"}

    destination = tmp_path / "destination-2.py"
    source.write_bytes(b"again")
    await fs._apply_lsp_plan(
        _plan(
            [
                PlannedOperation(
                    "rename",
                    destination,
                    None,
                    b"again",
                    None,
                    source=source,
                    source_old=b"again",
                ),
                PlannedOperation("delete", destination, b"again", None, sha256(b"again")),
            ]
        )
    )
    assert not source.exists()
    assert not destination.exists()


@pytest.mark.asyncio
async def test_lsp_recreated_source_and_stale_source_are_handled(tmp_path: Path):
    source = tmp_path / "source.py"
    destination = tmp_path / "destination.py"
    source.write_bytes(b"old")
    fs = Filesystem(tmp_path)
    source.write_bytes(b"changed")

    with pytest.raises(ValueError, match="stale"):
        await fs._apply_lsp_plan(
            _plan(
                [
                    PlannedOperation(
                        "rename",
                        destination,
                        None,
                        b"old",
                        None,
                        source=source,
                        source_old=b"old",
                    )
                ]
            )
        )
    assert source.read_bytes() == b"changed"
    assert not destination.exists()

    source.write_bytes(b"old")
    await fs._apply_lsp_plan(
        _plan(
            [
                PlannedOperation(
                    "rename",
                    destination,
                    None,
                    b"old",
                    None,
                    source=source,
                    source_old=b"old",
                ),
                PlannedOperation("create", source, None, b"replacement", None),
                PlannedOperation(
                    "update", source, b"replacement", b"updated", sha256(b"replacement")
                ),
            ]
        )
    )
    assert source.read_bytes() == b"updated"
    assert destination.read_bytes() == b"old"


@pytest.mark.asyncio
async def test_lsp_history_failure_rolls_back_normalized_sequence(tmp_path: Path, monkeypatch):
    source = tmp_path / "source.py"
    destination = tmp_path / "destination.py"
    source.write_bytes(b"old")
    fs = Filesystem(tmp_path)
    store = fs._history_store()

    def fail_record(_plans):
        raise RuntimeError("history failed")

    monkeypatch.setattr(store, "record_changes_sync", fail_record)
    monkeypatch.setattr(fs, "_history_store", lambda: store)
    with pytest.raises(RuntimeError, match="history record failed"):
        await fs._apply_lsp_plan(
            _plan(
                [
                    PlannedOperation(
                        "rename",
                        destination,
                        None,
                        b"old",
                        None,
                        source=source,
                        source_old=b"old",
                    ),
                    PlannedOperation("update", destination, b"old", b"new", sha256(b"old")),
                ]
            )
        )

    assert source.read_bytes() == b"old"
    assert not destination.exists()
