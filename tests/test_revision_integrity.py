from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import threading
from contextlib import asynccontextmanager
from pathlib import Path

import pytest

import mypr_mcp.revisions as revisions_module
from mypr_mcp.ast_rewrite import _PlanStore, _workspace_identity
from mypr_mcp.filesystem import Filesystem
from mypr_mcp.revisions import RevisionStore
from mypr_mcp.storage import Storage


def _revision(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("version", 3, "metadata is invalid"),
        ("kind", "modules", "metadata is invalid"),
        ("resource", "other.py", "metadata is invalid"),
        ("count", 0, "metadata is invalid"),
        ("sequence", 0, "record is invalid"),
        ("next_sequence", 1, "behind its records"),
        ("pruned_before", 1, "prune marker is invalid"),
        ("size", 64 * 1024 * 1024 + 1, "record is invalid"),
    ],
)
async def test_sync_and_async_index_loaders_share_validation(
    tmp_path: Path, field: str, value, message: str
):
    fs = Filesystem(tmp_path)
    store = RevisionStore(tmp_path, fs, "files")
    resource = "sample.py"
    index = {
        "version": 2,
        "kind": "files",
        "resource": resource,
        "count": 1,
        "revisions": [
            {
                "sequence": 1,
                "revision": "a" * 64,
                "size": 1,
                "created_at": "now",
            }
        ],
        "next_sequence": 2,
    }
    if field in {"sequence", "size"}:
        index["revisions"][0][field] = value
    else:
        index[field] = value
    index_path = tmp_path / store._index_path(resource)
    index_path.parent.mkdir(parents=True)
    index_path.write_text(json.dumps(index), encoding="utf-8")

    with pytest.raises(ValueError, match=message) as sync_error:
        store._load_index_sync(resource)
    with pytest.raises(ValueError, match=message) as async_error:
        await store.history(resource)
    assert str(sync_error.value) == str(async_error.value)


@pytest.mark.asyncio
async def test_corrupt_index_is_rejected_before_patch_changes_file(tmp_path: Path):
    fs = Filesystem(tmp_path)
    store = RevisionStore(tmp_path, fs, "files")
    target = tmp_path / "sample.py"
    target.write_text("old()\n", encoding="utf-8")
    index_path = tmp_path / store._index_path("sample.py")
    index_path.parent.mkdir(parents=True)
    index_path.write_text(
        '{"version":2,"kind":"files","resource":"other.py",'
        '"count":0,"revisions":[],"next_sequence":1}',
        encoding="utf-8",
    )

    patch = """*** Begin Patch
*** Update File: sample.py
@@
-old()
+new()
*** End Patch"""
    with pytest.raises(ValueError, match="metadata is invalid"):
        await fs.apply_patch(patch)

    assert target.read_text(encoding="utf-8") == "old()\n"


@pytest.mark.asyncio
async def test_revision_index_symlink_is_rejected_before_target_update(tmp_path: Path):
    fs = Filesystem(tmp_path)
    store = RevisionStore(tmp_path, fs, "files")
    resource = "sample.txt"
    target = tmp_path / ".mypr" / "other-index.json"
    target.parent.mkdir(parents=True)
    target.write_text(
        json.dumps(
            {
                "version": 2,
                "kind": "files",
                "resource": resource,
                "count": 0,
                "revisions": [],
                "next_sequence": 1,
            }
        ),
        encoding="utf-8",
    )
    index = tmp_path / store._index_path(resource)
    index.parent.mkdir(parents=True, exist_ok=True)
    index.symlink_to(target)

    with pytest.raises(ValueError, match="must not contain symlinks"):
        await store.record(resource, [b"old"])

    assert json.loads(target.read_text(encoding="utf-8"))["count"] == 0
    assert index.is_symlink()


@pytest.mark.asyncio
async def test_corrupt_existing_blob_is_rejected_before_target_write(tmp_path: Path):
    fs = Filesystem(tmp_path)
    store = RevisionStore(tmp_path, fs, "files")
    old = b"old\n"
    new = "new\n"
    target = tmp_path / "sample.txt"
    target.write_bytes(old)
    await store.prepare("sample.txt", [old])
    store._blob_path(_revision(old)).write_bytes(b"corrupt\n")

    with pytest.raises(RuntimeError, match="SHA-256 check"):
        await store.commit("sample.txt", old.decode(), new, expected_hash=_revision(old))

    assert target.read_bytes() == old


@pytest.mark.parametrize("existing", [b"corrupt\n", b"new\n"])
def test_blob_install_race_validates_the_winner(tmp_path: Path, monkeypatch, existing: bytes):
    data = b"new\n"
    path = tmp_path / "object"

    def race(source, destination, *, follow_symlinks=True):
        Path(destination).write_bytes(existing)
        raise FileExistsError(destination)

    monkeypatch.setattr(revisions_module.os, "link", race)
    if existing == data:
        revisions_module._write_blob(path, data)
        assert path.read_bytes() == data
    else:
        with pytest.raises(RuntimeError, match="SHA-256 check"):
            revisions_module._write_blob(path, data)
        assert path.read_bytes() == existing


@pytest.mark.asyncio
async def test_ast_history_preflight_runs_under_storage_transaction_and_worker(
    tmp_path: Path, monkeypatch
):
    fs = Filesystem(tmp_path)
    path = tmp_path / "sample.py"
    old = b"foo()\n"
    new = b"bar()\n"
    path.write_bytes(old)
    plan_store = _PlanStore(tmp_path / ".mypr" / "rewrites")
    plan_id = plan_store.create(
        {
            "workspace": _workspace_identity(tmp_path),
            "history": True,
            "original_bytes": len(old),
            "planned_bytes": len(new),
            "files": [
                {
                    "path": "sample.py",
                    "display": "sample.py",
                    "old_hash": _revision(old),
                    "old_size": len(old),
                    "new": base64.b64encode(new).decode("ascii"),
                }
            ],
        }
    )
    actual_store = fs._history_store()
    state = {"transaction": False, "lock": False, "thread": None}

    class SpyStore:
        def _resource_for_path(self, value):
            return actual_store._resource_for_path(value)

        @asynccontextmanager
        async def transaction(self, resource):
            async with actual_store.transaction(resource):
                state["transaction"] = True
                try:
                    yield
                finally:
                    state["transaction"] = False

        def prepare_changes_sync(self, plans):
            state["thread"] = threading.get_ident()
            return actual_store.prepare_changes_sync(plans)

        def record_changes_sync(self, plans):
            return actual_store.record_changes_sync(plans)

    class CheckedLock:
        async def acquire(self):
            assert state["transaction"]
            state["lock"] = True

        def release(self):
            state["lock"] = False

    spy = SpyStore()
    monkeypatch.setattr(fs, "_history_store", lambda: spy)
    monkeypatch.setattr(fs, "_lock", lambda _path: CheckedLock())
    main_thread = threading.get_ident()

    result = await fs.apply_rewrite(plan_id)

    assert result["applied"] is True
    assert path.read_bytes() == new
    assert state["thread"] != main_thread
    assert state["lock"] is False


@pytest.mark.asyncio
async def test_ast_apply_and_gc_serialize_revision_index_updates(tmp_path: Path, monkeypatch):
    fs = Filesystem(tmp_path)
    path = tmp_path / "sample.py"
    previous = None
    for value in range(4):
        previous = await fs.write(
            "sample.py",
            f"value({value})\n",
            expected_hash=previous["revision"] if previous else None,
            overwrite=previous is None,
        )
    old = path.read_bytes()
    new = b"replacement()\n"
    plan_store = _PlanStore(tmp_path / ".mypr" / "rewrites")
    plan_id = plan_store.create(
        {
            "workspace": _workspace_identity(tmp_path),
            "history": True,
            "original_bytes": len(old),
            "planned_bytes": len(new),
            "files": [
                {
                    "path": "sample.py",
                    "display": "sample.py",
                    "old_hash": _revision(old),
                    "old_size": len(old),
                    "new": base64.b64encode(new).decode("ascii"),
                }
            ],
        }
    )

    actual_store = fs._history_store()
    prepared = threading.Event()
    release = threading.Event()
    original_prepare = actual_store.prepare_changes_sync

    def blocked_prepare(plans):
        result = original_prepare(plans)
        prepared.set()
        if not release.wait(3):
            raise TimeoutError("AST preflight barrier was not released")
        return result

    actual_store.prepare_changes_sync = blocked_prepare
    monkeypatch.setattr(fs, "_history_store", lambda: actual_store)

    apply_task = asyncio.create_task(fs.apply_rewrite(plan_id))
    await asyncio.wait_for(asyncio.to_thread(prepared.wait, 2), timeout=3)
    gc_task = asyncio.create_task(
        Storage(tmp_path).gc(dry_run=False, max_bytes=0, revision_keep=1)
    )
    await asyncio.sleep(0.1)
    assert not gc_task.done()

    release.set()
    applied = await asyncio.wait_for(apply_task, timeout=3)
    collected = await asyncio.wait_for(gc_task, timeout=3)

    assert applied["applied"] is True
    assert collected["revision_pruned"]
    index = json.loads(
        (tmp_path / actual_store._index_path("sample.py")).read_text(encoding="utf-8")
    )
    assert index["count"] == 1
    assert index["revisions"][0]["revision"] == _revision(new)
