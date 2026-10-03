import asyncio
import threading
from types import SimpleNamespace

from mypr_mcp import patching
from mypr_mcp.filesystem import Filesystem
from mypr_mcp.lsp_edits import PlannedOperation, sha256
from mypr_mcp.revisions import RevisionStore


async def test_lsp_history_preflight_keeps_other_cells_responsive(tmp_path, monkeypatch):
    fs = Filesystem(tmp_path)
    path = tmp_path / "file.py"
    path.write_bytes(b"old")
    plan = SimpleNamespace(
        operations=[PlannedOperation("update", path, b"old", b"new", sha256(b"old"))]
    )
    entered, release = threading.Event(), threading.Event()
    prepare = RevisionStore.prepare_changes_sync

    def delayed_prepare(store, plans):
        entered.set()
        if not release.wait(3):
            raise TimeoutError("event loop did not release preflight")
        return prepare(store, plans)

    monkeypatch.setattr(RevisionStore, "prepare_changes_sync", delayed_prepare)
    applying = asyncio.create_task(fs._apply_lsp_plan(plan))
    try:
        assert await asyncio.to_thread(entered.wait, 2)
        assert not applying.done()
        await asyncio.sleep(0)
    finally:
        release.set()
        result = await applying
    assert result["history_recorded"]
    assert path.read_bytes() == b"new"


async def test_lsp_edit_aborts_when_a_parent_directory_changes(tmp_path, monkeypatch):
    workspace = tmp_path / "workspace"
    parent = workspace / "package"
    outside = tmp_path / "outside"
    workspace.mkdir()
    parent.mkdir()
    outside.mkdir()
    path = parent / "module.py"
    external = outside / "module.py"
    path.write_bytes(b"old")
    external.write_bytes(b"outside")
    plan = SimpleNamespace(
        operations=[PlannedOperation("update", path, b"old", b"new", sha256(b"old"))]
    )
    original = patching._read_state
    swapped = False

    def read_state(*args, **kwargs):
        nonlocal swapped
        state = original(*args, **kwargs)
        if not swapped and state.display == "package/module.py":
            parent.rename(workspace / "package-old")
            (workspace / "package").symlink_to(outside, target_is_directory=True)
            swapped = True
        return state

    monkeypatch.setattr(patching, "_read_state", read_state)
    try:
        await Filesystem(workspace)._apply_lsp_plan(plan)
    except RuntimeError as exc:
        assert "parent directory changed" in str(exc)
    else:
        raise AssertionError("LSP edit applied after its parent directory changed")
    assert (workspace / "package-old" / "module.py").read_bytes() == b"old"
    assert external.read_bytes() == b"outside"
