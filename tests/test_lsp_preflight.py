import asyncio
import threading
from types import SimpleNamespace

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
