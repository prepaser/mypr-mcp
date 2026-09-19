import asyncio

import pytest

from mypr_mcp.filesystem import Filesystem
from mypr_mcp.locks import WorkspaceLocks


@pytest.mark.asyncio
async def test_workspace_locks_are_sorted_and_reentrant_acquisition_fails():
    locks = WorkspaceLocks(lambda: {"client_id": "client", "exec_id": "exec"})
    async with locks.acquire("z", "a", "a"):
        state = locks.list()
        assert state[0]["names"] == ["a", "z"]
        assert state[0]["owner"]["client_id"] == "client"
        with pytest.raises(RuntimeError, match="not re-entrant"):
            async with locks.acquire("other"):
                pass
    assert locks.list() == []


@pytest.mark.asyncio
async def test_workspace_locks_cancelled_waiter_does_not_leak():
    locks = WorkspaceLocks()
    entered = asyncio.Event()
    release = asyncio.Event()

    async def holder():
        async with locks.acquire("build"):
            entered.set()
            await release.wait()

    owner = asyncio.create_task(holder())
    await entered.wait()
    waiter = asyncio.create_task(_hold_lock(locks, "build"))
    await asyncio.sleep(0)
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    release.set()
    await owner
    async with locks.acquire("build"):
        pass
    assert locks.list() == []


@pytest.mark.asyncio
async def test_workspace_locks_release_when_owner_task_is_cancelled():
    locks = WorkspaceLocks()
    entered = asyncio.Event()

    async def holder():
        async with locks.acquire("build"):
            entered.set()
            await asyncio.Event().wait()

    owner = asyncio.create_task(holder())
    await entered.wait()
    owner.cancel()
    with pytest.raises(asyncio.CancelledError):
        await owner
    for _ in range(10):
        if not locks.list():
            break
        await asyncio.sleep(0)
    assert locks.list() == []


@pytest.mark.asyncio
async def test_workspace_locks_cleanup_survives_repeated_cancellation_requests():
    locks = WorkspaceLocks()
    loop = asyncio.get_running_loop()

    async def worker():
        current = asyncio.current_task()
        assert current is not None
        async with locks.acquire("build"):
            loop.call_soon(current.cancel)
            loop.call_soon(current.cancel)
            await asyncio.sleep(0)

    task = asyncio.create_task(worker())
    with pytest.raises(asyncio.CancelledError):
        await task
    assert locks.list() == []


@pytest.mark.asyncio
async def test_workspace_locks_reuse_one_owner_done_callback():
    locks = WorkspaceLocks()
    callback_counts = []

    async def worker():
        for _ in range(8):
            async with locks.acquire("build"):
                callback_counts.append(len(locks._watched_tasks))

    task = asyncio.create_task(worker())
    await task
    assert callback_counts == [1] * 8
    await asyncio.sleep(0)
    assert not locks._watched_tasks


@pytest.mark.asyncio
async def test_workspace_locks_can_exit_context_from_another_task():
    locks = WorkspaceLocks()
    context = locks.acquire("build")
    await context.__aenter__()
    assert locks.list()[0]["names"] == ["build"]

    async def exit_context():
        await context.__aexit__(None, None, None)

    await asyncio.create_task(exit_context())
    assert locks.list() == []
    async with locks.acquire("build"):
        pass


async def _hold_lock(locks: WorkspaceLocks, name: str):
    async with locks.acquire(name):
        await asyncio.Event().wait()


@pytest.mark.asyncio
async def test_filesystem_tree_is_bounded_deterministic_and_does_not_follow_links(tmp_path):
    (tmp_path / ".hidden").write_text("hidden")
    for name in ("c", "a", "b"):
        (tmp_path / name).write_text(name)
    (tmp_path / "nested").mkdir()
    (tmp_path / "nested" / "child").write_text("child")
    (tmp_path / "outside").mkdir()
    (tmp_path / "outside" / "secret").write_text("secret")
    (tmp_path / "nested" / "link").symlink_to(tmp_path / "outside")

    result = await Filesystem(tmp_path).tree(depth=3, max_entries=3)
    assert [entry["path"] for entry in result["entries"]] == ["a", "b", "c"]
    assert result["truncated"] is True
    assert all(entry["path"] != "nested/link/secret" for entry in result["entries"])

    hidden = await Filesystem(tmp_path).tree(depth=1, hidden=True)
    assert hidden["entries"][0]["path"] == ".hidden"
    assert (await Filesystem(tmp_path).stat("nested/link"))["kind"] == "symlink"
    assert (await Filesystem(tmp_path).stat("nested/link", follow_symlinks=True))[
        "kind"
    ] == "directory"


@pytest.mark.asyncio
async def test_filesystem_tree_depth_zero_and_stat_metadata(tmp_path):
    (tmp_path / "file.txt").write_text("hello")
    fs = Filesystem(tmp_path)
    result = await fs.tree(depth=0)
    assert result["path"] == "."
    assert "entries" not in result
    assert result["truncated"] is False
    metadata = await fs.stat("file.txt")
    assert metadata["kind"] == "file"
    assert metadata["size"] == 5
    assert isinstance(metadata["mtime_ns"], int)
    assert isinstance(metadata["mode"], int)
