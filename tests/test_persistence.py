import asyncio
import threading

import pytest

from mypr_mcp.persistence import PersistenceWorker


async def test_cancelled_submit_waits_for_accepted_io_and_fifo_order():
    worker = PersistenceWorker(queue_size=1)
    loop = asyncio.get_running_loop()
    started = asyncio.Event()
    release = threading.Event()
    calls = []

    def blocking_write():
        calls.append(("start", threading.get_ident()))
        loop.call_soon_threadsafe(started.set)
        release.wait(5)
        calls.append(("finish", threading.get_ident()))

    try:
        first = asyncio.create_task(worker.call(blocking_write))
        await asyncio.wait_for(started.wait(), 2)
        second = asyncio.create_task(
            worker.call(lambda: calls.append(("second", threading.get_ident())))
        )
        third = asyncio.create_task(
            worker.call(lambda: calls.append(("third", threading.get_ident())))
        )
        await asyncio.sleep(0)
        first.cancel()
        await asyncio.sleep(0)
        assert not first.done()
        assert not third.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await first
        await asyncio.gather(second, third)
    finally:
        release.set()
        await worker.close()

    assert [name for name, _ in calls] == ["start", "finish", "second", "third"]
    assert len({thread_id for _, thread_id in calls}) == 1
