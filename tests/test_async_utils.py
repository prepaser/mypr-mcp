import asyncio

import pytest

from mypr_mcp.async_utils import finish_owned, wait_owned
from mypr_mcp.browser_service import _await_shielded


@pytest.mark.parametrize("wait", [wait_owned, _await_shielded])
async def test_cancelled_owned_task_propagates_without_spinning(wait):
    task = asyncio.create_task(asyncio.sleep(0))
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    with pytest.raises(asyncio.CancelledError):
        await wait(task)


@pytest.mark.parametrize("propagate", [True, False])
async def test_repeated_caller_cancellation_finishes_owned_operation(propagate):
    entered, release = asyncio.Event(), asyncio.Event()

    async def operation():
        entered.set()
        await release.wait()
        return 42

    inner = asyncio.create_task(operation())
    waiter = asyncio.create_task(wait_owned(inner, propagate=propagate))
    await entered.wait()
    waiter.cancel()
    await asyncio.sleep(0)
    waiter.cancel()
    await asyncio.sleep(0)
    assert not inner.cancelled()
    assert not waiter.done()
    release.set()
    if propagate:
        with pytest.raises(asyncio.CancelledError):
            await waiter
    else:
        assert await waiter == 42
    assert inner.result() == 42


async def test_finish_owned_returns_cancellation_flag_after_completion():
    entered, release = asyncio.Event(), asyncio.Event()

    async def operation():
        entered.set()
        await release.wait()
        return 42

    waiter = asyncio.create_task(finish_owned(operation()))
    await entered.wait()
    waiter.cancel()
    await asyncio.sleep(0)
    release.set()
    assert await waiter == (42, True)


async def test_owned_failure_remains_visible_after_caller_cancellation():
    entered, release = asyncio.Event(), asyncio.Event()

    async def operation():
        entered.set()
        await release.wait()
        raise ValueError("owned failure")

    waiter = asyncio.create_task(wait_owned(operation()))
    await entered.wait()
    waiter.cancel()
    await asyncio.sleep(0)
    release.set()
    with pytest.raises(ValueError, match="owned failure"):
        await waiter
