import asyncio
import time

import pytest_asyncio

from mypr_mcp.history import History
from mypr_mcp.persistence import PersistenceWorker
from mypr_mcp.runtime import Runtime


@pytest_asyncio.fixture
async def runtime(tmp_path):
    runtime = Runtime(tmp_path)
    (runtime.root / "runs").mkdir()
    runtime.persistence = PersistenceWorker()
    runtime.history = await runtime.persistence.call(History, tmp_path)
    runtime.healthy = True
    record = {
        "id": "a" * 32,
        "generation": runtime.generation,
        "client_id": "reader",
        "state": "queued",
        "code": "pass",
        "created": time.time(),
        "events": [],
        "bytes": 0,
        "truncated": False,
        "done": asyncio.Event(),
        "idle": asyncio.Event(),
    }
    runtime.execs[record["id"]] = record
    try:
        yield runtime, record
    finally:
        await runtime.persistence.call(runtime.history.close)
        await runtime.persistence.close()


async def test_available_output_does_not_wait_for_a_running_cell(runtime):
    runtime, record = runtime
    record["state"] = "running"
    await runtime.append(record, {"type": "stream", "stream": "stdout", "text": "ready"})

    page = await asyncio.wait_for(runtime.poll(record["id"], wait_ms=30000), 2)

    assert page["state"] == "running"
    assert page["cursor"] == 1
    assert page["output"][0]["text"] == "ready"


async def test_new_output_wakes_all_pollers_without_finishing_cell(runtime):
    runtime, record = runtime
    record["state"] = "running"
    waiters = [asyncio.create_task(runtime.poll(record["id"], wait_ms=30000)) for _ in range(2)]
    try:
        await asyncio.sleep(0)
        await runtime.append(record, {"type": "stream", "stream": "stderr", "text": "progress"})
        pages = await asyncio.wait_for(asyncio.gather(*waiters), 2)
        assert all(page["state"] == "running" for page in pages)
        assert all(page["output"][0]["text"] == "progress" for page in pages)
    finally:
        for waiter in waiters:
            waiter.cancel()
        await asyncio.gather(*waiters, return_exceptions=True)


async def test_start_transition_does_not_add_an_empty_poll_round_trip(runtime):
    runtime, record = runtime
    waiter = asyncio.create_task(runtime.poll(record["id"], wait_ms=30000))
    try:
        await asyncio.sleep(0)
        await runtime.update_execution(record, {"state": "running"}, event="running")
        await runtime.finish(record, "succeeded")
        page = await asyncio.wait_for(waiter, 2)
        assert page["state"] == "succeeded"
        assert page["output"] == []
    finally:
        waiter.cancel()
        await asyncio.gather(waiter, return_exceptions=True)


async def test_cancelling_poll_cleans_waiters_without_cancelling_execution(runtime):
    runtime, record = runtime
    before = asyncio.all_tasks()
    waiter = asyncio.create_task(runtime.poll(record["id"], wait_ms=30000))
    await asyncio.sleep(0)
    waiter.cancel()
    await asyncio.gather(waiter, return_exceptions=True)

    assert record["state"] == "queued"
    assert asyncio.all_tasks() <= before
    await runtime.finish(record, "succeeded")
    assert (await runtime.poll(record["id"], wait_ms=30000))["state"] == "succeeded"


async def test_execute_wait_keeps_waiting_for_completion_after_initial_output(runtime):
    runtime, record = runtime
    record["state"] = "running"
    await runtime.append(record, {"type": "stream", "stream": "stdout", "text": "ready"})
    waiter = asyncio.create_task(runtime.poll(record["id"], wait_ms=30000, wake_on_output=False))
    try:
        await asyncio.sleep(0)
        await runtime.finish(record, "succeeded")
        page = await asyncio.wait_for(waiter, 2)
        assert page["state"] == "succeeded"
        assert page["output"][0]["text"] == "ready"
    finally:
        waiter.cancel()
        await asyncio.gather(waiter, return_exceptions=True)
