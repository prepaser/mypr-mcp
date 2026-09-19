import asyncio

import pytest

import mypr_mcp.kernel_api as api
from mypr_mcp.services import Shells


async def test_task_read_waits_and_preserves_cursor():
    buffer = api.OutputBuffer()

    async def work():
        await asyncio.sleep(0.01)
        buffer.write("héllo")
        return 1

    task = asyncio.create_task(work())
    handle = api.TaskHandle("task", task, buffer)
    page = await handle.read(max_bytes=3, wait_ms=100)
    assert page["output"] == "hé"
    assert page["has_more"]
    assert (await handle.expect("llo", page["cursor"], timeout=1))["matched"]
    await task


async def test_task_expect_timeout_keeps_retry_cursor():
    buffer = api.OutputBuffer()
    task = asyncio.create_task(asyncio.sleep(1))
    handle = api.TaskHandle("task", task, buffer)
    result = await handle.expect("missing", timeout=0.01)
    assert result["reason"] == "timeout"
    assert result["cursor"] == api.TaskHandle("task", task, buffer)._encode_output_cursor("all", 0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_shell_read_pages_large_event(tmp_path):
    service = Shells(tmp_path)
    try:
        job = await service.start("printf abcdef")
        cursor = None
        output = ""
        while True:
            page = await service.read(job["id"], cursor, max_bytes=2, wait_ms=100)
            output += "".join(event["text"] for event in page["output"])
            cursor = page["cursor"]
            if page["state"] == "succeeded" and not page["has_more"]:
                break
        assert output == "abcdef"
    finally:
        await service.close()


@pytest.mark.parametrize("job_id", ["/" * 32, "z" * 32, "../" + "a" * 29])
async def test_shell_read_rejects_invalid_persisted_job_ids(tmp_path, job_id):
    service = Shells(tmp_path)
    try:
        with pytest.raises(ValueError, match="invalid shell job ID"):
            await service.read(job_id)
    finally:
        await service.close()
