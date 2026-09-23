import asyncio

from mypr_mcp.runtime import Runtime


async def test_dispatch_timings_and_bridge_samples_are_bounded(workspace):
    runtime = Runtime(workspace)
    dispatch = runtime._dispatch

    async def fake_dispatch(req):
        if req.get("op") == "execute":
            await asyncio.sleep(0)
            return {"state": "succeeded"}
        return await dispatch(req)

    runtime._dispatch = fake_dispatch
    result = await runtime.dispatch(
        {
            "op": "execute",
            "_bridge_sample": {
                "bridge_ready": 1.25,
                "manager_rpc": 2,
                "bridge_total": -1,
                "arbitrary": 100,
            },
        }
    )

    assert result["_timing_ms"]["manager_dispatch"] >= 0
    snapshot = runtime.performance_snapshot()
    assert snapshot["manager_dispatch"]["execute"]["total_count"] == 1
    assert snapshot["bridge"]["bridge_ready"]["max"] == 1.25
    assert snapshot["bridge"]["manager_rpc"]["max"] == 2.0
    assert "bridge_total" not in snapshot["bridge"]
    assert "arbitrary" not in snapshot["bridge"]

    result = await runtime.dispatch({"op": "performance"})
    assert "manager_dispatch" in result
    assert "storage" in result
    assert "kernel" in result


def test_kernel_roundtrip_is_recorded_once_and_private_fields_stay_internal(tmp_path):
    runtime = Runtime(tmp_path)
    rec = {"_kernel_sent_at": 0.0, "_admitted_at": 0.0, "state": "succeeded"}

    runtime.observe_kernel_roundtrip(rec)
    runtime.observe_kernel_roundtrip(rec)

    assert runtime.timings.snapshot()["kernel.roundtrip"]["total_count"] == 1
    assert "_kernel_sent_at" not in runtime.public_record(rec)
    assert "_admitted_at" not in runtime.public_record(rec)
