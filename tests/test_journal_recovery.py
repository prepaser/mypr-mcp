import json

from mypr_mcp.history import History
from mypr_mcp.runtime import Runtime


async def test_cold_execution_poll_recovers_damaged_output(tmp_path):
    runtime = Runtime(tmp_path)
    runtime.generation = "new"
    runtime.response_limit = 4096
    runs = runtime.root / "runs"
    runs.mkdir()
    ident = "a" * 32
    (runs / f"{ident}.json").write_text(
        json.dumps(
            {
                "state": "succeeded",
                "generation": "old",
                "truncated": False,
            }
        )
    )
    journal = runs / f"{ident}.jsonl"
    original = b'{"text":"saved"}\n{"text":"cut'
    journal.write_bytes(original)
    result = await Runtime.poll(runtime, ident)
    assert result["state"] == "succeeded"
    assert result["execution_generation"] == "old"
    assert result["output"][0]["text"] == "saved"
    assert result["warnings"][0]["code"] == "journal_truncated"
    assert result["cursor"] == 2
    assert not result["has_more"]
    end = await Runtime.poll(runtime, ident, cursor=result["cursor"])
    assert end["output"] == []
    assert not end["has_more"]
    assert journal.read_bytes() == original


async def test_cold_execution_poll_reports_durable_output_eviction(tmp_path):
    runtime = Runtime(tmp_path)
    runtime.generation = "new"
    runtime.response_limit = 4096
    runs = runtime.root / "runs"
    runs.mkdir()
    ident = "b" * 32
    (runs / f"{ident}.json").write_text(
        json.dumps({"id": ident, "state": "succeeded", "generation": "old"})
    )
    (runs / f"{ident}.jsonl").write_text('{"text":"deleted output"}\n')
    history = History(tmp_path)
    history.record("execution", {"id": ident, "state": "succeeded", "output_evicted": True})
    runtime.history = history
    try:
        result = await runtime.poll(ident, wait_ms=0)
    finally:
        history.close()
    assert result["output"] == []
    assert result["cursor"] == 0
    assert result["has_more"] is False
    assert result["truncated"] is True
    assert result["output_evicted"] is True
    assert result["warnings"][0]["code"] == "output_expired"
