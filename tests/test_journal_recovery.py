import json
from types import SimpleNamespace

from mypr_mcp.runtime import Runtime


async def test_cold_execution_poll_recovers_damaged_output(tmp_path):
    runs = tmp_path / "runs"
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
    runtime = SimpleNamespace(root=tmp_path, execs={}, generation="new", response_limit=4096)
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
