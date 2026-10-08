import json

import pytest

from mypr_mcp.history import History
from mypr_mcp.runtime import Runtime
from mypr_mcp.scan_service import ScanService
from mypr_mcp.services import Shells
from mypr_mcp.storage import Storage


@pytest.mark.parametrize(
    "state,missing", [("succeeded", True), ("succeeded", False), ("running", True)]
)
async def test_saved_scan_output_availability(tmp_path, state, missing):
    root = tmp_path / ".mypr/scans"
    root.mkdir(parents=True)
    ident = "a" * 32
    output = root / f"{ident}.jsonl"
    if not missing:
        output.touch()
    (root / f"{ident}.json").write_text(json.dumps({
        "id": ident, "mode": "tcp", "state": state, "complete": state == "succeeded",
        "result_path": str(output), "result_count": 1 if missing else 0,
        "warnings": [], "truncated": False,
    }))
    shells = Shells(tmp_path)
    try:
        service = ScanService(tmp_path, shells)
        if state == "running":
            service._records[ident] = json.loads((root / f"{ident}.json").read_text())
            service._records[ident]["state"] = "running"
        page = await service.results(ident)
        assert page["results"] == []
        if missing and state == "succeeded":
            assert page["output_unavailable"] is True
            assert page["complete"] is False
            assert page["truncated"] is True
            assert page["warnings"][0]["code"] == "scan_output_unavailable"
        else:
            assert "output_unavailable" not in page
            assert page["has_more"] == (state == "running")
            assert page["complete"] == (state == "succeeded")
    finally:
        await shells.close()


@pytest.mark.parametrize("mode", ["tcp", "nmap"])
async def test_gc_evicted_scan_results_keep_missing_output_contract(tmp_path, mode):
    runtime = Runtime(tmp_path)
    runtime.history = History(tmp_path)
    runtime.scans = ScanService(tmp_path, runtime.shells)
    ident = "b" * 32
    output = runtime.scans.root / f"{ident}.jsonl"
    output.write_text(json.dumps({"host": "local", "state": "open"}) + "\n")
    record = {
        "id": ident, "kind": "scan", "mode": mode, "state": "succeeded",
        "complete": True, "result_path": str(output), "warnings": [],
    }
    runtime.scans._write_record(record)
    runtime.history.record("scan", record)
    try:
        collected = await Storage(tmp_path, history=runtime.history).gc(
            dry_run=False, max_bytes=0
        )
        assert any(item["path"].endswith(".jsonl") for item in collected["deleted"])
        page = await runtime.dispatch({"op": "scan_results", "id": ident})

        assert page["results"] == []
        assert page["state"] == "succeeded"
        assert page["expired"] is True
        assert page["truncated"] is True
        assert page["complete"] is False
        assert page["output_unavailable"] is True
        assert any(warning["code"] == "scan_output_unavailable" for warning in page["warnings"])
    finally:
        await runtime.shells.close()
        runtime.history.close()
