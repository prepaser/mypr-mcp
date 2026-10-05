import json

import pytest

from mypr_mcp.scan_service import ScanService
from mypr_mcp.services import Shells


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
