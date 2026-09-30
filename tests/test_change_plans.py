from __future__ import annotations

from pathlib import Path

import pytest

from mypr_mcp.change_plans import ChangePlanError, ChangePlanStore


def test_change_plans_round_trip_paths_and_bytes(tmp_path: Path):
    store = ChangePlanStore(tmp_path, "replace")
    plan_id = store.create(
        {"operations": [{"path": tmp_path / "x.py", "old": b"old", "new": b"new"}]}
    )
    payload = store.load(plan_id)
    assert payload["operations"][0]["path"] == tmp_path / "x.py"
    assert payload["operations"][0]["old"] == b"old"
    store.remove(plan_id)
    with pytest.raises(ChangePlanError):
        store.load(plan_id)


def test_change_plans_reject_oversized_payload(tmp_path: Path):
    store = ChangePlanStore(tmp_path, "replace", max_input_output_bytes=3)
    with pytest.raises(ChangePlanError, match="source buffers"):
        store.create({"operations": [{"old": b"old", "new": b"new!"}]})
