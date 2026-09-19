from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

import pytest

from mypr_mcp.services import Shells


async def _wait_for_terminal(shells: Shells, job_id: str) -> dict:
    for _ in range(500):
        result = await shells.poll(job_id)
        if result["state"] in {"succeeded", "failed", "cancelled"}:
            return result
        await asyncio.sleep(0.01)
    pytest.fail("shell did not finish")


@pytest.mark.asyncio
async def test_output_write_failure_does_not_block_pipe_and_survives_eviction(
    monkeypatch, tmp_path: Path
):
    shells = Shells(tmp_path, completed_records=1, output_limit=4 * 1024 * 1024)

    def fail(_job, _event):
        raise OSError("simulated journal I/O failure")

    monkeypatch.setattr(Shells, "_write_output", staticmethod(fail))
    command = [
        sys.executable,
        "-c",
        "import sys; sys.stdout.write('x' * (2 * 1024 * 1024))",
    ]
    try:
        started = await shells.start(command)
        result = await _wait_for_terminal(shells, started["id"])
        assert result["state"] == "succeeded"
        assert result["result"] == {"returncode": 0}
        assert any(warning["code"] == "output_persist_failed" for warning in result["warnings"])
        assert len("".join(event["text"] for event in result["output"])) == 2 * 1024 * 1024
        shells.completed_records = 0
        shells._prune_completed()
        assert started["id"] not in shells._jobs

        persisted = await shells.poll(started["id"], cursor=result["cursor"])
        assert persisted["state"] == "succeeded"
        assert persisted["cursor"] == result["cursor"]
        assert persisted["warnings"] == result["warnings"]
        for cursor in (-1, True, 1.5, "1", result["cursor"] + 1):
            with pytest.raises(ValueError, match="cursor"):
                await shells.poll(started["id"], cursor=cursor)
    finally:
        await shells.close()


@pytest.mark.asyncio
async def test_metadata_write_failure_keeps_completed_job_bounded_in_memory(monkeypatch, tmp_path):
    shells = Shells(tmp_path, completed_records=0)

    def fail(_job):
        raise OSError("simulated metadata I/O failure")

    monkeypatch.setattr(Shells, "_write_metadata", staticmethod(fail))
    try:
        started = await shells.start("printf retained")
        result = await _wait_for_terminal(shells, started["id"])
        assert result["state"] == "succeeded"
        assert result["warnings"] == [
            {"code": "metadata_persist_failed", "text": "simulated metadata I/O failure"}
        ]
        assert started["id"] in shells._jobs
        next_job = await shells.start("printf next")
        await _wait_for_terminal(shells, next_job["id"])
        assert list(shells._jobs) == [next_job["id"]]
    finally:
        await shells.close()


@pytest.mark.asyncio
async def test_output_reader_failure_is_reported(tmp_path: Path):
    class BrokenStream:
        async def read(self, _size):
            raise OSError("simulated pipe read failure")

    shells = Shells(tmp_path)
    job = shells._new_job("0123456789abcdef0123456789abcdef", object(), 0)
    await shells._drain(job, BrokenStream(), "stdout")
    assert job.warnings == [{"code": "output_read_failed", "text": "simulated pipe read failure"}]


@pytest.mark.asyncio
async def test_partial_final_shell_journal_keeps_valid_prefix(tmp_path: Path):
    job_id = "0123456789abcdef0123456789abcdef"
    jobs = tmp_path / ".mypr" / "jobs"
    jobs.mkdir(parents=True)
    (jobs / f"{job_id}.json").write_text(
        json.dumps(
            {
                "id": job_id,
                "state": "succeeded",
                "result": {"returncode": 0},
                "error": None,
                "truncated": False,
                "warnings": [],
            }
        )
    )
    (jobs / f"{job_id}.jsonl").write_bytes(
        b'{"stream":"stdout","text":"ok"}\n{"stream":"stdout","text":"cut'
    )
    shells = Shells(tmp_path)
    try:
        result = await shells.poll(job_id)
        assert result["state"] == "succeeded"
        assert result["output"][0] == {"stream": "stdout", "text": "ok"}
        assert result["output"][1]["type"] == "warning"
        assert result["output"][1]["code"] == "journal_truncated"
        assert result["warnings"][0]["code"] == "journal_truncated"
        assert result["cursor"] == 2
    finally:
        await shells.close()


async def test_many_corrupt_shell_records_have_bounded_warnings(tmp_path):
    job_id = "a" * 32
    shells = Shells(tmp_path)
    (shells.jobs_root / f"{job_id}.json").write_text(
        json.dumps({"state": "succeeded", "result": {"returncode": 0}})
    )
    (shells.jobs_root / f"{job_id}.jsonl").write_bytes(b"broken\n" * 10)
    result = await shells.poll(job_id)
    assert len(result["output"]) == result["cursor"] == 10
    assert len(result["warnings"]) == 4
    assert result["warnings_truncated"]
    await shells.close()
