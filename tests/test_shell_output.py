from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

import pytest

import mypr_mcp.services as services_module
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
async def test_missing_persisted_journal_is_reported_as_truncated_output(tmp_path: Path):
    shells = Shells(tmp_path)
    job_id = "b" * 32
    shells.jobs_root.mkdir(parents=True, exist_ok=True)
    (shells.jobs_root / f"{job_id}.json").write_text(
        json.dumps({
            "id": job_id,
            "state": "succeeded",
            "output_count": 1,
            "warnings": [{"code": "old", "text": str(index)} for index in range(4)],
        }),
        encoding="utf-8",
    )
    try:
        polled = await shells.poll(job_id)
        read = await shells.read(job_id)
    finally:
        await shells.close()

    for result in (polled, read):
        assert result["output"] == []
        assert result["truncated"] is True
        assert result["warnings"][0]["code"] == "journal_unavailable"
        assert result["warnings_truncated"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize("cursor", [0, 1])
async def test_read_journal_eviction_race_reports_missing_output(
    monkeypatch, tmp_path: Path, cursor: int
):
    shells = Shells(tmp_path)
    job_id = "e" * 32
    journal = shells.jobs_root / f"{job_id}.jsonl"
    shells.jobs_root.mkdir(parents=True, exist_ok=True)
    journal.write_text(
        "{\"stream\":\"stdout\",\"text\":\"one\"}\n"
        "{\"stream\":\"stdout\",\"text\":\"two\"}\n",
        encoding="utf-8",
    )
    (shells.jobs_root / f"{job_id}.json").write_text(
        json.dumps({"id": job_id, "state": "succeeded", "output_count": 2}),
        encoding="utf-8",
    )
    original = services_module.read_page

    def delete_before_read(path, *args):
        journal.unlink()
        return original(path, *args)

    monkeypatch.setattr(services_module, "read_page", delete_before_read)
    try:
        result = await shells.read(job_id, cursor=cursor)
    finally:
        await shells.close()

    assert result["output"] == []
    assert result["truncated"] is True
    assert result["warnings"][0]["code"] == "journal_unavailable"


@pytest.mark.asyncio
async def test_read_journal_forged_cursor_still_fails_when_file_exists(tmp_path: Path):
    shells = Shells(tmp_path)
    job_id = "f" * 32
    shells.jobs_root.mkdir(parents=True, exist_ok=True)
    (shells.jobs_root / f"{job_id}.jsonl").write_text(
        '{"stream":"stdout","text":"saved"}\n', encoding="utf-8"
    )
    (shells.jobs_root / f"{job_id}.json").write_text(
        json.dumps({"id": job_id, "state": "succeeded", "output_count": 1}),
        encoding="utf-8",
    )
    try:
        with pytest.raises(ValueError, match="Invalid output cursor"):
            await shells.read(job_id, cursor=2)
    finally:
        await shells.close()


@pytest.mark.asyncio
async def test_persisted_journal_without_metadata_recovers_lost_job(tmp_path: Path):
    shells = Shells(tmp_path)
    job_id = "c" * 32
    journal = shells.jobs_root / f"{job_id}.jsonl"
    journal.write_text(
        json.dumps({"stream": "stdout", "text": "before-crash"}) + "\n",
        encoding="utf-8",
    )
    try:
        polled = await shells.poll(job_id)
        read = await shells.read(job_id)
        unknown = await shells.poll("d" * 32)
        with pytest.raises(ValueError, match="unknown job"):
            await shells.read("d" * 32)
    finally:
        await shells.close()

    for result in (polled, read):
        assert result["state"] == "lost"
        assert result["outcome_unknown"] is True
        assert result["output"] == [{"stream": "stdout", "text": "before-crash"}]
        assert result["warnings"][0]["code"] == "shell_outcome_unknown"
    assert unknown["state"] == "unknown"
    assert unknown["output"] == []


@pytest.mark.asyncio
@pytest.mark.parametrize("output_errors", ["replace", "surrogateescape"])
async def test_shell_output_cap_flushes_counted_decoder_fragment(tmp_path: Path, output_errors):
    shells = Shells(tmp_path, output_limit=4)
    payload = b"a" * 3 + b"\xc3"
    try:
        started = await shells.start(
            [sys.executable, "-c", f"import sys; sys.stdout.buffer.write({payload!r})"],
            output_errors=output_errors,
        )
        await shells.wait(started["id"])
        result = await shells.poll(started["id"])
    finally:
        await shells.close()

    expected = payload.decode("utf-8", output_errors)
    assert "".join(event["text"] for event in result["output"]) == expected
    assert result["truncated"] is False


@pytest.mark.asyncio
async def test_shell_persistence_rejects_replaced_workspace(tmp_path: Path):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    shells = Shells(workspace)
    try:
        started = await shells.start([sys.executable, "-c", "import time; time.sleep(.2)"])
        moved = tmp_path / "moved"
        workspace.rename(moved)
        workspace.mkdir()
        (workspace / ".mypr" / "jobs").mkdir(parents=True)
        result = await shells.wait(started["id"])
        assert any(warning["code"] == "metadata_persist_failed" for warning in result["warnings"])
        assert not list((workspace / ".mypr" / "jobs").iterdir())
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
