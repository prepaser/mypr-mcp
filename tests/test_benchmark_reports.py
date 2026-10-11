from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

_SPEC = importlib.util.spec_from_file_location(
    "mypr_benchmark_perf", Path(__file__).parents[1] / "benchmarks" / "perf.py"
)
assert _SPEC and _SPEC.loader
perf = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(perf)


class FakeStdio:
    async def __aenter__(self):
        return object(), object()

    async def __aexit__(self, *args):
        return None


class FakeResult:
    is_error = False

    def __init__(self, value):
        self.structured_content = value


class FakeSession:
    def __init__(self, initialize_error=None, init_error=None):
        self.initialize_error = initialize_error
        self.init_error = init_error

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return None

    async def initialize(self):
        if self.initialize_error:
            raise self.initialize_error

    async def call_tool(self, name, arguments):
        if self.init_error:
            raise self.init_error
        assert name == "init"
        return FakeResult({"runtime": {"capabilities": []}})


def sample(name):
    return {
        "name": name,
        "total_ms": 1.0,
        "calls": 1,
        "polls": 0,
        "serialized_response_bytes": 1,
        "output_events": 0,
        "output_text_bytes": 0,
        "inline_image_bytes": 0,
        "request_latency_ms": [1.0],
        "response_bytes_per_call": [1],
        "timing_ms": {},
    }


def prepare(monkeypatch, session, stopped):
    monkeypatch.setattr(perf, "source_environment", lambda: {"prepared": True})
    monkeypatch.setattr(perf, "revision", lambda: {"commit": "test", "dirty": False})
    monkeypatch.setattr(perf, "ClientSession", lambda reader, writer: session)
    monkeypatch.setattr(perf, "stdio_client", lambda parameters: FakeStdio())

    async def stop(workspace, env, manager_ready):
        stopped.append(manager_ready)

    monkeypatch.setattr(perf, "stop_manager", stop)


@pytest.mark.asyncio
async def test_startup_failure_writes_bounded_partial_report(tmp_path, monkeypatch):
    stopped = []
    prepare(monkeypatch, FakeSession(initialize_error=RuntimeError("x" * 1000)), stopped)

    async def failing_stop(workspace, env, manager_ready):
        stopped.append(manager_ready)
        raise RuntimeError("cleanup failed")

    monkeypatch.setattr(perf, "stop_manager", failing_stop)
    output = tmp_path / "nested" / "partial.json"

    report = await perf.run_benchmark(1, tmp_path, output)

    assert report["status"] == "failed"
    assert report["failure"]["phase"] == "frontend_initialize"
    assert len(report["failure"]["error"]) <= perf.MAX_ERROR_LENGTH
    assert report["startup"]["frontend_initialize_ms"] is not None
    assert report["startup"]["init_ms"] is None
    assert stopped == [False]
    assert [failure["phase"] for failure in report["failures"]] == [
        "frontend_initialize",
        "cleanup",
    ]
    saved = json.loads(output.read_text())
    assert saved["failure"]["phase"] == "frontend_initialize"
    assert saved["status"] == "failed"


@pytest.mark.asyncio
async def test_init_failure_keeps_manager_unready_and_records_duration(tmp_path, monkeypatch):
    stopped = []
    prepare(monkeypatch, FakeSession(init_error=RuntimeError("init failed")), stopped)

    report = await perf.run_benchmark(1, tmp_path)

    assert report["status"] == "failed"
    assert report["failure"]["phase"] == "init"
    assert report["startup"]["frontend_initialize_ms"] is not None
    assert report["startup"]["init_ms"] is not None
    assert stopped == [False]


@pytest.mark.asyncio
async def test_case_failure_stops_cases_skips_probes_and_checkpoints_samples(
    tmp_path, monkeypatch
):
    stopped = []
    prepare(monkeypatch, FakeSession(), stopped)
    monkeypatch.setattr(perf, "CASES", {"empty": "pass", "scalar": "42", "later": "3"})
    calls = []

    async def execute_case(session, name, code):
        calls.append(name)
        if name == "scalar":
            raise TimeoutError("uncertain")
        return sample(name)

    probe_calls = []

    async def probe(session, runs):
        probe_calls.append(runs)
        raise AssertionError("probes must not run after a case failure")

    monkeypatch.setattr(perf, "execute_case", execute_case)
    monkeypatch.setattr(perf, "available_output_probe", probe)
    output = tmp_path / "partial.json"

    report = await perf.run_benchmark(3, tmp_path, output)

    assert calls == ["warmup", "empty", "scalar"]
    assert probe_calls == []
    assert report["status"] == "failed"
    assert report["cases"]["empty"]["runs"] == 1
    assert report["cases"]["scalar"]["failed"] is True
    saved = json.loads(output.read_text())
    assert saved["cases"]["empty"]["runs"] == 1
    assert stopped == [True]


def test_compare_skips_failed_cases_and_handles_missing_probe_values(tmp_path, capsys):
    case = {
        "case_latency_ms": {"median": 10, "p95": 12},
        "request_latency_ms": {"median": 2, "p95": 3},
        "serialized_response_bytes": {"median": 100},
        "call_count": {"median": 2, "total": 4},
    }
    baseline = {
        "cases": {"good": case, "failed": {"failed": True}},
        "available_output_poll": {
            "request_latency_ms": {"median": None, "p95": None},
            "serialized_response_bytes": {"median": None},
        },
    }
    current = {
        "cases": {"good": case},
        "available_output_poll": {
            "request_latency_ms": {"median": 2, "p95": 3},
            "serialized_response_bytes": {"median": 100},
        },
    }
    baseline_path = tmp_path / "baseline.json"
    current_path = tmp_path / "current.json"
    baseline_path.write_text(json.dumps(baseline))
    current_path.write_text(json.dumps(current))

    perf.compare(baseline_path, current_path)

    output = capsys.readouterr().out
    assert "good" in output
    assert "failed" not in output
    assert "n/a" in output
