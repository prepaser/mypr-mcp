#!/usr/bin/env python3

from __future__ import annotations

import argparse
import asyncio
import base64
import contextlib
import json
import math
import os
import platform
import statistics
import subprocess
import sys
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

ROOT = Path(__file__).resolve().parents[1]
CASES = {
    "empty": "pass",
    "scalar": "42",
    "stream_output": "print('x' * 65536)",
    "korean": "print('한글 출력' * 2048)",
    "inline_image": (
        "from IPython.display import display; "
        "display({'image/png': "
        "'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgA'"
        "'I/ScL/nwAAAABJRU5ErkJggg=='}, "
        "raw=True)"
    ),
}


def result_data(result):
    if result.is_error:
        raise RuntimeError(result.content[0].text if result.content else "MCP call failed")
    data = result.structured_content
    if data is None:
        data = json.loads(result.content[0].text)
    return data


def percentile(values, percent):
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(percent * len(ordered)) - 1)]


def distribution(values):
    if not values:
        return {"count": 0, "median": None, "p95": None}
    return {
        "count": len(values),
        "median": round(statistics.median(values), 3),
        "p95": round(percentile(values, 0.95), 3),
    }


def response_image_bytes(result):
    return sum(
        len(base64.b64decode(item.data))
        for item in result.content
        if getattr(item, "type", None) == "image"
    )


def timing_values(result):
    metadata = getattr(result, "meta", None)
    if not isinstance(metadata, dict):
        return {}
    values = metadata.get("timing_ms")
    return values if isinstance(values, dict) else {}


async def request(session, tool, arguments):
    started = time.perf_counter()
    result = await session.call_tool(tool, arguments)
    elapsed_ms = (time.perf_counter() - started) * 1000
    response_bytes = len(result.model_dump_json(by_alias=True).encode("utf-8"))
    return result, elapsed_ms, response_bytes


async def execute_case(session, name, code):
    started = time.perf_counter()
    result, latency, size = await request(session, "execute", {"code": code, "wait_ms": 1000})
    request_latencies = [latency]
    response_sizes = [size]
    stage_times = {}
    for stage, value in timing_values(result).items():
        if isinstance(value, (int, float)):
            stage_times.setdefault(stage, []).append(float(value))
    output_events = 0
    output_text_bytes = 0
    inline_image_bytes = 0
    polls = 0
    preview = []

    while True:
        data = result_data(result)
        events = data.get("output", [])
        output_events += len(events)
        output_text_bytes += sum(
            len(event.get("text", "").encode("utf-8"))
            for event in events
            if isinstance(event.get("text", ""), str)
        )
        if name == "runtime_performance":
            preview.extend(
                event.get("text", "")
                for event in events
                if isinstance(event.get("text", ""), str)
            )
        inline_image_bytes += response_image_bytes(result)
        if data.get("state") not in {"queued", "running"} and not data.get("has_more"):
            if data.get("state") != "succeeded":
                raise RuntimeError(f"{name} failed: {data.get('error') or data.get('state')}")
            break
        result, latency, size = await request(
            session,
            "poll",
            {"exec_id": data["exec_id"], "cursor": data["cursor"], "wait_ms": 1000},
        )
        polls += 1
        request_latencies.append(latency)
        response_sizes.append(size)
        for stage, value in timing_values(result).items():
            if isinstance(value, (int, float)):
                stage_times.setdefault(stage, []).append(float(value))

    total_ms = (time.perf_counter() - started) * 1000
    sample = {
        "name": name,
        "total_ms": round(total_ms, 3),
        "calls": len(request_latencies),
        "polls": polls,
        "serialized_response_bytes": sum(response_sizes),
        "output_events": output_events,
        "output_text_bytes": output_text_bytes,
        "inline_image_bytes": inline_image_bytes,
        "request_latency_ms": request_latencies,
        "response_bytes_per_call": response_sizes,
        "timing_ms": stage_times,
    }
    if name == "runtime_performance":
        sample["output"] = "".join(preview)[:12000]
    return sample


async def available_output_probe(session, runs):
    samples = []
    code = (
        "ws.local['perf_gate'] = __import__('asyncio').Event()\n"
        "print('ready', flush=True)\n"
        "await ws.local['perf_gate'].wait()"
    )
    for _ in range(runs):
        result, _, _ = await request(session, "execute", {"code": code, "wait_ms": 0})
        data = result_data(result)
        exec_id = data["exec_id"]
        ready = any("ready" in event.get("text", "") for event in data.get("output", []))
        deadline = time.monotonic() + 30
        while not ready:  # noqa: ASYNC110
            if time.monotonic() >= deadline:
                raise TimeoutError("The benchmark cell did not produce its ready marker")
            await asyncio.sleep(0.01)
            result, _, _ = await request(
                session,
                "poll",
                {"exec_id": exec_id, "cursor": 0, "wait_ms": 0},
            )
            data = result_data(result)
            if data["state"] not in {"queued", "running"}:
                raise RuntimeError(f"The gated benchmark cell ended early: {data}")
            ready = any("ready" in event.get("text", "") for event in data.get("output", []))

        polled = None
        try:
            result, latency, size = await request(
                session,
                "poll",
                {"exec_id": exec_id, "cursor": 0, "wait_ms": 1000},
            )
            polled = result_data(result)
            if polled["state"] != "running" or not any(
                "ready" in event.get("text", "") for event in polled.get("output", [])
            ):
                raise RuntimeError(f"Available-output poll returned an unexpected page: {polled}")
            stages = {
                key: float(value)
                for key, value in timing_values(result).items()
                if isinstance(value, (int, float))
            }
            sample = {
                "request_latency_ms": round(latency, 3),
                "serialized_response_bytes": size,
                "state": polled["state"],
                "output_events": len(polled.get("output", [])),
                "timing_ms": stages,
            }
            samples.append(sample)
            release, _, _ = await request(
                session,
                "execute",
                {"code": "ws.local['perf_gate'].set()", "wait_ms": 1000},
            )
            result_data(release)
            cursor = polled["cursor"]
            while polled["state"] in {"queued", "running"} or polled.get("has_more"):
                result, _, _ = await request(
                    session,
                    "poll",
                    {"exec_id": exec_id, "cursor": cursor, "wait_ms": 1000},
                )
                polled = result_data(result)
                cursor = polled["cursor"]
        finally:
            if polled is None or polled["state"] in {"queued", "running"}:
                with contextlib.suppress(Exception):
                    release, _, _ = await request(
                        session,
                        "execute",
                        {"code": "ws.local['perf_gate'].set()", "wait_ms": 1000},
                    )
                    result_data(release)
    stage_names = sorted({key for sample in samples for key in sample["timing_ms"]})
    return {
        "runs": len(samples),
        "measured_calls": len(samples),
        "request_latency_ms": distribution([sample["request_latency_ms"] for sample in samples]),
        "serialized_response_bytes": distribution(
            [sample["serialized_response_bytes"] for sample in samples]
        ),
        "timing_ms": {
            stage: distribution(
                [sample["timing_ms"][stage] for sample in samples if stage in sample["timing_ms"]]
            )
            for stage in stage_names
        },
        "samples": samples,
    }


def summarize(samples):
    stages = {}
    latencies = []
    response_sizes = []
    for sample in samples:
        latencies.extend(sample["request_latency_ms"])
        response_sizes.append(sample["serialized_response_bytes"])
        for name, values in sample["timing_ms"].items():
            stages.setdefault(name, []).extend(values)
    return {
        "runs": len(samples),
        "call_count": {
            "median": round(statistics.median(sample["calls"] for sample in samples), 2),
            "min": min(sample["calls"] for sample in samples),
            "max": max(sample["calls"] for sample in samples),
            "total": sum(sample["calls"] for sample in samples),
        },
        "serialized_response_bytes": distribution(response_sizes),
        "request_latency_ms": distribution(latencies),
        "case_latency_ms": distribution([sample["total_ms"] for sample in samples]),
        "timing_ms": {name: distribution(values) for name, values in sorted(stages.items())},
        "output_events": sum(sample["output_events"] for sample in samples),
        "output_text_bytes": sum(sample["output_text_bytes"] for sample in samples),
        "inline_image_bytes": sum(sample["inline_image_bytes"] for sample in samples),
        "samples": samples,
    }


def revision():
    try:
        commit = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], cwd=ROOT, text=True
        ).strip()
        dirty = subprocess.call(["git", "diff", "--quiet"], cwd=ROOT) != 0
        return {"commit": commit, "dirty": dirty}
    except (OSError, subprocess.CalledProcessError):
        return {"commit": None, "dirty": None}


async def stop_manager(workspace, env, manager_ready):
    stop = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "mypr_mcp.cli",
        "stop",
        "--force",
        cwd=workspace,
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    communication = asyncio.create_task(stop.communicate())
    try:
        stdout, stderr = await asyncio.wait_for(asyncio.shield(communication), 60)
    except TimeoutError:
        with contextlib.suppress(ProcessLookupError):
            stop.terminate()
        try:
            await asyncio.wait_for(asyncio.shield(communication), 5)
        except TimeoutError:
            with contextlib.suppress(ProcessLookupError):
                stop.kill()
            await communication
        raise TimeoutError("Timed out stopping the benchmark manager") from None
    if stop.returncode:
        message = (stderr or stdout).decode(errors="replace")[-2000:]
        if not manager_ready and "No reachable workspace manager" in message:
            return
        raise RuntimeError("Could not stop the benchmark manager: " + message)


async def run_benchmark(runs, workspace_root):
    if runs < 1:
        raise ValueError("--runs must be at least 1")
    root = await asyncio.to_thread(Path(workspace_root).resolve)
    state_root = root / ".mypr"
    await asyncio.to_thread(state_root.mkdir, exist_ok=True)
    if not (ROOT / ".venv").is_dir():
        raise RuntimeError("Run this benchmark from a checkout with its project .venv installed")

    report = {
        "schema": 1,
        "created_at": datetime.now(UTC).isoformat(),
        "revision": revision(),
        "environment": {
            "python": sys.version.split()[0],
            "platform": platform.platform(),
            "workspace_root": str(root),
            "runs": runs,
        },
        "cases": {},
    }
    with tempfile.TemporaryDirectory(prefix="perf-", dir=state_root) as temporary:
        temporary = Path(temporary)
        workspace = temporary / "workspace"
        workspace.mkdir()
        state = workspace / ".mypr"
        state.mkdir()
        (state / "venv").symlink_to(ROOT / ".venv", target_is_directory=True)
        with tempfile.TemporaryDirectory(prefix="mypr-perf-runtime-") as runtime_dir:
            env = {
                **os.environ,
                "PYTHONPATH": str(ROOT / "src"),
                "XDG_RUNTIME_DIR": runtime_dir,
                "NO_COLOR": "1",
            }
            parameters = StdioServerParameters(
                command=sys.executable,
                args=["-m", "mypr_mcp.cli", "serve"],
                cwd=str(workspace),
                env=env,
            )
            manager_ready = False
            try:
                async with stdio_client(parameters) as (reader, writer):
                    async with ClientSession(reader, writer) as session:
                        await session.initialize()
                        init_result = await session.call_tool("init", {})
                        init = result_data(init_result)
                        manager_ready = True
                        report["runtime"] = {
                            key: value
                            for key, value in init.get("runtime", {}).items()
                            if key in {"manager_version", "bridge_version", "protocol_version"}
                        }
                        await execute_case(session, "warmup", "pass")
                        raw = {name: [] for name in CASES}
                        for _ in range(runs):
                            for name, code in CASES.items():
                                raw[name].append(await execute_case(session, name, code))
                        report["cases"] = {
                            name: summarize(samples) for name, samples in raw.items()
                        }
                        report["available_output_poll"] = await available_output_probe(
                            session, runs
                        )
                        if "performance" in init.get("runtime", {}).get("capabilities", []):
                            try:
                                report["runtime_performance"] = await execute_case(
                                    session,
                                    "runtime_performance",
                                    "import json; print(json.dumps(await ws.performance(), "
                                    "ensure_ascii=False))",
                                )
                            except Exception as exc:
                                report["runtime_performance_error"] = str(exc)
            finally:
                await stop_manager(workspace, env, manager_ready)
    return report


def compare(baseline_path, current_path):
    baseline = json.loads(Path(baseline_path).read_text())
    current = json.loads(Path(current_path).read_text())
    print(f"{'case':<18} {'metric':<22} {'baseline':>12} {'current':>12} {'change':>10}")
    for name in sorted(set(baseline.get("cases", {})) & set(current.get("cases", {}))):
        for metric, title in (
            ("case_latency_ms", "case median ms"),
            ("request_latency_ms", "request median ms"),
            ("request_latency_ms", "request p95 ms"),
            ("serialized_response_bytes", "response median bytes"),
        ):
            field = "p95" if title.endswith("p95 ms") else "median"
            before = baseline["cases"][name][metric][field]
            after = current["cases"][name][metric][field]
            if before is None or after is None:
                change = "n/a"
            else:
                change = f"{(after / before - 1) * 100:+.1f}%" if before else "n/a"
            print(f"{name:<18} {title:<22} {before!s:>12} {after!s:>12} {change:>10}")
    if "available_output_poll" in baseline and "available_output_poll" in current:
        for metric, title, field in (
            ("request_latency_ms", "poll request median ms", "median"),
            ("request_latency_ms", "poll request p95 ms", "p95"),
            ("serialized_response_bytes", "poll response median bytes", "median"),
        ):
            before = baseline["available_output_poll"][metric][field]
            after = current["available_output_poll"][metric][field]
            change = f"{(after / before - 1) * 100:+.1f}%" if before else "n/a"
            print(f"{'available_output':<18} {title:<22} {before!s:>12} {after!s:>12} {change:>10}")


def main():
    parser = argparse.ArgumentParser(description="Measure mypr-mcp request and output costs")
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="run the MCP benchmark")
    run.add_argument("--runs", type=int, default=5)
    run.add_argument("--workspace-root", type=Path, default=ROOT)
    run.add_argument("--output", type=Path)
    diff = commands.add_parser("compare", help="compare two saved benchmark JSON reports")
    diff.add_argument("baseline", type=Path)
    diff.add_argument("current", type=Path)
    args = parser.parse_args()
    if args.command == "compare":
        compare(args.baseline, args.current)
        return
    report = asyncio.run(run_benchmark(args.runs, args.workspace_root))
    encoded = json.dumps(report, ensure_ascii=False, indent=2) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded)
    print(encoded, end="")


if __name__ == "__main__":
    main()
