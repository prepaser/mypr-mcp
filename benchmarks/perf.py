#!/usr/bin/env python3

from __future__ import annotations

import argparse
import asyncio
import base64
import contextlib
import importlib.metadata
import importlib.util
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
from tomllib import loads

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

ROOT = Path(__file__).resolve().parents[1]
CASE_DEADLINE_SECONDS = 180
MAX_ERROR_LENGTH = 500
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
    "shell_output": (
        "import json, sys\n"
        "job = await ws.shell.start([sys.executable, '-c', "
        "\"import sys; sys.stdout.write('x' * 262144)\"])\n"
        "result = await job\n"
        "print(json.dumps({'returncode': result.get('returncode'), "
        "'retained_output_bytes': len(job.output().encode('utf-8'))}))"
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


def bounded_error(error):
    text = " ".join(str(error).split()) or type(error).__name__
    if len(text) > MAX_ERROR_LENGTH:
        text = text[: MAX_ERROR_LENGTH - 3] + "..."
    return text


def report_failure(report, phase, error):
    failure = {"phase": phase, "error": bounded_error(error)}
    report.setdefault("failures", []).append(failure)
    report["status"] = "failed"
    if report.get("failure") is None:
        report["failure"] = failure
    return failure


def source_environment():
    project = ROOT / "pyproject.toml"
    source_version = None
    with contextlib.suppress(OSError, KeyError, TypeError, ValueError):
        source_version = loads(project.read_text(encoding="utf-8"))["project"]["version"]
    try:
        distribution = importlib.metadata.distribution("mypr-mcp")
    except importlib.metadata.PackageNotFoundError:
        installed_version = None
        installed_location = None
    else:
        installed_version = distribution.version
        installed_location = str(distribution.locate_file(""))
    source_root = (ROOT / "src").resolve()
    package_root = source_root / "mypr_mcp"
    venv = (ROOT / ".venv").resolve()
    try:
        runner_origin = importlib.util.find_spec("mypr_mcp").origin
    except (ImportError, AttributeError, ModuleNotFoundError, ValueError):
        runner_origin = None
    source_origin_path = (package_root / "__init__.py").resolve()
    source_origin = str(source_origin_path)
    runner_source_selected = runner_origin == source_origin
    manager_source_selected = source_origin_path.is_file()
    return {
        "prepared": venv.is_dir() and package_root.is_dir(),
        "mode": "checkout_source",
        "code": revision(),
        "versions": {"source": source_version, "installed": installed_version},
        "venv": {"path": str(venv), "exists": venv.is_dir()},
        "source": {
            "root": str(ROOT),
            "package_root": str(package_root),
            "version": source_version,
            "exists": package_root.is_dir(),
        },
        "installed": {
            "distribution": "mypr-mcp",
            "version": installed_version,
            "location": installed_location,
        },
        "path_validation": {
            "python": str(Path(sys.executable).resolve()),
            "interpreter": str(Path(sys.executable).resolve()),
            "source_on_pythonpath": str(source_root),
            "strategy": "PYTHONPATH-precedence",
            "runner_origin": runner_origin,
            "manager_origin": source_origin if manager_source_selected else None,
            "runner_source_selected": runner_source_selected,
            "manager_source_selected": manager_source_selected,
            "source_selected": manager_source_selected,
        },
    }


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
                    release, _, _ = await asyncio.wait_for(
                        request(
                            session,
                            "execute",
                            {"code": "ws.local['perf_gate'].set()", "wait_ms": 1000},
                        ),
                        5,
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


def write_report(report, output_path):
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


async def run_benchmark(runs, workspace_root, output_path=None):
    root = await asyncio.to_thread(Path(workspace_root).resolve)
    environment_error = None
    try:
        prepared_environment = source_environment()
    except Exception as exc:
        prepared_environment = {"prepared": False, "error": bounded_error(exc)}
        environment_error = exc
    report = {
        "schema": 1,
        "status": "running",
        "created_at": datetime.now(UTC).isoformat(),
        "revision": revision(),
        "environment": {
            "python": sys.version.split()[0],
            "platform": platform.platform(),
            "workspace_root": str(root),
            "runs": runs,
            "prepared_environment": prepared_environment,
        },
        "cases": {},
        "failures": [],
        "failure": None,
    }
    workspace = None
    env = None
    manager_ready = False
    phase = "setup"

    async def checkpoint():
        if output_path is None:
            return
        with contextlib.suppress(Exception):
            await asyncio.to_thread(write_report, report, output_path)

    await checkpoint()
    try:
        if environment_error is not None:
            raise RuntimeError("Could not inspect the benchmark environment") from environment_error
        if runs < 1:
            raise ValueError("--runs must be at least 1")
        state_root = root / ".mypr"
        await asyncio.to_thread(state_root.mkdir, exist_ok=True)
        if not (ROOT / ".venv").is_dir():
            raise RuntimeError(
                "Run this benchmark from a checkout with its project .venv installed"
            )
        with tempfile.TemporaryDirectory(prefix="perf-", dir=state_root) as temporary:
            temporary = Path(temporary)
            workspace = temporary / "workspace"
            workspace.mkdir()
            state = workspace / ".mypr"
            state.mkdir()
            (state / "venv").symlink_to(ROOT / ".venv", target_is_directory=True)
            with contextlib.ExitStack() as resources:
                runtime_dir = resources.enter_context(
                    tempfile.TemporaryDirectory(prefix="mypr-perf-runtime-")
                )
                xdg_dir = resources.enter_context(
                    tempfile.TemporaryDirectory(prefix="mypr-perf-xdg-")
                )
                env = {
                    key: value
                    for key, value in os.environ.items()
                    if not key.startswith("MYPR_")
                }
                env.update(
                    {
                        "PYTHONPATH": str(ROOT / "src"),
                        "XDG_RUNTIME_DIR": runtime_dir,
                        "XDG_CONFIG_HOME": str(Path(xdg_dir) / "config"),
                        "XDG_STATE_HOME": str(Path(xdg_dir) / "state"),
                        "XDG_DATA_HOME": str(Path(xdg_dir) / "data"),
                        "XDG_CACHE_HOME": str(Path(xdg_dir) / "cache"),
                        "NO_COLOR": "1",
                    }
                )
                parameters = StdioServerParameters(
                    command=sys.executable,
                    args=["-m", "mypr_mcp.cli", "serve"],
                    cwd=str(workspace),
                    env=env,
                )
                primary_error = None
                cleanup_error = None
                try:
                    phase = "frontend_initialize"
                    report["startup"] = {
                        "frontend_initialize_ms": None,
                        "init_ms": None,
                    }
                    await checkpoint()
                    frontend_started = time.perf_counter()
                    try:
                        async with stdio_client(parameters) as (reader, writer):
                            async with ClientSession(reader, writer) as session:
                                try:
                                    await asyncio.wait_for(
                                        session.initialize(), CASE_DEADLINE_SECONDS
                                    )
                                finally:
                                    report["startup"]["frontend_initialize_ms"] = round(
                                        (time.perf_counter() - frontend_started) * 1000, 3
                                    )
                                    await checkpoint()
                                phase = "init"
                                init_started = time.perf_counter()
                                try:
                                    init_result = await asyncio.wait_for(
                                        session.call_tool("init", {}), CASE_DEADLINE_SECONDS
                                    )
                                finally:
                                    report["startup"]["init_ms"] = round(
                                        (time.perf_counter() - init_started) * 1000, 3
                                    )
                                    await checkpoint()
                                init = result_data(init_result)
                                manager_ready = True
                                report["runtime"] = {
                                    key: value
                                    for key, value in init.get("runtime", {}).items()
                                    if key
                                    in {"manager_version", "bridge_version", "protocol_version"}
                                }
                                phase = "warmup"
                                await asyncio.wait_for(
                                    execute_case(session, "warmup", "pass"), CASE_DEADLINE_SECONDS
                                )
                                raw = {name: [] for name in CASES}
                                case_failures = set()
                                case_aborted = False
                                for _ in range(runs):
                                    for name, code in CASES.items():
                                        phase = f"case:{name}"
                                        try:
                                            sample = await asyncio.wait_for(
                                                execute_case(session, name, code),
                                                CASE_DEADLINE_SECONDS,
                                            )
                                        except Exception as exc:
                                            failure = report_failure(report, f"case:{name}", exc)
                                            case_failures.add(name)
                                            report["cases"][name] = {
                                                "failed": True,
                                                "failure": failure,
                                            }
                                            await checkpoint()
                                            case_aborted = True
                                            break
                                        else:
                                            raw[name].append(sample)
                                            summary = summarize(raw[name])
                                            report["cases"][name] = summary
                                            await checkpoint()
                                    if case_aborted:
                                        break
                                for name, samples in raw.items():
                                    if not samples:
                                        continue
                                    summary = summarize(samples)
                                    if name in case_failures:
                                        summary["failed"] = True
                                    report["cases"][name] = summary
                                if not case_aborted:
                                    probe_failed = False
                                    try:
                                        phase = "available_output_poll"
                                        report["available_output_poll"] = await asyncio.wait_for(
                                            available_output_probe(session, runs),
                                            CASE_DEADLINE_SECONDS,
                                        )
                                        await checkpoint()
                                    except Exception as exc:
                                        report_failure(report, "available_output_poll", exc)
                                        probe_failed = True
                                        await checkpoint()
                                    capabilities = init.get("runtime", {}).get("capabilities", [])
                                    if not probe_failed and "performance" in capabilities:
                                        try:
                                            phase = "runtime_performance"
                                            report["runtime_performance"] = await asyncio.wait_for(
                                                execute_case(
                                                    session,
                                                    "runtime_performance",
                                                    "import json; print(json.dumps("
                                                    "await ws.performance(), ensure_ascii=False))",
                                                ),
                                                CASE_DEADLINE_SECONDS,
                                            )
                                            await checkpoint()
                                        except Exception as exc:
                                            report["runtime_performance_error"] = bounded_error(exc)
                                            report_failure(report, "runtime_performance", exc)
                                            await checkpoint()
                    except Exception as exc:
                        primary_error = (phase, exc)
                    finally:
                        if report["startup"]["frontend_initialize_ms"] is None:
                            report["startup"]["frontend_initialize_ms"] = round(
                                (time.perf_counter() - frontend_started) * 1000, 3
                            )
                            await checkpoint()
                        phase = "cleanup"
                        try:
                            await stop_manager(workspace, env, manager_ready)
                        except Exception as exc:
                            cleanup_error = exc
                    if primary_error is not None:
                        report_failure(report, *primary_error)
                    if cleanup_error is not None:
                        report_failure(report, "cleanup", cleanup_error)
                    await checkpoint()
                except Exception as exc:
                    report_failure(report, phase, exc)
    except Exception as exc:
        report_failure(report, phase, exc)
    finally:
        report["status"] = "failed" if report["failures"] else "succeeded"
        await checkpoint()
    return report


def compare(baseline_path, current_path):
    baseline = json.loads(Path(baseline_path).read_text())
    current = json.loads(Path(current_path).read_text())
    print(f"{'case':<18} {'metric':<22} {'baseline':>12} {'current':>12} {'change':>10}")

    def successful_cases(report):
        cases = report.get("cases", {}) if isinstance(report, dict) else {}
        if not isinstance(cases, dict):
            return {}
        return {
            name: case
            for name, case in cases.items()
            if isinstance(case, dict) and not case.get("failed")
        }

    baseline_cases = successful_cases(baseline)
    current_cases = successful_cases(current)

    def metric_value(case, metric, field):
        value = case.get(metric) if isinstance(case, dict) else None
        value = value.get(field) if isinstance(value, dict) else None
        return value if isinstance(value, (int, float)) and not isinstance(value, bool) else None

    for name in sorted(set(baseline_cases) & set(current_cases)):
        baseline_case = baseline_cases[name]
        current_case = current_cases[name]
        for metric, title in (
            ("case_latency_ms", "case median ms"),
            ("request_latency_ms", "request median ms"),
            ("request_latency_ms", "request p95 ms"),
            ("serialized_response_bytes", "response median bytes"),
            ("call_count", "calls median"),
            ("call_count", "calls total"),
        ):
            field = (
                "p95"
                if title.endswith("p95 ms")
                else "total"
                if title.endswith("total")
                else "median"
            )
            before = metric_value(baseline_case, metric, field)
            after = metric_value(current_case, metric, field)
            if before is None or after is None:
                change = "n/a"
            else:
                change = f"{(after / before - 1) * 100:+.1f}%" if before else "n/a"
            print(f"{name:<18} {title:<22} {before!s:>12} {after!s:>12} {change:>10}")
    if (
        isinstance(baseline, dict)
        and isinstance(current, dict)
        and "available_output_poll" in baseline
        and "available_output_poll" in current
    ):
        for metric, title, field in (
            ("request_latency_ms", "poll request median ms", "median"),
            ("request_latency_ms", "poll request p95 ms", "p95"),
            ("serialized_response_bytes", "poll response median bytes", "median"),
        ):
            before = metric_value(baseline.get("available_output_poll"), metric, field)
            after = metric_value(current.get("available_output_poll"), metric, field)
            change = (
                f"{(after / before - 1) * 100:+.1f}%"
                if before is not None and after is not None and before != 0
                else "n/a"
            )
            print(f"{'available_output':<18} {title:<22} {before!s:>12} {after!s:>12} {change:>10}")
    for label, report in (("baseline", baseline), ("current", current)):
        failures = report.get("failures", [])
        if failures:
            print(f"{label} failures: {len(failures)}")


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
    report = asyncio.run(run_benchmark(args.runs, args.workspace_root, args.output))
    encoded = json.dumps(report, ensure_ascii=False, indent=2) + "\n"
    if args.output:
        write_report(report, args.output)
    print(encoded, end="")
    if report.get("status") != "succeeded":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
