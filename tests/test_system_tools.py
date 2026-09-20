import asyncio
import json
import os
from pathlib import Path

import pytest

import mypr_mcp.system_tools as api


@pytest.mark.parametrize(
    "kwargs",
    [
        {"interval": 0},
        {"interval": True},
        {"interval": float("nan")},
        {"interval": 11, "timeout": 20},
        {"interval": 1, "timeout": 1},
        {"timeout": float("inf")},
        {"timeout": -1},
    ],
)
async def test_invalid_measurement_never_launches_helpers(tmp_path, kwargs, monkeypatch):
    tools = api.SystemTools(tmp_path)

    async def probe(*args):
        raise AssertionError("must validate before launch")

    monkeypatch.setattr(tools, "_probe", probe)
    with pytest.raises(ValueError):
        await tools.usage(**kwargs)


async def test_probes_are_bounded_across_concurrent_calls_and_fail_independently(
    tmp_path, monkeypatch
):
    tools = api.SystemTools(tmp_path)
    active = maximum = 0

    async def probe(section, request):
        nonlocal active, maximum
        active += 1
        maximum = max(maximum, active)
        try:
            await asyncio.sleep(0.02)
            if section == "gpu:amd":
                raise RuntimeError("driver unavailable")
            return (
                {"devices": [{"vendor": section}]}
                if section.startswith("gpu:")
                else {"cpu": {"logical": 4}}
            )
        finally:
            active -= 1

    monkeypatch.setattr(tools, "_probe", probe)
    results = await asyncio.gather(tools.info(), tools.info())
    assert maximum == 4
    for result in results:
        assert result["cpu"]["logical"] == 4
        assert len(result["gpus"]) == 2
        assert result["sources"]["gpu:amd"]["status"] == "unavailable"


async def test_large_result_is_bounded_and_explicitly_truncated(tmp_path, monkeypatch):
    tools = api.SystemTools(tmp_path)

    async def probe(section, request):
        return {"processes": [{"pid": i, "cmdline": ["x" * 3000]} for i in range(200)]}

    monkeypatch.setattr(tools, "_probe", probe)
    result = await tools.processes()
    assert len(json.dumps(result, ensure_ascii=False).encode()) <= 32768
    assert result["truncated"]
    assert result["omitted"]["processes"]
    assert result["processes"][0]["pid"] == 0


def fake_worker(tmp_path, monkeypatch, code):
    guard = Path(api.__file__).with_name("process_guard.py")
    (tmp_path / "process_guard.py").symlink_to(guard)
    (tmp_path / "system_probe.py").write_text(code)
    monkeypatch.setattr(api, "__file__", str(tmp_path / "api.py"))
    return api.SystemTools(tmp_path)


async def test_real_helper_returns_valid_json(tmp_path, monkeypatch):
    tools = fake_worker(
        tmp_path,
        monkeypatch,
        "import json,sys\nr=json.load(sys.stdin)\n"
        "print(json.dumps({'ok':True,'data':{'pid':r['reference_pid']}}))\n",
    )
    result = await tools.disks()
    assert result["pid"] == os.getpid()
    assert result["sources"]["disks"]["status"] == "ok"


@pytest.mark.parametrize("cancel", [False, True])
async def test_hung_probe_and_detached_descendants_are_cleaned(tmp_path, monkeypatch, cancel):
    tools = fake_worker(
        tmp_path,
        monkeypatch,
        """
import json,os,signal,sys,time
json.load(sys.stdin)
if os.fork() == 0:
    os.setsid()
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    open('descendant.pid','w').write(str(os.getpid()))
    time.sleep(30)
else:
    time.sleep(30)
""",
    )
    task = asyncio.create_task(tools.disks(timeout=1))
    async with asyncio.timeout(3):
        while not await asyncio.to_thread((tmp_path / "descendant.pid").exists):  # noqa: ASYNC110
            await asyncio.sleep(0.01)
    pid = int((tmp_path / "descendant.pid").read_text())
    if cancel:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    else:
        result = await task
        assert result["sources"]["disks"]["status"] == "timeout"
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)


async def test_output_flood_is_stopped(tmp_path, monkeypatch):
    tools = fake_worker(
        tmp_path,
        monkeypatch,
        "import sys\nsys.stdin.read()\nwhile True: print('x' * 65536, flush=True)\n",
    )
    result = await tools.disks(timeout=2)
    assert result["sources"]["disks"]["status"] == "unavailable"
    assert "byte limit" in result["sources"]["disks"]["error"]


async def test_deadline_keeps_completed_sections(tmp_path, monkeypatch):
    tools = api.SystemTools(tmp_path)

    async def probe(section, request):
        if section == "info":
            return {"cpu": {"logical_cores": 12}}
        await asyncio.sleep(10)

    monkeypatch.setattr(tools, "_probe", probe)
    result = await tools.info(timeout=0.1)
    assert result["cpu"]["logical_cores"] == 12
    assert result["sources"]["info"]["status"] == "ok"
    assert all(result["sources"][name]["status"] == "timeout" for name in api._GPUS)


async def test_disk_path_uses_workspace(tmp_path, monkeypatch):
    tools = api.SystemTools(tmp_path)
    requests = []

    async def probe(section, request):
        requests.append(request)
        return {"disks": []}

    monkeypatch.setattr(tools, "_probe", probe)
    await tools.disks("subdir")
    assert requests[0]["path"] == str(tmp_path / "subdir")


async def test_section_availability_is_not_overwritten(tmp_path, monkeypatch):
    tools = api.SystemTools(tmp_path)

    async def probe(section, request):
        if section == "base_usage":
            return {
                "available": False,
                "warnings": ["CPU inaccessible"],
                "memory": {"used_bytes": 4},
            }
        return {"available": True, "devices": [], "disks": []}

    monkeypatch.setattr(tools, "_probe", probe)
    result = await tools.usage()
    assert "available" not in result
    assert result["memory"]["used_bytes"] == 4
    assert result["sources"]["base_usage"]["status"] == "unavailable"
    assert result["sources"]["disks"]["status"] == "ok"


def test_worker_bounds_large_results_before_writing_pipe(monkeypatch, capsys):
    import io
    import sys
    from types import SimpleNamespace

    from mypr_mcp import system_base, system_probe

    monkeypatch.setattr(
        sys, "stdin", SimpleNamespace(buffer=io.BytesIO(b'{"section":"processes"}'))
    )
    monkeypatch.setattr(
        system_base,
        "collect",
        lambda *_: {
            "warnings": [],
            "processes": [{"pid": i, "cmdline": ["x" * 4096] * 100} for i in range(20)],
        },
    )
    assert system_probe.main() == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"]
    assert payload["data"]["truncated"]
    assert len(json.dumps(payload["data"], ensure_ascii=False).encode()) <= 32768


async def test_intel_stream_finishes_within_outer_deadline(tmp_path, monkeypatch):
    import sys

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    device = tmp_path / "devices" / "0000:03:00.0"
    device.mkdir(parents=True)
    (device / "vendor").write_text("0x8086")
    card = tmp_path / "drm" / "card0"
    card.mkdir(parents=True)
    (card / "device").symlink_to(device)
    xpu = bin_dir / "xpu-smi"
    xpu.write_text(
        f"#!{sys.executable}\n"
        "import json,sys\n"
        "print(json.dumps({'device_list':[{'device_id':0,'device_name':'Intel Test',"
        "'pci_bdf':'0000:03:00.0'}]}))\n"
    )
    xpu.chmod(0o755)
    top = bin_dir / "intel_gpu_top"
    top.write_text(
        f"#!{sys.executable}\n"
        "import os,time\n"
        "open('intel-top.pid','w').write(str(os.getpid()))\n"
        'print(\'[ {"period":{"duration":100},"engines":{"Render/3D":{"busy":25}}},\',flush=True)\n'
        'print(\'{"engines":{"Render/3D":{"busy":99}}\',flush=True)\n'
        "time.sleep(60)\n"
    )
    top.chmod(0o755)
    monkeypatch.setenv("PATH", str(bin_dir))
    tools = fake_worker(
        tmp_path,
        monkeypatch,
        "from pathlib import Path\n"
        "from mypr_mcp import system_gpu,system_probe\n"
        f"system_gpu._SYSFS=Path({str(tmp_path / 'drm')!r})\n"
        "raise SystemExit(system_probe.main())\n",
    )
    async with asyncio.timeout(5):
        result = await tools.gpus(interval=0.1, timeout=2)
    assert result["sources"]["gpu:intel"]["status"] != "timeout", result
    intel = next(g for g in result["gpus"] if g["vendor"] == "intel")
    assert intel["metrics"]["intel_gpu_top"]["engines"]["Render/3D"]["busy_percent"] == 25
    with pytest.raises(ProcessLookupError):
        os.kill(int((tmp_path / "intel-top.pid").read_text()), 0)
