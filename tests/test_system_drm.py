from pathlib import Path

import pytest

from mypr_mcp import system_drm as drm


def process(root, pid, *, start=100):
    directory = root / str(pid)
    (directory / "fdinfo").mkdir(parents=True, exist_ok=True)
    (directory / "stat").write_text(f"{pid} (worker) S " + "0 " * 18 + str(start))
    (directory / "comm").write_text("worker\n")
    return directory


def fd(directory, number, *, client=7, counter=0, pci="0000:03:00.0"):
    (directory / "fdinfo" / str(number)).write_text(
        f"drm-driver: xe\ndrm-client-id: {client}\ndrm-pdev: {pci}\n"
        f"drm-engine-render: {counter} ns\ndrm-engine-capacity-render: 2\n"
        "drm-total-memory: 32 KiB\ndrm-resident-vram: 2 MiB\n"
    )


def test_shared_descriptors_are_counted_once_across_processes(tmp_path, monkeypatch):
    first, second = process(tmp_path, 10), process(tmp_path, 20)
    monkeypatch.setattr(drm, "_PROC", tmp_path)
    clock = [0.0]
    monkeypatch.setattr(drm.time, "monotonic", lambda: clock[0])

    def update(interval):
        clock[0] += 1
        for directory in (first, second):
            fd(directory, 3, counter=1_000_000_000)
            fd(directory, 4, counter=1_000_000_000)

    monkeypatch.setattr(drm.time, "sleep", update)
    for directory in (first, second):
        fd(directory, 3)
        fd(directory, 4)
    result = drm.sample(["00000000:03:00.0"], interval=0.5, reference_pid=10)
    device = result["0000:03:00.0"]
    assert len(device["clients"]) == 1
    client = device["clients"][0]
    assert client["owner_count"] == 2
    assert client["reference"]
    assert client["engines"]["render"] == {
        "busy_ns": 1_000_000_000,
        "capacity": 2,
        "percent": 50.0,
    }
    assert client["sample_seconds"] == 1
    assert client["memory_regions"]["memory"]["total_bytes"] == 32768
    assert client["memory_regions"]["vram"]["resident_bytes"] == 2 * 1024**2


@pytest.mark.parametrize("change", ["pid_reuse", "new_client", "counter_decrease"])
def test_missing_or_reused_baselines_are_not_reported_as_busy(tmp_path, monkeypatch, change):
    directory = process(tmp_path, 10)
    fd(directory, 3, counter=100)
    monkeypatch.setattr(drm, "_PROC", tmp_path)
    clock = [0.0]
    monkeypatch.setattr(drm.time, "monotonic", lambda: clock[0])

    def update(interval):
        clock[0] += 1
        if change == "pid_reuse":
            process(tmp_path, 10, start=200)
        fd(directory, 3, client=8 if change == "new_client" else 7, counter=50)

    monkeypatch.setattr(drm.time, "sleep", update)
    value = drm.sample(["03:00.0"], interval=0.5, reference_pid=10)
    engine = value["0000:03:00.0"]["clients"][0]["engines"]["render"]
    assert engine["percent"] == (0 if change == "counter_decrease" else None)


def test_unknown_pci_is_not_assigned_to_every_device(tmp_path, monkeypatch):
    directory = process(tmp_path, 10)
    fd(directory, 3, pci="unknown")
    monkeypatch.setattr(drm, "_PROC", tmp_path)
    monkeypatch.setattr(drm.time, "sleep", lambda _: None)
    result = drm.sample(["03:00.0", "04:00.0"], interval=0.1, reference_pid=10)
    assert all(not item["clients"] and item["warnings"] for item in result.values())


def test_client_limit_and_inaccessible_files_are_reported(tmp_path, monkeypatch):
    directory = process(tmp_path, 10)
    fd(directory, 3, client=3)
    fd(directory, 4, client=4)
    monkeypatch.setattr(drm, "_PROC", tmp_path)
    monkeypatch.setattr(drm.time, "sleep", lambda _: None)
    original = Path.iterdir

    def iterdir(path):
        if path == tmp_path / "20" / "fdinfo":
            raise PermissionError("denied")
        return original(path)

    process(tmp_path, 20)
    monkeypatch.setattr(Path, "iterdir", iterdir)
    result = drm.sample(["03:00.0"], interval=0.1, reference_pid=10, limit=1)
    device = result["0000:03:00.0"]
    assert len(device["clients"]) == 1
    assert device["truncated"]
    assert any("inaccessible" in warning for warning in device["warnings"])


def test_xe_cycle_counters_use_gpu_clock_and_engine_capacity(tmp_path, monkeypatch):
    directory = process(tmp_path, 10)
    monkeypatch.setattr(drm, "_PROC", tmp_path)

    def update(interval=0):
        fd(directory, 3)
        with (directory / "fdinfo" / "3").open("a") as stream:
            stream.write(f"drm-cycles-ccs: {100 + int(interval > 0) * 200}\n")
            stream.write(f"drm-total-cycles-ccs: {1000 + int(interval > 0) * 100}\n")
            stream.write("drm-engine-capacity-ccs: 4\n")

    update()
    monkeypatch.setattr(drm.time, "sleep", update)
    result = drm.sample(["03:00.0"], interval=0.1, reference_pid=10)
    engine = result["0000:03:00.0"]["clients"][0]["engines"]["ccs"]
    assert engine == {"capacity": 4, "percent": 50, "busy_cycles": 200, "total_cycles": 100}
