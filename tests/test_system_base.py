from __future__ import annotations

from collections import namedtuple
from types import SimpleNamespace

import pytest

from mypr_mcp import system_base, system_limits

Stat = namedtuple("Stat", "user nice system idle iowait")
Mem = namedtuple("Mem", "total available used free percent")
Swap = namedtuple("Swap", "total used free percent")
Net = namedtuple(
    "Net", "bytes_sent bytes_recv packets_sent packets_recv errin errout dropin dropout"
)
DiskIO = namedtuple("DiskIO", "read_bytes write_bytes")
Freq = namedtuple("Freq", "current min max")
Usage = namedtuple("Usage", "total used free percent")
Partition = namedtuple("Partition", "device mountpoint fstype opts")


class FakeProcess:
    pid = 42

    def create_time(self):
        return 10.0

    def cpu_times(self):
        return Stat(1, 0, 1, 1, 0)

    def memory_info(self):
        return namedtuple("Info", "rss")(100)

    def io_counters(self):
        return namedtuple("IO", "read_bytes write_bytes")(20, 30)

    def name(self):
        return "worker"

    def username(self):
        return "tester"

    def status(self):
        return "running"

    def cmdline(self):
        return ["worker", "--test"]


class FakePsutil:
    def __init__(self):
        self.cpu = [Stat(1, 0, 1, 8, 0), Stat(2, 0, 2, 13, 1)]
        self.net = [
            {"eth0": Net(10, 20, 1, 2, 0, 0, 0, 0)},
            {"eth0": Net(30, 60, 1, 2, 0, 0, 0, 0)},
        ]
        self.disk = [
            {"sda": DiskIO(100, 200)},
            {"sda": DiskIO(200, 500)},
        ]

    def cpu_times(self, *, percpu=False):
        return self.cpu.pop(0)

    def cpu_count(self, logical=True):
        return 8 if logical else 4

    def cpu_freq(self):
        return Freq(3000, 1000, 4000)

    def virtual_memory(self):
        return Mem(1000, 700, 300, 700, 30)

    def swap_memory(self):
        return Swap(500, 50, 450, 10)

    def net_io_counters(self, *, pernic=True):
        return self.net.pop(0)

    def disk_io_counters(self, *, perdisk=True):
        return self.disk.pop(0)

    def process_iter(self):
        return [FakeProcess()]

    def disk_partitions(self, *, all=False):
        return [Partition("/dev/sda1", "/", "ext4", "rw")]

    def disk_usage(self, path):
        return Usage(1000, 400, 600, 40)


@pytest.fixture
def fake(monkeypatch, tmp_path):
    value = FakePsutil()
    monkeypatch.setattr(system_base, "_psutil", lambda: value)
    monkeypatch.setattr(system_base, "collect_limits", lambda pid: {"pid": pid})
    monkeypatch.setattr(system_base, "_PROC", tmp_path / "proc")
    monkeypatch.setattr(system_base, "_SYSFS", tmp_path / "sys")
    (tmp_path / "proc").mkdir()
    (tmp_path / "proc" / "cpuinfo").write_text("model name\t: Test CPU\n")
    block = tmp_path / "sys" / "block" / "vda"
    (block / "device").mkdir(parents=True)
    (block / "device" / "model").write_text("Test Disk\n")
    (block / "size").write_text("2048\n")
    (block / "queue").mkdir()
    (block / "queue" / "rotational").write_text("0\n")
    return value


def test_info_is_json_compatible_and_includes_physical_storage(fake):
    value = system_base.collect("info", {"reference_pid": 42})

    assert value["cpu"]["model"] == "Test CPU"
    assert value["storage"] == [
        {"name": "vda", "size_bytes": 1024 * 1024, "model": "Test Disk", "rotational": False}
    ]


def test_base_usage_uses_independent_counter_deltas(fake, monkeypatch):
    clock = iter((100.0, 100.5))
    monkeypatch.setattr(system_base.time, "monotonic", lambda: next(clock))
    monkeypatch.setattr(system_base.time, "sleep", lambda interval: None)

    value = system_base.collect("base_usage", {"interval": 0.5, "reference_pid": 42})

    assert value["sample_seconds"] == pytest.approx(0.5)
    assert value["network"]["eth0"]["bytes_sent_per_sec"] == pytest.approx(40)
    assert value["disk_io"]["sda"]["write_bytes_per_sec"] == pytest.approx(600)
    assert value["limits"]["pid"] == 42


def test_processes_report_new_baselines_as_unknown(fake, monkeypatch):
    monkeypatch.setattr(system_base.time, "sleep", lambda interval: None)
    value = system_base.collect("processes", {"interval": 0.1, "limit": 1, "cmdline": True})

    row = value["processes"][0]
    assert row["pid"] == 42
    assert row["cpu_percent"] == pytest.approx(0)
    assert row["cmdline"] == ["worker", "--test"]
    assert value["total"] == 1
    assert value["truncated"] is False


def test_disks_resolve_relative_path_against_workspace(fake, tmp_path):
    value = system_base.collect(
        "disks", {"workspace": str(tmp_path), "path": "data", "reference_pid": 42}
    )

    assert value["disks"][0]["mountpoint"] == str((tmp_path / "data").resolve())
    assert value["disks"][0]["free_bytes"] == 600


def test_interval_is_bounded():
    with pytest.raises(ValueError, match="between 0.1 and 10"):
        system_base.collect("base_usage", {"interval": 0.01})


def test_cpu_guest_fields_are_not_counted_twice():
    cpu = namedtuple("CPU", "user nice system idle guest guest_nice")
    value = system_base._cpu_usage(cpu(10, 0, 1, 7, 2, 0), cpu(20, 0, 2, 14, 4, 0), 1)

    assert value["total_seconds"] == pytest.approx(18)
    assert "guest" not in value["fields"]


def test_process_sample_uses_measured_elapsed(fake, monkeypatch):
    class Process(FakeProcess):
        calls = 0

        def cpu_times(self):
            self.calls += 1
            return Stat(self.calls * 0.25, 0, 1, 1, 0)

    process = Process()
    fake.process_iter = lambda: [process]
    clock = iter((10.0, 10.1, 10.35, 10.5))
    monkeypatch.setattr(system_base.time, "monotonic", lambda: next(clock))
    monkeypatch.setattr(system_base.time, "sleep", lambda interval: None)

    value = system_base.collect("processes", {"interval": 0.5, "limit": 1})

    assert value["sample_seconds"] == pytest.approx(0.5)
    assert value["processes"][0]["cpu_percent"] == pytest.approx(100)


def test_pid_reuse_invalidates_process_delta(fake, monkeypatch):
    class Process(FakeProcess):
        def __init__(self):
            self.generation = 0

        def create_time(self):
            self.generation += 1
            return float(self.generation)

    process = Process()
    calls = 0

    def process_iter():
        nonlocal calls
        calls += 1
        return [process]

    cleared = 0

    def clear_cache():
        nonlocal cleared
        cleared += 1

    process_iter.cache_clear = clear_cache
    fake.process_iter = process_iter
    monkeypatch.setattr(system_base.time, "sleep", lambda interval: None)

    value = system_base.collect("processes", {"interval": 0.1, "limit": 1})

    assert calls == 2
    assert cleared == 2
    assert value["processes"][0]["cpu_percent"] is None


def test_access_denied_io_keeps_process(fake, monkeypatch):
    class Process(FakeProcess):
        def io_counters(self):
            raise PermissionError("denied")

    fake.process_iter = lambda: [Process()]
    monkeypatch.setattr(system_base.time, "sleep", lambda interval: None)

    value = system_base.collect("processes", {"interval": 0.1, "limit": 1})

    assert value["processes"][0]["rss_bytes"] == 100
    assert value["processes"][0]["read_bytes"] is None
    assert any("I/O unavailable" in warning for warning in value["warnings"])


def test_counter_reset_has_no_negative_rate(fake, monkeypatch):
    fake.net = [
        {"eth0": Net(100, 100, 1, 1, 0, 0, 0, 0)},
        {"eth0": Net(50, 80, 1, 1, 0, 0, 0, 0)},
    ]
    clock = iter((100.0, 100.5))
    monkeypatch.setattr(system_base.time, "monotonic", lambda: next(clock))
    monkeypatch.setattr(system_base.time, "sleep", lambda interval: None)

    value = system_base.collect("base_usage", {"interval": 0.5})

    assert value["network"]["eth0"]["bytes_sent_per_sec"] is None
    assert value["network"]["eth0"]["bytes_recv_per_sec"] is None


def test_cgroup_fixture_maps_ancestors_and_preserves_leaf_cpuset(tmp_path, monkeypatch):
    proc = tmp_path / "proc"
    mount = tmp_path / "cgroup"
    (proc / "self").mkdir(parents=True)
    (proc / "42").mkdir()
    (mount / "slice" / "leaf").mkdir(parents=True)
    (mount / "slice").mkdir(exist_ok=True)
    (proc / "self" / "mountinfo").write_text(f"31 24 0:29 / {mount} rw - cgroup2 cgroup rw\n")
    (proc / "42" / "cgroup").write_text("0::/slice/leaf\n")
    leaf = mount / "slice" / "leaf"
    parent = mount / "slice"
    for directory, quota, memory, current, cpuset in (
        (leaf, "50000 100000", "1000", "100", "2-3"),
        (parent, "max 100000", "2000", "500", "0-3"),
    ):
        (directory / "cpu.max").write_text(quota)
        (directory / "memory.max").write_text(memory)
        (directory / "memory.current").write_text(current)
        (directory / "cpuset.cpus.effective").write_text(cpuset)

    class Process:
        def cpu_affinity(self):
            return [2, 3]

    fake_psutil = SimpleNamespace(Process=lambda pid: Process())
    monkeypatch.setitem(__import__("sys").modules, "psutil", fake_psutil)
    value = system_limits.collect_limits(42, proc_root=proc)

    assert value["cpu_quota_cpus"] == pytest.approx(0.5)
    assert value["memory_headroom_bytes"] == 900
    assert value["cpuset"] == "2-3"
    assert value["cgroup"]["ancestors"][0].endswith("slice/leaf")


def test_cgroup_root_without_controllers_is_known_unlimited(tmp_path, monkeypatch):
    proc = tmp_path / "proc"
    mount = tmp_path / "cgroup"
    (proc / "self").mkdir(parents=True)
    (proc / "42").mkdir()
    mount.mkdir()
    (proc / "self" / "mountinfo").write_text(f"31 24 0:29 / {mount} rw - cgroup2 cgroup rw\n")
    (proc / "42" / "cgroup").write_text("2:memory:/legacy\n0::/\n")
    fake_psutil = SimpleNamespace(Process=lambda pid: SimpleNamespace(cpu_affinity=lambda: [0]))
    monkeypatch.setitem(__import__("sys").modules, "psutil", fake_psutil)

    value = system_limits.collect_limits(42, proc_root=proc)

    assert value["cpu_quota_known"] is True
    assert value["memory_limit_known"] is True
    assert value["cpu_quota_cpus"] is None
    assert value["memory_limit_bytes"] is None


def test_cgroup_invalid_or_inaccessible_ancestor_is_unknown(tmp_path, monkeypatch):
    proc = tmp_path / "proc"
    mount = tmp_path / "cgroup"
    child = mount / "child"
    (proc / "self").mkdir(parents=True)
    (proc / "42").mkdir()
    child.mkdir(parents=True)
    (proc / "self" / "mountinfo").write_text(f"31 24 0:29 / {mount} rw - cgroup2 cgroup rw\n")
    (proc / "42" / "cgroup").write_text("0::/child\n")
    (child / "cpu.max").write_text("invalid 100000")
    (child / "memory.max").write_text("invalid")
    fake_psutil = SimpleNamespace(Process=lambda pid: SimpleNamespace(cpu_affinity=lambda: [0]))
    monkeypatch.setitem(__import__("sys").modules, "psutil", fake_psutil)

    value = system_limits.collect_limits(42, proc_root=proc)

    assert value["cpu_quota_known"] is False
    assert value["memory_limit_known"] is False
    assert any("unavailable" in warning for warning in value["warnings"])


def test_missing_frequency_keeps_cpu_identity_and_memory(fake):
    def unavailable():
        raise OSError("no cpufreq driver")

    fake.cpu_freq = unavailable
    result = system_base.collect("info")
    assert result["cpu"]["model"] == "Test CPU"
    assert result["cpu"]["logical_cores"] == 8
    assert result["cpu"]["frequency_mhz"] is None
    assert result["memory"]["total_bytes"] == 1000
    assert result["warnings"]
