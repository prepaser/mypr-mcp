from __future__ import annotations

import fcntl
import os
import stat
import sys
import venv
from pathlib import Path

import pytest

from mypr_mcp import package_worker


def _uv_script(path: Path, seen: Path) -> None:
    path.write_text(
        "#!/bin/sh\n"
        f"printf '%s\\n' \"$*\" >> {seen!s}\n"
        "if [ \"$2\" = install ]; then\n"
        f"  for arg in \"$@\"; do case \"$arg\" in --constraint) next=1;; *) "
        f"if [ \"$next\" = 1 ]; then cp \"$arg\" {seen.parent / 'constraints.txt'!s}; "
        "next=0; fi;; esac; done\n"
        "  exit 0\n"
        "fi\n"
        "if [ \"$4\" = freeze ]; then printf 'pillow==1\\nrequests==2\\n'; exit 0; fi\n"
        "exit 0\n",
        encoding="utf-8",
    )
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


def _state(*, pillow=False, requests=True, request_import=True):
    distributions = {}
    if pillow:
        distributions["pillow"] = {"name": "Pillow", "version": "1"}
    if requests:
        distributions["requests"] = {"name": "requests", "version": "2"}
    return {
        "distributions": distributions,
        "imports": {
            "PIL": {"ok": pillow and request_import},
        },
    }


def test_automatic_noop_does_not_invoke_uv_or_touch_manifest(tmp_path, monkeypatch):
    root = tmp_path / ".mypr"
    root.mkdir()
    manifest = root / "requirements.txt"
    manifest.write_text("existing==1\n")
    uv = tmp_path / "uv"
    uv.write_text("#!/bin/sh\nexit 99\n")
    uv.chmod(uv.stat().st_mode | stat.S_IXUSR)
    monkeypatch.setattr(
        package_worker,
        "_probe_environment",
        lambda *_args: _state(pillow=True),
    )

    result = package_worker.install_packages(
        sys.executable, root, ["pillow"], uv=str(uv), automatic=True
    )

    assert result["specs"] == []
    assert result["already_satisfied"] == ["pillow"]
    assert result["durability"] == "unchanged"
    assert manifest.read_text() == "existing==1\n"


def test_probe_suppresses_registered_module_import_output(tmp_path):
    environment = tmp_path / "venv"
    venv.EnvBuilder(with_pip=False, symlinks=True).create(environment)
    sites = list((environment / "lib").glob("python*/site-packages"))
    assert len(sites) == 1
    (sites[0] / "tomlkit.py").write_text("print('package import banner')\n")
    metadata = sites[0] / "tomlkit-0.15.0.dist-info"
    metadata.mkdir()
    (metadata / "METADATA").write_text(
        "Metadata-Version: 2.1\nName: tomlkit\nVersion: 0.15.0\n"
    )

    result = package_worker._probe_environment(
        environment / "bin" / "python", ["tomlkit"]
    )

    assert result["imports"]["tomlkit"] == {"ok": True}


def test_probe_suppresses_fd_level_registered_module_output(tmp_path):
    environment = tmp_path / "venv"
    venv.EnvBuilder(with_pip=False, symlinks=True).create(environment)
    sites = list((environment / "lib").glob("python*/site-packages"))
    assert len(sites) == 1
    (sites[0] / "tomlkit.py").write_text(
        "import os\nos.write(1, b'package native banner\\n')\n"
    )
    metadata = sites[0] / "tomlkit-0.15.0.dist-info"
    metadata.mkdir()
    (metadata / "METADATA").write_text(
        "Metadata-Version: 2.1\nName: tomlkit\nVersion: 0.15.0\n"
    )

    result = package_worker._probe_environment(
        environment / "bin" / "python", ["tomlkit"]
    )

    assert result["imports"]["tomlkit"] == {"ok": True}


def test_automatic_install_pins_existing_distributions(tmp_path, monkeypatch):
    root = tmp_path / ".mypr"
    root.mkdir()
    seen = tmp_path / "uv.log"
    uv = tmp_path / "uv"
    _uv_script(uv, seen)
    states = iter([_state(pillow=False), _state(pillow=True)])
    monkeypatch.setattr(package_worker, "_probe_environment", lambda *_args: next(states))

    result = package_worker.install_packages(
        sys.executable, root, ["pillow"], uv=str(uv), automatic=True
    )

    assert result["specs"] == ["pillow"]
    assert result["already_satisfied"] == []
    assert (tmp_path / "constraints.txt").read_text() == "requests==2\n"
    assert "--constraint" in seen.read_text()


def test_automatic_repairs_unusable_core_without_upgrading_others(tmp_path, monkeypatch):
    root = tmp_path / ".mypr"
    root.mkdir()
    uv = tmp_path / "uv"
    uv.write_text("")
    uv.chmod(uv.stat().st_mode | stat.S_IXUSR)
    calls = []
    states = iter(
        [
            {
                "distributions": {
                    "ipykernel": {"name": "ipykernel", "version": "7.2.0"},
                    "requests": {"name": "requests", "version": "2"},
                },
                "imports": {
                    "ipykernel": {"ok": False, "error": "broken import"},
                },
            },
            {
                "distributions": {
                    "ipykernel": {"name": "ipykernel", "version": "7.3.0"},
                    "requests": {"name": "requests", "version": "2"},
                },
                "imports": {"ipykernel": {"ok": True}},
            },
        ]
    )

    def probe(*_args):
        return next(states)

    def run(command, phase, **_kwargs):
        calls.append((command, phase))
        if phase == "install":
            constraint = Path(command[command.index("--constraint") + 1])
            assert "ipykernel==7.2.0" not in constraint.read_text()
            assert "requests==2" in constraint.read_text()
        return b""

    monkeypatch.setattr(package_worker, "_probe_environment", probe)
    monkeypatch.setattr(package_worker, "_run", run)
    result = package_worker.install_packages(
        sys.executable, root, ["ipykernel"], uv=str(uv), automatic=True
    )

    assert result["specs"] == ["ipykernel"]
    assert calls[0][1] == "install"
    assert calls[0][0][-3:] == ["--reinstall-package", "ipykernel", "ipykernel>=7.3,<8"]


def test_automatic_rechecks_under_workspace_lock(tmp_path, monkeypatch):
    root = tmp_path / ".mypr"
    root.mkdir()
    uv = tmp_path / "uv"
    uv.write_text("#!/bin/sh\nif [ \"$4\" = freeze ]; then exit 0; fi\nexit 0\n")
    uv.chmod(uv.stat().st_mode | stat.S_IXUSR)
    observed = []

    def probe(_python, _modules):
        fd = os.open(root / "packages.lock", os.O_RDONLY)
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                observed.append(True)
            else:
                observed.append(False)
                fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)
        return _state(pillow=True)

    monkeypatch.setattr(package_worker, "_probe_environment", probe)
    package_worker.install_packages(
        sys.executable, root, ["pillow"], uv=str(uv), automatic=True
    )
    assert observed == [True]


def test_automatic_broken_import_is_not_reinstalled(tmp_path, monkeypatch):
    root = tmp_path / ".mypr"
    root.mkdir()
    uv = tmp_path / "uv"
    uv.write_text("#!/bin/sh\nexit 99\n")
    uv.chmod(uv.stat().st_mode | stat.S_IXUSR)
    monkeypatch.setattr(
        package_worker,
        "_probe_environment",
        lambda *_args: _state(pillow=True, request_import=False),
    )

    with pytest.raises(RuntimeError, match="installed but unusable"):
        package_worker.install_packages(
            sys.executable, root, ["pillow"], uv=str(uv), automatic=True
        )


def test_automatic_install_rejects_existing_version_changes(tmp_path, monkeypatch):
    root = tmp_path / ".mypr"
    root.mkdir()
    manifest = root / "requirements.txt"
    manifest.write_text("old==1\n")
    uv = tmp_path / "uv"
    _uv_script(uv, tmp_path / "uv.log")
    before = _state(pillow=False)
    after = _state(pillow=True)
    after["distributions"]["requests"]["version"] = "3"
    states = iter([before, after])
    monkeypatch.setattr(package_worker, "_probe_environment", lambda *_args: next(states))

    with pytest.raises(RuntimeError, match="changed existing distributions"):
        package_worker.install_packages(
            sys.executable, root, ["pillow"], uv=str(uv), automatic=True
        )
    assert manifest.read_text() == "old==1\n"


@pytest.mark.parametrize("timeout", [float("nan"), float("inf"), float("-inf")])
def test_package_lock_rejects_nonfinite_timeout(timeout):
    with pytest.raises(ValueError, match="finite positive"):
        package_worker._lock(None, timeout)


@pytest.mark.parametrize("spec", ["pillow>=1", "requests", "https://example.invalid/pkg"])
def test_automatic_install_accepts_only_registered_plain_packages(tmp_path, spec):
    root = tmp_path / ".mypr"
    root.mkdir()
    with pytest.raises(ValueError, match="supported plain package names"):
        package_worker.install_packages(sys.executable, root, [spec], automatic=True)
