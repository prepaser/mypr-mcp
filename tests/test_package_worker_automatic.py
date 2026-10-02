from __future__ import annotations

import fcntl
import os
import stat
import sys
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


@pytest.mark.parametrize("spec", ["pillow>=1", "requests", "https://example.invalid/pkg"])
def test_automatic_install_accepts_only_registered_plain_packages(tmp_path, spec):
    root = tmp_path / ".mypr"
    root.mkdir()
    with pytest.raises(ValueError, match="supported plain package names"):
        package_worker.install_packages(sys.executable, root, [spec], automatic=True)
