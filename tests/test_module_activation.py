from __future__ import annotations

import hashlib
import sys
from pathlib import Path

import pytest

from mypr_mcp.filesystem import Filesystem
from mypr_mcp.modules import ModuleManager


@pytest.fixture
def module_namespace():
    original_path = list(sys.path)

    def clear():
        for name in list(sys.modules):
            if name == "ws_lib" or name.startswith("ws_lib."):
                sys.modules.pop(name, None)

    clear()
    try:
        yield
    finally:
        clear()
        sys.path[:] = original_path


def manager(tmp_path: Path) -> ModuleManager:
    root = tmp_path / ".mypr" / "lib" / "ws_lib"
    root.mkdir(parents=True)
    (root / "__init__.py").write_text("", encoding="utf-8")
    return ModuleManager(tmp_path, Filesystem(tmp_path), object())


def test_cold_nested_load_executes_imported_child_once(tmp_path, module_namespace):
    modules = manager(tmp_path)
    package = modules.root / "pkg"
    package.mkdir()
    marker = tmp_path / "worker-runs"
    (package / "__init__.py").write_text(
        "from . import worker\nEXPORTED = worker.VALUE\n", encoding="utf-8"
    )
    (package / "sibling.py").write_text("VALUE = 41\n", encoding="utf-8")
    (package / "worker.py").write_text(
        f"from pathlib import Path\n"
        f"marker = Path({str(marker)!r})\n"
        "marker.write_text(marker.read_text() + 'x' if marker.exists() else 'x')\n"
        "from .sibling import VALUE as SIBLING\n"
        "VALUE = SIBLING + 1\n",
        encoding="utf-8",
    )

    loaded = modules.load("pkg.worker")

    assert loaded.VALUE == 42
    assert marker.read_text() == "x"
    assert sys.modules["ws_lib.pkg"].worker is loaded
    assert sys.modules["ws_lib.pkg"].EXPORTED == 42


def test_reload_replaces_module_and_failure_restores_old_binding(tmp_path, module_namespace):
    modules = manager(tmp_path)
    path = modules.root / "demo.py"
    path.write_text("VALUE = 1\n", encoding="utf-8")

    old = modules.load("demo")
    path.write_text("VALUE = 2\n", encoding="utf-8")
    fresh = modules.reload("demo")

    assert fresh is not old
    assert old.VALUE == 1
    assert fresh.VALUE == 2
    assert sys.modules["ws_lib.demo"] is fresh
    assert sys.modules["ws_lib"].demo is fresh

    path.write_text("raise RuntimeError('broken')\n", encoding="utf-8")
    with pytest.raises(RuntimeError, match="broken"):
        modules.reload("demo")
    assert sys.modules["ws_lib.demo"] is fresh
    assert sys.modules["ws_lib"].demo is fresh


def test_nested_parent_import_uses_checked_source_after_disk_change(
    tmp_path, module_namespace
):
    modules = manager(tmp_path)
    package = modules.root / "pkg"
    package.mkdir()
    worker = package / "worker.py"
    checked = "VALUE = 'checked'\n"
    worker.write_text(checked, encoding="utf-8")
    (package / "__init__.py").write_text(
        "from pathlib import Path\n"
        f"Path({str(worker)!r}).write_text(\"VALUE = 'edited'\\n\")\n"
        "from . import worker\n",
        encoding="utf-8",
    )

    loaded = modules.load(
        "pkg.worker", expected_hash=hashlib.sha256(checked.encode()).hexdigest()
    )

    assert loaded.VALUE == "checked"
    assert worker.read_text(encoding="utf-8") == "VALUE = 'edited'\n"


def test_failed_parent_import_cleans_nested_modules(tmp_path, module_namespace):
    modules = manager(tmp_path)
    package = modules.root / "pkg"
    package.mkdir()
    (package / "worker.py").write_text("VALUE = 1\n", encoding="utf-8")
    (package / "__init__.py").write_text(
        "from . import worker\nraise RuntimeError('parent failed')\n", encoding="utf-8"
    )

    with pytest.raises(RuntimeError, match="parent failed"):
        modules.load("pkg.worker")
    assert "ws_lib.pkg" not in sys.modules
    assert "ws_lib.pkg.worker" not in sys.modules
