from __future__ import annotations

import json
import os
import shutil
import site
import subprocess
import sys
import venv
import zipfile
from pathlib import Path


def test_packaged_kernel_boot_keeps_workspace_dependencies_first(tmp_path: Path):
    manager_root = tmp_path / "manager-site-packages"
    package_root = manager_root / "mypr_mcp"
    shutil.copytree(Path(__file__).parents[1] / "src/mypr_mcp", package_root)
    metadata = manager_root / "mypr_mcp-test.dist-info"
    metadata.mkdir()
    (metadata / "METADATA").write_text("Metadata-Version: 2.3\nName: mypr-mcp\nVersion: 0.0.0\n")

    workspace_root = tmp_path / "workspace-site-packages"
    manager_ipython = manager_root / "IPython" / "core"
    manager_ipython.mkdir(parents=True)
    (manager_root / "IPython" / "__init__.py").write_text("origin = 'manager'\n")
    (manager_root / "IPython" / "core" / "__init__.py").write_text("")
    (manager_ipython / "interactiveshell.py").write_text(
        "class ExecutionInfo: pass\nclass ExecutionResult: pass\nclass InteractiveShell: pass\n"
    )
    ipython_root = workspace_root / "IPython" / "core"
    ipython_root.mkdir(parents=True)
    (workspace_root / "IPython" / "__init__.py").write_text("origin = 'workspace'\n")
    (workspace_root / "IPython" / "core" / "__init__.py").write_text("")
    (ipython_root / "interactiveshell.py").write_text(
        "class ExecutionInfo: pass\nclass ExecutionResult: pass\nclass InteractiveShell: pass\n"
    )

    boot = package_root / "kernel_boot.py"
    probe = (
        "import json, runpy, sys\n"
        f"runpy.run_path({str(boot)!r}, run_name='kernel_boot_probe')\n"
        "print(json.dumps({'mypr': sys.modules['mypr_mcp'].__file__, "
        "'ipython': sys.modules['IPython'].__file__}))\n"
    )
    env = dict(os.environ, PYTHONPATH=str(workspace_root))
    result = subprocess.run(
        [sys.executable, "-c", probe],
        cwd=tmp_path,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    loaded = json.loads(result.stdout)
    assert Path(loaded["mypr"]).resolve() == (package_root / "__init__.py").resolve()
    assert (
        Path(loaded["ipython"]).resolve() == (workspace_root / "IPython" / "__init__.py").resolve()
    )


def test_wheel_kernel_boot_uses_workspace_runtime(tmp_path: Path):
    repository = Path(__file__).parents[1]
    wheel_dir = tmp_path / "wheel"
    wheel_dir.mkdir()
    build_env = dict(os.environ, UV_CACHE_DIR="/tmp/mypr-uv-cache")
    subprocess.run(
        ["uv", "build", "--no-sources", "--wheel", "--out-dir", str(wheel_dir)],
        cwd=repository,
        env=build_env,
        check=True,
        capture_output=True,
        text=True,
    )
    wheels = list(wheel_dir.glob("*.whl"))
    assert len(wheels) == 1

    manager_root = tmp_path / "manager-site-packages"
    manager_root.mkdir()
    with zipfile.ZipFile(wheels[0]) as archive:
        archive.extractall(manager_root)

    workspace_venv = tmp_path / "workspace-venv"
    venv.EnvBuilder(with_pip=False).create(workspace_venv)
    workspace_python = workspace_venv / "bin/python"
    workspace_site = Path(
        subprocess.check_output(
            [str(workspace_python), "-c", "import site; print(site.getsitepackages()[0])"],
            text=True,
        ).strip()
    )
    source_site = Path(site.getsitepackages()[0])
    for dependency in source_site.iterdir():
        if "mypr_mcp" in dependency.name:
            continue
        (workspace_site / dependency.name).symlink_to(dependency)

    boot = manager_root / "mypr_mcp" / "kernel_boot.py"
    probe = (
        "import json, runpy, sys\n"
        f"namespace = runpy.run_path({str(boot)!r}, run_name='kernel_boot_probe')\n"
        f"workspace = namespace['create_workspace']({str(tmp_path)!r}, {{}})\n"
        "kernel_class = namespace['_kernel_class']()\n"
        "import IPython, ipykernel, yaml\n"
        "print(json.dumps({'mypr': sys.modules['mypr_mcp'].__file__, "
        "'version': sys.modules['mypr_mcp'].__version__, "
        "'workspace': workspace.inspect()['workspace'], "
        "'ipython': IPython.__file__, 'ipykernel': ipykernel.__file__, "
        "'yaml': yaml.__file__, 'kernel': kernel_class.__name__}))\n"
    )
    probe_env = dict(os.environ)
    probe_env.pop("PYTHONPATH", None)
    result = subprocess.run(
        [str(workspace_python), "-c", probe],
        cwd=tmp_path,
        env=probe_env,
        check=True,
        capture_output=True,
        text=True,
    )
    loaded = json.loads(result.stdout)
    assert Path(loaded["mypr"]).resolve() == (manager_root / "mypr_mcp" / "__init__.py").resolve()
    assert loaded["version"]
    assert loaded["workspace"] == str(tmp_path)
    assert Path(loaded["ipython"]).is_relative_to(workspace_site)
    assert Path(loaded["ipykernel"]).is_relative_to(workspace_site)
    assert Path(loaded["yaml"]).is_relative_to(workspace_site)
    assert loaded["kernel"] == "WorkspaceKernel"
