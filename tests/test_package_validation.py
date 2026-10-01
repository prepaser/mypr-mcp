import asyncio
from types import SimpleNamespace

import pytest

from mypr_mcp.package_worker import _validate_specs
from mypr_mcp.runtime import Runtime


@pytest.mark.parametrize("specs", [[" --help"], ["\t--target=/tmp/install"], [" \n"], [None]])
def test_package_specs_reject_invalid_values_after_normalizing(specs):
    with pytest.raises(ValueError):
        _validate_specs(specs)


def test_package_specs_preserve_requirements_and_paths(tmp_path):
    assert _validate_specs([" pkg[extra]>=1 ", " other; python_version >= '3.14' "]) == [
        "pkg[extra]>=1", "other; python_version >= '3.14'"
    ]
    assert _validate_specs([f" {tmp_path} "]) == [str(tmp_path)]


async def test_package_option_is_rejected_before_starting_a_worker(tmp_path):
    class NoCommands:
        async def start(self, *args, **kwargs):
            pytest.fail("invalid requirement must not launch a package worker")

    runtime = Runtime.__new__(Runtime)
    runtime.clients = {}
    runtime.stopping = asyncio.Event()
    runtime.restarting = None
    runtime.resetting = False
    runtime.workspace = tmp_path
    runtime.root = tmp_path / ".mypr"
    runtime.py = tmp_path / "python"
    runtime.generation = "g"
    runtime.shells = NoCommands()
    runtime._admission_lock = asyncio.Lock()
    runtime.healthy = True
    runtime.history = SimpleNamespace()
    with pytest.raises(ValueError, match="options"):
        await runtime._dispatch({"op": "packages_add", "specs": [" --help"]})
