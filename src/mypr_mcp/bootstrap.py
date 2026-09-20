"""Keep workspace runtime dependencies aligned with the installed distribution."""

from __future__ import annotations

import asyncio
import importlib.metadata
import json
from pathlib import Path

RUNTIME_PACKAGES = ("ipykernel", "pyyaml", "httpx2", "playwright", "h2", "socksio", "psutil")
_PROBE = """
import importlib.metadata as m, json, sys
found = {}
for name in sys.argv[1:]:
    try:
        found[name] = m.version(name)
    except m.PackageNotFoundError:
        pass
print(json.dumps(found))
"""


async def runtime_versions(python: Path) -> dict[str, str]:
    process = await asyncio.create_subprocess_exec(
        str(python),
        "-I",
        "-c",
        _PROBE,
        *RUNTIME_PACKAGES,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        async with asyncio.timeout(15):
            output, error = await process.communicate()
    except BaseException:
        if process.returncode is None:
            process.kill()
        await process.wait()
        raise
    if process.returncode:
        raise RuntimeError(
            f"Unable to inspect workspace Python: {error.decode(errors='replace')[:512]}"
        )
    result = json.loads(output)
    if not isinstance(result, dict):
        raise RuntimeError("Invalid workspace dependency inventory")
    return result


async def ensure_runtime(python: Path, command) -> None:
    expected = {name: importlib.metadata.version(name) for name in RUNTIME_PACKAGES}
    current = await runtime_versions(python)
    if any(current.get(name) != version for name, version in expected.items()):
        await command(
            "uv",
            "pip",
            "install",
            "--python",
            str(python),
            *(f"{name}=={version}" for name, version in expected.items()),
        )
        actual = await runtime_versions(python)
        if any(actual.get(name) != version for name, version in expected.items()):
            raise RuntimeError("Workspace runtime dependencies did not synchronize")
    marker = python.parent.parent / ".mypr-runtime.json"
    marker.write_text(json.dumps(expected, sort_keys=True) + "\n")
