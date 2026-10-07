"""Built-in Python dependency metadata.

This module intentionally uses only the Python standard library.  The package
worker imports it from an isolated interpreter before the workspace environment
has been populated.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType

PYTHON_MINIMUM = (3, 14)


@dataclass(frozen=True, slots=True)
class PythonPackage:
    name: str
    module: str
    requirement: str
    core: bool = False


_CATALOG = (
    PythonPackage("ipykernel", "ipykernel", "ipykernel>=7.3,<8", True),
    PythonPackage("tomlkit", "tomlkit", "tomlkit>=0.15,<1", True),
    PythonPackage("pyyaml", "yaml", "PyYAML>=6,<7"),
    PythonPackage("httpx2", "httpx2", "httpx2>=2.12,<3"),
    PythonPackage("h2", "h2", "h2"),
    PythonPackage("socksio", "socksio", "socksio"),
    PythonPackage("playwright", "playwright", "playwright>=1.58,<2"),
    PythonPackage("psutil", "psutil", "psutil>=7.2,<8"),
    PythonPackage("pillow", "PIL", "Pillow"),
    PythonPackage("pymupdf", "pymupdf", "PyMuPDF"),
    PythonPackage("trafilatura", "trafilatura", "trafilatura"),
    PythonPackage("cssselect", "cssselect", "cssselect"),
    PythonPackage("python-docx", "docx", "python-docx"),
    PythonPackage("python-pptx", "pptx", "python-pptx"),
    PythonPackage("openpyxl", "openpyxl", "openpyxl"),
)

PYTHON_PACKAGE_CATALOG = _CATALOG
PYTHON_PACKAGES = MappingProxyType({item.name: item.module for item in _CATALOG})
PYTHON_PACKAGE_REQUIREMENTS = MappingProxyType({item.name: item.requirement for item in _CATALOG})
PYTHON_PACKAGE_METADATA = MappingProxyType({item.name: item for item in _CATALOG})
CORE_PACKAGES = tuple(item.name for item in _CATALOG if item.core)


def python_version_satisfies(version: str | None) -> bool:
    if not isinstance(version, str):
        return False
    parts = version.split(".")
    if len(parts) < 2:
        return False
    try:
        major, minor = int(parts[0]), int(parts[1])
    except ValueError:
        return False
    return (major, minor) >= PYTHON_MINIMUM


def version_satisfies(version: str | None, requirement: str | None) -> bool:
    """Validate installed versions in the manager or installer environment."""

    if not isinstance(version, str) or not version.strip():
        return False
    if not isinstance(requirement, str) or not requirement.strip():
        return True
    from packaging.requirements import InvalidRequirement, Requirement
    from packaging.version import InvalidVersion, Version

    try:
        return Requirement(requirement).specifier.contains(Version(version))
    except InvalidRequirement, InvalidVersion:
        return False


def package_environment(
    config: Mapping[str, object] | None = None,
    environ: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Return the installer environment with explicit env precedence."""

    result = dict(os.environ if environ is None else environ)
    values = config or {}
    if isinstance(values.get("dependencies"), Mapping):
        values = values["dependencies"]
    for option, variable in (("uv_cache_dir", "UV_CACHE_DIR"), ("uv_link_mode", "UV_LINK_MODE")):
        value = values.get(option)
        if variable not in result and isinstance(value, str) and value:
            result[variable] = str(Path(value).expanduser()) if option == "uv_cache_dir" else value
    return result


def uv_diagnostics(
    config: Mapping[str, object] | None = None,
    workspace: str | os.PathLike[str] | None = None,
    environ: Mapping[str, str] | None = None,
) -> dict[str, object]:
    """Describe uv's effective cache and linking context without creating it."""

    provided = dict(os.environ if environ is None else environ)
    environment = package_environment(config, provided)
    values = config or {}
    if isinstance(values.get("dependencies"), Mapping):
        values = values["dependencies"]
    configured_link = values.get("uv_link_mode")
    raw_cache = environment.get("UV_CACHE_DIR")
    if raw_cache:
        cache_path = Path(raw_cache).expanduser()
        cache_source = "environment" if "UV_CACHE_DIR" in provided else "config"
    else:
        xdg_cache = environment.get("XDG_CACHE_HOME")
        cache_base = Path(xdg_cache).expanduser() if xdg_cache else Path.home() / ".cache"
        cache_path = cache_base / "uv"
        cache_source = "uv-default"
    if not cache_path.is_absolute():
        base = Path.cwd() if workspace is None else Path(workspace)
        cache_path = base / cache_path
    cache_path = cache_path.resolve()
    workspace_path = Path.cwd() if workspace is None else Path(workspace).expanduser().resolve()

    def device(path: Path) -> int | None:
        candidate = path
        while True:
            try:
                return os.stat(candidate).st_dev
            except OSError:
                if candidate == candidate.parent:
                    return None
                candidate = candidate.parent

    cache_device = device(cache_path)
    workspace_device = device(workspace_path)
    link_mode = environment.get("UV_LINK_MODE") or "uv-default"
    return {
        "cache_dir": str(cache_path),
        "cache_exists": cache_path.is_dir(),
        "cache_source": cache_source,
        "link_mode": link_mode,
        "link_source": (
            "environment"
            if "UV_LINK_MODE" in provided
            else "config"
            if configured_link
            else "uv-default"
        ),
        "same_filesystem": (
            None
            if cache_device is None or workspace_device is None
            else cache_device == workspace_device
        ),
        "workspace": str(workspace_path),
    }


def package_metadata(name: str) -> PythonPackage:
    try:
        return PYTHON_PACKAGE_METADATA[name]
    except KeyError as exc:
        raise KeyError(f"unknown built-in Python package: {name}") from exc


__all__ = [
    "CORE_PACKAGES",
    "PYTHON_MINIMUM",
    "PYTHON_PACKAGE_CATALOG",
    "PYTHON_PACKAGE_METADATA",
    "PYTHON_PACKAGE_REQUIREMENTS",
    "PYTHON_PACKAGES",
    "PythonPackage",
    "package_environment",
    "package_metadata",
    "python_version_satisfies",
    "uv_diagnostics",
    "version_satisfies",
]
