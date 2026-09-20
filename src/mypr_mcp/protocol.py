"""Manager protocol compatibility is independent of package releases."""

import sys
from pathlib import Path

from packaging.version import InvalidVersion, Version

from . import __version__

PROTOCOL_VERSION = 1
LEGACY_CAPABILITIES = (
    "execute",
    "poll",
    "messages",
    "files",
    "git",
    "http",
    "browser",
    "network",
    "skills",
    "modules",
    "locks",
)
CAPABILITIES = [*LEGACY_CAPABILITIES, "restart", "system"]
LEGACY_INSTRUCTIONS = """This workspace uses the legacy 0.9.0 runtime.
Use execute for Python and poll for submitted executions. Store client-local values in
ws.local. ws.status(), ws.reset(), ws.fs, ws.git, ws.http, ws.browser, ws.net, ws.tasks,
ws.shell, ws.mcp, ws.skills, ws.modules, ws.locks, and ws.messages are available.
ws.restart() is unavailable. To upgrade this runtime, run mypr-mcp restart from the
workspace using the new installation. Restart clears Python memory for every client.
"""


def descriptor():
    from .instructions import INSTRUCTIONS

    return {
        "version": __version__,
        "protocol_version": PROTOCOL_VERSION,
        "capabilities": list(CAPABILITIES),
        "instructions": INSTRUCTIONS,
    }


def check_compatibility(state):
    protocol = state.get("protocol_version")
    if protocol is None and state.get("version") == "0.9.0":
        return {
            **state,
            "protocol_version": 0,
            "capabilities": list(LEGACY_CAPABILITIES),
            "instructions": LEGACY_INSTRUCTIONS,
            "legacy": True,
        }
    if type(protocol) is not int or protocol != PROTOCOL_VERSION:
        raise RuntimeError(
            f"Incompatible workspace protocol {protocol!r} "
            f"(manager {state.get('version', 'unknown')}, client {__version__}); "
            "run mypr-mcp restart from this workspace to replace it explicitly"
        )
    if not isinstance(state.get("capabilities"), list) or not isinstance(
        state.get("instructions"), str
    ):
        raise RuntimeError("Invalid workspace protocol descriptor; inspect .mypr/manager.log")
    return state


def target_installation():
    return {
        "python": sys.executable,
        "package_root": str(Path(__file__).resolve().parent.parent),
        "version": __version__,
    }


def runtime_info(state, bridge_version=__version__, *, include_instructions=False):
    state = check_compatibility(state)
    try:
        pending = Version(bridge_version) > Version(state["version"])
    except (InvalidVersion, KeyError) as exc:
        raise RuntimeError("Invalid runtime package version") from exc
    result = {
        "bridge_version": bridge_version,
        "manager_version": state["version"],
        "protocol_version": state["protocol_version"],
        "generation": state.get("generation"),
        "update_pending": pending,
        "capabilities": state["capabilities"],
    }
    if include_instructions:
        result["instructions"] = state["instructions"]
    return result
