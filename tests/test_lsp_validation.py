from __future__ import annotations

import pytest

from mypr_mcp.code_tools import CodeTools
from mypr_mcp.config import ConfigError, ConfigStore


def _definition(command: list[str], languages: list[str]) -> dict:
    return {"command": command, "languages": languages}


@pytest.mark.parametrize(
    ("command", "languages", "message"),
    [
        (["server"] * 65, ["python"], "at most 64 strings"),
        (["x" * 4097], ["python"], "at most 4096 characters"),
        (["server"], [f"lang_{index}" for index in range(65)], "at most 64 language IDs"),
        (["server"], ["python/lsp"], "language IDs must be"),
    ],
)
def test_persisted_lsp_configuration_uses_runtime_limits(
    tmp_path, command, languages, message
):
    store = ConfigStore(tmp_path, tmp_path / "global.toml")

    with pytest.raises(ConfigError, match=message):
        store.save_server("lsp", "demo", _definition(command, languages))


def test_persisted_lsp_configuration_accepts_runtime_boundaries(tmp_path):
    command = [f"arg_{index}" for index in range(64)]
    command[-1] = "x" * 4096
    languages = [f"lang_{index}" for index in range(64)]
    definition = _definition(command, languages)
    store = ConfigStore(tmp_path, tmp_path / "global.toml")

    snapshot = store.save_server("lsp", "demo", definition)

    assert snapshot.values["lsp"]["servers"]["demo"]["command"] == command
    assert snapshot.values["lsp"]["servers"]["demo"]["languages"] == languages
    assert CodeTools._configuration(command, languages) == (tuple(command), frozenset(languages))
