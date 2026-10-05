from __future__ import annotations

import io
import json
import sys
from types import SimpleNamespace

from mypr_mcp import system_base, system_probe
from mypr_mcp.system_tools import _bounded


def test_system_text_preserves_surrogateescaped_path_bytes():
    value = "file-\udcff.txt"
    assert system_base._text(value) == value


def test_system_output_budget_handles_surrogateescaped_paths():
    value = {"warnings": [], "path": "file-\udcff.txt"}
    assert _bounded(value)["path"] == "file-\udcff.txt"


def test_system_probe_serializes_surrogateescaped_paths(monkeypatch, capsys):
    monkeypatch.setattr(
        sys,
        "stdin",
        SimpleNamespace(buffer=io.BytesIO(b'{"section":"info"}')),
    )
    monkeypatch.setattr(
        system_base,
        "collect",
        lambda *_: {"warnings": [], "path": "file-\udcff.txt"},
    )
    assert system_probe.main() == 0
    assert json.loads(capsys.readouterr().out)["data"]["path"] == "file-\udcff.txt"
