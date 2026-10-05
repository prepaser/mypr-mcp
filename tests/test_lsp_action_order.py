from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

from mypr_mcp.code_tools import CodeTools, _Document
from mypr_mcp.lsp_edits import apply_text_edits


def _insert(line: int, character: int, text: str) -> dict:
    point = {"line": line, "character": character}
    return {"range": {"start": point, "end": point.copy()}, "newText": text}


def _replace(line: int, start: int, end: int, text: str) -> dict:
    return {
        "range": {
            "start": {"line": line, "character": start},
            "end": {"line": line, "character": end},
        },
        "newText": text,
    }


def test_same_position_inserts_keep_protocol_order() -> None:
    assert apply_text_edits("x", [_insert(0, 1, "A"), _insert(0, 1, "B")], "utf-16") == "xAB"


def test_same_position_inserts_precede_a_replacement() -> None:
    edits = [_insert(0, 0, "A"), _insert(0, 0, "B"), _replace(0, 0, 1, "C")]
    assert apply_text_edits("x", edits, "utf-16") == "ABC"


@pytest.mark.asyncio
async def test_disabled_action_listing_does_not_read_its_edit_targets(tmp_path: Path) -> None:
    path = tmp_path / "sample.py"
    path.write_text("x\n", encoding="utf-8")
    document = _Document(path, path.as_uri(), "python", "x\n", 1)
    server = SimpleNamespace(
        name="fake",
        generation="generation",
        position_encoding="utf-16",
        documents={document.uri: document},
        capabilities={"codeActionProvider": {}},
        process=SimpleNamespace(returncode=None),
        _failure=None,
        _operation_lock=asyncio.Lock(),
    )

    async def code_actions(*_args, **_kwargs):
        return document, [
            {
                "title": "Disabled external edit",
                "disabled": {"reason": "not applicable"},
                "edit": {"changes": {"file:///tmp/outside.py": []}},
            }
        ]

    async def aclose() -> None:
        return None

    server.code_actions = code_actions
    server.aclose = aclose
    code = CodeTools(tmp_path)
    code._servers["fake"] = server
    try:
        result = await code.actions("fake", path, 1, 1)
        assert result["actions"][0]["supported"] is False
        with pytest.raises(ValueError, match="disabled"):
            await code.prepare_action(result["actions"][0]["action_id"])
    finally:
        await code.aclose()
