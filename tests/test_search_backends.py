from __future__ import annotations

import json
import shutil
import subprocess
import zipfile
from pathlib import Path

import pytest

from mypr_mcp.search_backends import build, inspect_backends, parse_ast


def test_rga_builds_document_search_with_workspace_configuration(tmp_path: Path):
    command = build(
        tmp_path,
        "rga",
        {
            "pattern": ["needle", "TODO"],
            "paths": ["docs", "archive.zip"],
            "glob": ["*.pdf", "!vendor/**"],
            "mode": "matches",
            "fixed": True,
            "ignore_case": True,
            "accurate": True,
            "cache": True,
            "archive_depth": 3,
            "adapters": ["pandoc", "poppler"],
        },
    )
    assert command[:2] == [
        "rga",
        "--rga-config-file=" + str(tmp_path / ".mypr/searches/config/rga.json"),
    ]
    assert "--rga-cache-path=" + str(tmp_path / ".mypr/searches/rga-cache") in command
    assert "--rga-max-archive-recursion=3" in command
    assert "--rga-adapters=pandoc,poppler" in command
    assert "--json" in command
    assert "--fixed-strings" in command
    assert "--" in command
    assert command.count("--regexp") == 2


@pytest.mark.parametrize(
    ("mode", "expected"),
    [
        ("files", "--files-with-matches"),
        ("counts", "--count-matches"),
        ("exists", "--quiet"),
    ],
)
def test_rga_modes(mode: str, expected: str, tmp_path: Path):
    command = build(tmp_path, "rga", {"pattern": "x", "mode": mode})
    assert expected in command
    assert "--json" not in command


def test_ast_pattern_build_is_read_only_and_disables_rewrite(tmp_path: Path):
    command = build(
        tmp_path,
        "ast",
        {
            "pattern": "requests.get($URL)",
            "lang": "python",
            "paths": ["src"],
            "glob": ["*.py"],
            "hidden": True,
        },
    )
    assert command[:8] == [
        "ast-grep",
        "run",
        "--config",
        str(tmp_path / ".mypr/searches/config/sgconfig.yml"),
        "--pattern",
        "requests.get($URL)",
        "--lang",
        "python",
    ]
    assert "--json=stream" in command
    assert "--threads" in command and command[command.index("--threads") + 1] == "2"
    assert "--no-ignore" in command and "hidden" in command
    assert "--globs" in command and "*.py" in command


def test_ast_rule_is_inline_json_with_safe_defaults(tmp_path: Path):
    command = build(
        tmp_path,
        "ast",
        {
            "rule": {"kind": "call_expression", "has": {"pattern": "print($A)"}},
            "lang": "python",
            "constraints": {"A": {"regex": "^x"}},
            "utils": {"is_x": {"pattern": "x"}},
        },
    )
    inline = json.loads(command[command.index("--inline-rules") + 1])
    assert inline["id"] == "mypr-search"
    assert inline["language"] == "python"
    assert inline["severity"] == "hint"
    assert inline["rule"]["kind"] == "call_expression"
    assert "--hint=mypr-search" in command
    assert "--include-metadata" in command


@pytest.mark.parametrize("field", ["rewrite", "fix", "ruleDirs", "testConfigs"])
def test_ast_rejects_mutating_or_project_configuration_rules(tmp_path: Path, field: str):
    with pytest.raises(ValueError, match="does not support"):
        build(tmp_path, "ast", {"rule": {field: "bad"}, "lang": "python"})


def test_parse_ast_normalizes_positions_and_captures():
    result = parse_ast(
        {
            "file": "src/app.py",
            "language": "Python",
            "text": "print(value)",
            "range": {
                "start": {"line": 4, "column": 2},
                "end": {"line": 4, "column": 14},
                "byteOffset": {"start": 51, "end": 63},
            },
            "metaVariables": {
                "single": {
                    "A": {
                        "text": "value",
                        "range": {
                            "start": {"line": 4, "column": 6},
                            "end": {"line": 4, "column": 11},
                            "byteOffset": {"start": 55, "end": 60},
                        },
                    }
                },
                "multi": {"ARGS": []},
                "transformed": {"A": "value"},
            },
        }
    )
    assert result["line"] == 5
    assert result["column"] == 3
    assert result["range"] == {
        "start": {"line": 5, "column": 3, "byte": 51},
        "end": {"line": 5, "column": 15, "byte": 63},
    }
    assert result["captures"]["single"]["A"]["range"]["start"]["byte"] == 55
    assert "transformed" not in result["captures"]


async def test_inspect_backends_uses_bounded_runner_and_verifies_sg(tmp_path: Path):
    calls = []

    async def runner(command, **kwargs):
        calls.append((command, kwargs))
        if command == ["rg", "--version"]:
            return {"returncode": 0, "stdout": "ripgrep 14\nPCRE2 10.4\n"}
        if command == ["rga", "--version"]:
            return {"returncode": 0, "stdout": "rga 0.10\n"}
        config_arg = "--rga-config-file=" + str(tmp_path / ".mypr/searches/config/rga.json")
        if command[0:2] == ["rga", config_arg]:
            return {"returncode": 0, "stdout": "pandoc\npoppler\n"}
        if command == ["rg", "--pcre2-version"]:
            return {"returncode": 0, "stdout": "PCRE2 10.4\n"}
        if command == ["ast-grep", "--version"]:
            return {"returncode": 1, "stderr": "missing"}
        if command == ["sg", "--version"]:
            return {"returncode": 0, "stdout": "ast-grep 0.40\n"}
        if command == ["sg", "run", "--help"]:
            return {"returncode": 0, "stdout": "--json=stream"}
        if command == ["sg", "scan", "--help"]:
            return {"returncode": 0, "stdout": "--inline-rules"}
        raise AssertionError(command)

    result = await inspect_backends(tmp_path, runner)
    assert result["backends"]["rg"]["features"]["pcre2"] is True
    assert result["backends"]["rga"]["adapters"] == ["pandoc", "poppler"]
    assert result["backends"]["ast"]["binary"] == "sg"
    assert all(call[1]["timeout"] == 2 and call[1]["max_bytes"] == 65536 for call in calls)


@pytest.mark.skipif(shutil.which("rga") is None, reason="rga is not installed")
def test_rga_searches_a_zip_with_workspace_owned_configuration(tmp_path: Path):
    archive = tmp_path / "docs.zip"
    with zipfile.ZipFile(archive, "w") as output:
        output.writestr("nested/readme.txt", "needle in an archive\n")
    command = build(tmp_path, "rga", {"pattern": "needle", "mode": "matches"})
    completed = subprocess.run(command, cwd=tmp_path, capture_output=True, text=True, timeout=10)
    assert completed.returncode == 0
    assert "needle" in completed.stdout
    assert (tmp_path / ".mypr/searches/config/rga.json").read_text() == "{}\n"


@pytest.mark.skipif(shutil.which("rga") is None, reason="rga is not installed")
def test_rga_searches_a_zip_with_ephemeral_cache_path(tmp_path: Path):
    archive = tmp_path / "docs.zip"
    with zipfile.ZipFile(archive, "w") as output:
        output.writestr("nested/readme.txt", "ephemeral needle\n")
    cache_path = tmp_path / "ephemeral-cache"
    command = build(
        tmp_path,
        "rga",
        {"pattern": "ephemeral", "mode": "matches", "cache": False, "_cache_path": cache_path},
    )
    assert "--rga-no-cache" not in command
    completed = subprocess.run(command, cwd=tmp_path, capture_output=True, text=True, timeout=10)
    assert completed.returncode == 0
    assert "ephemeral needle" in completed.stdout


@pytest.mark.skipif(shutil.which("ast-grep") is None, reason="ast-grep is not installed")
@pytest.mark.parametrize(
    "options",
    [
        {"pattern": "print($A)", "lang": "python"},
        {"rule": {"pattern": "print($A)"}, "lang": "python"},
    ],
)
def test_ast_searches_python_without_modifying_source(tmp_path: Path, options: dict):
    source = tmp_path / "example.py"
    original = "print(value)\n"
    source.write_text(original)
    command = build(tmp_path, "ast", options)
    completed = subprocess.run(command, cwd=tmp_path, capture_output=True, text=True, timeout=10)
    assert completed.returncode == 0
    record = json.loads(completed.stdout.splitlines()[0])
    assert parse_ast(record)["path"] == "example.py"
    assert source.read_text() == original


@pytest.mark.skipif(shutil.which("ast-grep") is None, reason="ast-grep is not installed")
@pytest.mark.parametrize(
    "lang,name,text,pattern",
    [
        ("javascript", "a.js", 'console.log("hello");\n', "console.log($A)"),
        ("go", "a.go", 'package main\nfunc main() { println("hello") }\n', "println($A)"),
    ],
)
def test_ast_native_languages_and_capture_ranges(tmp_path, lang, name, text, pattern):
    (tmp_path / name).write_text(text)
    command = build(tmp_path, "ast", {"pattern": pattern, "lang": lang, "paths": [name]})
    run = subprocess.run(command, cwd=tmp_path, capture_output=True, text=True, timeout=5)
    assert run.returncode == 0, run.stderr
    match = parse_ast(json.loads(run.stdout.splitlines()[0]))
    assert match["captures"]["single"]["A"]["text"] == '"hello"'
    span = match["captures"]["single"]["A"]["range"]
    assert text.encode()[span["start"]["byte"] : span["end"]["byte"]] == b'"hello"'


@pytest.mark.skipif(shutil.which("ast-grep") is None, reason="ast-grep is not installed")
def test_ast_pattern_constraints_and_utils_are_applied(tmp_path):
    (tmp_path / "a.py").write_text('print(1)\nprint("hello")\n')
    command = build(
        tmp_path,
        "ast",
        {
            "pattern": "print($A)",
            "lang": "python",
            "paths": ["a.py"],
            "constraints": {"A": {"matches": "is_string"}},
            "utils": {"is_string": {"kind": "string"}},
        },
    )
    run = subprocess.run(command, cwd=tmp_path, capture_output=True, text=True, timeout=5)
    assert run.returncode == 0, run.stderr
    records = [json.loads(line) for line in run.stdout.splitlines()]
    assert len(records) == 1
    assert records[0]["text"] == 'print("hello")'


@pytest.mark.parametrize("adapters", [[], "", ["zip", ""], ["\0"]])
def test_rga_rejects_empty_or_invalid_adapter_lists(tmp_path, adapters):
    with pytest.raises(ValueError, match="adapters must contain"):
        build(tmp_path, "rga", {"pattern": "x", "adapters": adapters})
