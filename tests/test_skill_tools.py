from __future__ import annotations

from pathlib import Path

import pytest

from mypr_mcp.filesystem import Filesystem
from mypr_mcp.kernel_api import Skills


def skills(tmp_path: Path) -> Skills:
    return Skills(tmp_path, Filesystem(tmp_path))


@pytest.mark.asyncio
async def test_validate_accepts_legacy_and_reports_missing_markdown(tmp_path: Path):
    value = skills(tmp_path)
    result = await value.validate("demo", "# Demo\n\n[details](details.md)\n")
    assert result["valid"] is True
    assert result["warnings"] == [
        "legacy skill without YAML front matter",
        "local Markdown reference is missing: details.md",
    ]


@pytest.mark.asyncio
async def test_write_rejects_bad_yaml_and_supports_cas(tmp_path: Path):
    value = skills(tmp_path)
    with pytest.raises(ValueError, match="Invalid skill"):
        await value.write("bad", "---\ntags: [broken\n---\n")
    created = await value.write("demo", "---\nname: demo\n---\n# Demo\n")
    with pytest.raises(ValueError, match="Revision mismatch"):
        await value.write("demo", "# changed\n", expected_hash="0" * 64)
    preview = await value.write(
        "demo", "---\nname: demo\n---\n# Changed\n", expected_hash=created["revision"], dry_run=True
    )
    assert preview["dry_run"] is True
    assert "Changed" not in (tmp_path / ".mypr" / "skills" / "demo" / "SKILL.md").read_text()


@pytest.mark.asyncio
async def test_validate_rejects_non_mapping_and_non_string_name(tmp_path: Path):
    value = skills(tmp_path)
    root = await value.validate("scalar", "---\n- one\n---\n")
    assert root["valid"] is False
    assert "root must be a mapping" in root["errors"][0]
    name = await value.validate("typed", "---\nname: [one]\n---\n")
    assert name["valid"] is False
    assert "name must be a string" in name["errors"][0]
    description = await value.validate("description", "---\ndescription: [one]\n---\n")
    assert description["valid"] is False
    assert "description must be a string" in description["errors"][0]


@pytest.mark.asyncio
async def test_write_rejects_dot_paths_before_filesystem_access(tmp_path: Path):
    value = skills(tmp_path)
    with pytest.raises(ValueError, match="invalid path component"):
        await value.write(".", "# invalid\n")
    with pytest.raises(ValueError, match="relative path"):
        await value.write("/outside", "# invalid\n")


@pytest.mark.asyncio
async def test_dry_run_existing_skill_requires_revision(tmp_path: Path):
    value = skills(tmp_path)
    await value.write("demo", "# Demo\n")
    with pytest.raises(FileExistsError, match="expected_hash"):
        await value.write("demo", "# Changed\n", dry_run=True)
