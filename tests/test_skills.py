from pathlib import Path

import pytest

from mypr_mcp.kernel_api import Skills, Workspace


def write_skill(root: Path, name: str, text: str) -> Path:
    path = root / name / "SKILL.md"
    path.parent.mkdir(parents=True)
    path.write_text(text, encoding="utf-8")
    return path


def test_malformed_metadata_is_reported_without_breaking_inspect(tmp_path: Path):
    root = tmp_path / ".mypr" / "skills"
    write_skill(root, "broken", "---\ndescription: [broken\n---\n# Broken\n")
    write_skill(root, "good", "---\ndescription: Good\n---\n# Good\n")
    skills = Skills(tmp_path)

    items = skills.list()
    broken = next(item for item in items if item["name"] == "broken")
    good = next(item for item in items if item["name"] == "good")

    assert "Invalid YAML front matter:" in broken["error"]
    assert good["description"] == "Good"
    assert Workspace(tmp_path).inspect()["skills"] == items


def test_malformed_skill_recovers_after_edit(tmp_path: Path):
    root = tmp_path / ".mypr" / "skills"
    path = write_skill(root, "editable", "---\ntags: [broken\n---\n")
    skills = Skills(tmp_path)

    assert "error" in skills.list()[0]

    path.write_text("---\ndescription: fixed\n---\n# Fixed\n", encoding="utf-8")
    item = skills.list()[0]
    assert item["name"] == "editable"
    assert item["description"] == "fixed"
    assert "error" not in item


def test_list_applies_read_path_policy_to_symlinks(tmp_path: Path):
    root = tmp_path / ".mypr" / "skills"
    inside = write_skill(root, "inside", "# Inside\n")
    external_root = tmp_path / "external"
    external = write_skill(external_root, "outside", "# Outside\n")
    internal_link = root / "internal-link"
    external_link = root / "external-link"
    internal_link.symlink_to(inside.parent, target_is_directory=True)
    external_link.symlink_to(external.parent, target_is_directory=True)
    skills = Skills(tmp_path)

    names = {item["name"] for item in skills.list()}
    assert {"inside", "internal-link"} <= names
    assert "external-link" not in names
    assert skills.read("internal-link") == "# Inside\n"
    with pytest.raises(ValueError, match="escapes workspace"):
        skills.read("external-link")
