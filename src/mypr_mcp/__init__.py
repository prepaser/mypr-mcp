from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("mypr-mcp")
except PackageNotFoundError:
    from pathlib import Path
    from tomllib import loads

    source = Path(__file__).resolve().parents[1]
    project = source.parent / "pyproject.toml"
    if source.name != "src" or not project.is_file():
        raise
    __version__ = loads(project.read_text(encoding="utf-8"))["project"]["version"]
