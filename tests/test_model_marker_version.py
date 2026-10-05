from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from mypr_mcp.dependency_store import DependencyStore


def _write_model(store: DependencyStore, version: str) -> None:
    model_dir = store.model_root / "tessdata_fast"
    model_dir.mkdir(parents=True)
    model = model_dir / "eng.traineddata"
    data = b"model fixture"
    model.write_bytes(data)
    (model_dir / ".eng.mypr-complete.json").write_text(
        json.dumps(
            {
                "name": "tessdata:eng",
                "version": version,
                "sha256": hashlib.sha256(data).hexdigest(),
            }
        )
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("version", ["", "garbage", "4.1.0-beta", "4"])
async def test_model_marker_rejects_non_release_versions(tmp_path: Path, version: str):
    store = DependencyStore(tmp_path / "data", tmp_path / "cache", platform_key="x86_64")
    try:
        _write_model(store, version)
        state = await store.inspect("tessdata:eng")
        assert state["status"] == "unusable"
    finally:
        await store.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("version", ["4.1.0", "4.2.0"])
async def test_model_marker_accepts_release_versions(tmp_path: Path, version: str):
    store = DependencyStore(tmp_path / "data", tmp_path / "cache", platform_key="x86_64")
    try:
        _write_model(store, version)
        state = await store.inspect("tessdata:eng")
        assert state["status"] == "installed"
        assert state["version"] == version
    finally:
        await store.close()
