from __future__ import annotations

import re
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from conftest import execute, mcp_session, result_text, stop_manager

from mypr_mcp.client_ids import ADJECTIVES, ANIMALS
from mypr_mcp.history import History

CLIENT_ID_RE = re.compile(r"^[a-z]+-[a-z]+$")


def _client_id_from_output(payload: dict) -> str:
    return result_text(payload).strip(" '\n")


def test_allocated_ids_are_readable_and_use_known_words(tmp_path: Path):
    assert len(ADJECTIVES) == len(set(ADJECTIVES)) == 256
    assert len(ANIMALS) == len(set(ANIMALS)) == 256
    assert all(re.fullmatch(r"[a-z]+", word) for word in (*ADJECTIVES, *ANIMALS))
    history = History(tmp_path)
    try:
        client_id = history.allocate_client_id()
        adjective, animal = client_id.split("-", 1)
        assert CLIENT_ID_RE.fullmatch(client_id)
        assert adjective in ADJECTIVES
        assert animal in ANIMALS
    finally:
        history.close()


def test_collision_retries_with_a_new_pair(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    history = History(tmp_path)
    try:
        choices = iter(
            (
                ADJECTIVES[0],
                ANIMALS[0],
                ADJECTIVES[0],
                ANIMALS[0],
                ADJECTIVES[1],
                ANIMALS[1],
            )
        )
        monkeypatch.setattr("mypr_mcp.history.secrets.choice", lambda words: next(choices))

        first = history.allocate_client_id()
        second = history.allocate_client_id()

        assert first == f"{ADJECTIVES[0]}-{ANIMALS[0]}"
        assert second == f"{ADJECTIVES[1]}-{ANIMALS[1]}"
    finally:
        history.close()


def test_allocated_ids_remain_reserved_after_database_reopen(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    history = History(tmp_path)
    choices = iter((ADJECTIVES[0], ANIMALS[0]))
    monkeypatch.setattr("mypr_mcp.history.secrets.choice", lambda words: next(choices))
    client_id = history.allocate_client_id()
    assert client_id == f"{ADJECTIVES[0]}-{ANIMALS[0]}"
    history.close()

    history = History(tmp_path)
    try:
        choices = iter((ADJECTIVES[0], ANIMALS[0], ADJECTIVES[1], ANIMALS[1]))
        monkeypatch.setattr(
            "mypr_mcp.history.secrets.choice",
            lambda words: next(choices),
        )
        assert history.allocate_client_id() == f"{ADJECTIVES[1]}-{ANIMALS[1]}"
    finally:
        history.close()


def test_legacy_history_records_reserve_client_ids(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    history = History(tmp_path)
    reserved = [
        f"{ADJECTIVES[0]}-{ANIMALS[0]}",
        f"{ADJECTIVES[1]}-{ANIMALS[1]}",
    ]
    history.record(
        "python",
        {"id": "legacy-execution", "client_id": reserved[0], "state": "succeeded"},
    )
    history.append("connection", "connected", {"id": "legacy-connection", "client_id": reserved[1]})
    history.close()

    history = History(tmp_path)
    try:
        choices = iter(
            (
                ADJECTIVES[0],
                ANIMALS[0],
                ADJECTIVES[1],
                ANIMALS[1],
                ADJECTIVES[2],
                ANIMALS[2],
            )
        )
        monkeypatch.setattr("mypr_mcp.history.secrets.choice", lambda words: next(choices))
        assert history.allocate_client_id() == f"{ADJECTIVES[2]}-{ANIMALS[2]}"
    finally:
        history.close()


def test_exhausted_wordlists_raise_clear_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr("mypr_mcp.history.ADJECTIVES", ("calm",))
    monkeypatch.setattr("mypr_mcp.history.ANIMALS", ("otter",))
    history = History(tmp_path)
    try:
        assert history.allocate_client_id() == "calm-otter"
        with pytest.raises(RuntimeError, match="No unused client IDs remain in this workspace"):
            history.allocate_client_id()
    finally:
        history.close()


def test_old_database_ids_are_migrated_and_last_free_pair_is_available(tmp_path, monkeypatch):
    monkeypatch.setattr("mypr_mcp.history.ADJECTIVES", ("calm",))
    monkeypatch.setattr("mypr_mcp.history.ANIMALS", ("otter", "owl", "fox"))
    monkeypatch.setattr("mypr_mcp.history.secrets.choice", lambda words: words[0])
    history = History(tmp_path)
    history.record("execution", {"id": "old", "client": "calm-otter"})
    history.append("connection", "connected", {"client_id": "calm-owl"})
    history._db.execute("DROP TABLE client_ids")
    history.close()

    history = History(tmp_path)
    try:
        assert history.allocate_client_id() == "calm-fox"
        with pytest.raises(RuntimeError, match="No unused client IDs"):
            history.allocate_client_id()
    finally:
        history.close()


def test_two_history_connections_allocate_unique_ids_concurrently(tmp_path: Path, monkeypatch):
    monkeypatch.setattr("mypr_mcp.history.ADJECTIVES", ("calm", "quiet"))
    monkeypatch.setattr("mypr_mcp.history.ANIMALS", ("otter", "owl", "fox", "ant"))
    monkeypatch.setattr("mypr_mcp.history.secrets.choice", lambda words: words[0])
    histories = [History(tmp_path), History(tmp_path)]
    try:
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = [
                executor.submit(
                    histories[index % len(histories)].allocate_client_id,
                )
                for index in range(8)
            ]
            client_ids = [future.result() for future in futures]
        assert len(set(client_ids)) == len(client_ids)
        assert all(CLIENT_ID_RE.fullmatch(client_id) for client_id in client_ids)
    finally:
        for history in histories:
            history.close()


@pytest.mark.asyncio
async def test_mcp_sessions_get_readable_unique_ids_and_survive_manager_restart(workspace: Path):
    async with mcp_session(workspace) as first:
        first_id = _client_id_from_output(await execute(first, "ws.client.id"))
        await execute(first, "ws.local['owner'] = 'first'")

    async with mcp_session(workspace) as second:
        second_id = _client_id_from_output(await execute(second, "ws.client.id"))
        local_state = _client_id_from_output(await execute(second, "'owner' in ws.local"))
        assert local_state == "False"

    await stop_manager(workspace)
    async with mcp_session(workspace) as third:
        third_id = _client_id_from_output(await execute(third, "ws.client.id"))

    assert CLIENT_ID_RE.fullmatch(first_id)
    assert CLIENT_ID_RE.fullmatch(second_id)
    assert CLIENT_ID_RE.fullmatch(third_id)
    assert len({first_id, second_id, third_id}) == 3
