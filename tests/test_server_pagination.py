from __future__ import annotations

import pytest

from mypr_mcp.services import _page_servers


@pytest.fixture
def servers() -> dict[str, dict[str, str]]:
    return {name: {"command": "server"} for name in ("alpha", "bravo", "charlie")}


def test_server_pages_accept_decimal_string_cursors_and_none(servers):
    assert _page_servers(servers, {"cursor": "1", "limit": 1}) == {
        "servers": [{"name": "bravo", "transport": "stdio"}],
        "next_cursor": "2",
    }
    assert len(_page_servers(servers, {"cursor": None})["servers"]) == 3


@pytest.mark.parametrize("cursor", [-1, "-1", "", True, 1.5, "bad"])
def test_server_pages_reject_invalid_cursors(servers, cursor):
    with pytest.raises(ValueError, match="cursor"):
        _page_servers(servers, {"cursor": cursor})


@pytest.mark.parametrize("limit", [0, -1, 1001, True, 1.5, "10"])
def test_server_pages_reject_invalid_limits(servers, limit):
    with pytest.raises(ValueError, match="limit"):
        _page_servers(servers, {"limit": limit})
