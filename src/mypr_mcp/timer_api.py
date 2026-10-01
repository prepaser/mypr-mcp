"""Client-scoped timer helpers for the persistent workspace API."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any

from .diagnostics import RPCError


class TimerAPI:
    """Create and inspect persistent client-scoped deadline notifications."""

    def __init__(
        self,
        rpc: Callable[..., Awaitable[Any]],
        client_context: Any,
    ) -> None:
        self._rpc = rpc
        self._client_context = client_context

    def _require_client(self) -> None:
        if self._client_context.get() is None:
            raise RPCError("ws.timers requires an initialized client")

    @staticmethod
    def _at(value: datetime | str) -> str:
        if isinstance(value, datetime):
            parsed = value
        elif isinstance(value, str):
            text = value.strip()
            if not text:
                raise ValueError("at must be a timezone-aware ISO 8601 datetime")
            try:
                parsed = datetime.fromisoformat(text)
            except ValueError as exc:
                raise ValueError("at must be a timezone-aware ISO 8601 datetime") from exc
        else:
            raise TypeError("at must be a datetime or ISO 8601 string")
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError("at must include a timezone")
        return parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")

    async def start(
        self,
        seconds: int | float | None = None,
        *,
        at: datetime | str | None = None,
        label: str = "",
    ) -> dict[str, Any]:
        """Schedule a one-shot timer using a duration or timezone-aware deadline."""
        self._require_client()
        if (seconds is None) == (at is None):
            raise ValueError("provide exactly one of seconds or at")
        fields: dict[str, Any] = {"label": label}
        if seconds is not None:
            fields["seconds"] = seconds
        else:
            assert at is not None
            fields["at"] = self._at(at)
        return await self._rpc("timer_start", **fields)

    async def check(self, timer_id: str) -> dict[str, Any]:
        """Return the current state of one timer without acknowledging it."""
        self._require_client()
        return await self._rpc("timer_check", timer_id=timer_id)

    async def list(
        self,
        state: str | None = None,
        limit: int = 50,
        cursor: str | int | None = None,
    ) -> dict[str, Any]:
        """List this client's timers in bounded pages."""
        self._require_client()
        fields: dict[str, Any] = {"limit": limit}
        if state is not None:
            fields["state"] = state
        if cursor is not None:
            fields["cursor"] = cursor
        return await self._rpc("timer_list", **fields)

    async def cancel(self, timer_id: str) -> dict[str, Any]:
        """Cancel a scheduled timer; repeated cancellation is idempotent."""
        self._require_client()
        return await self._rpc("timer_cancel", timer_id=timer_id)

    async def ack(self, ids: list[str]) -> int:
        """Acknowledge expired timer notifications and return the changed count."""
        self._require_client()
        return await self._rpc("timer_ack", ids=ids)


__all__ = ["TimerAPI"]
