"""Durable execution results for cells that replace their own manager."""

import json
import os
import sqlite3
import time
from pathlib import Path

from .diagnostics import RPCError
from .history import History
from .journal import read_page


def restart_id_for_execution(workspace, exec_id):
    if (
        not isinstance(exec_id, str)
        or len(exec_id) != 32
        or any(char not in "0123456789abcdef" for char in exec_id)
    ):
        return None
    path = Path(workspace) / ".mypr" / "runs" / f"{exec_id}.json"
    try:
        record = json.loads(path.read_text())
    except (FileNotFoundError, OSError, ValueError):
        return None
    ident = record.get("restart_id")
    return ident if isinstance(ident, str) else None


def finalize_origin(workspace, ticket):
    origin = ticket.get("origin") or {}
    ident = origin.get("exec_id")
    if not ident:
        return
    path = Path(workspace) / ".mypr" / "runs" / f"{ident}.json"
    try:
        record = json.loads(path.read_text())
    except FileNotFoundError:
        return
    if record.get("restart_finalized") == ticket["id"]:
        return
    state = ticket["state"]
    if state not in {"succeeded", "failed"}:
        return
    error = ticket.get("error") if state == "failed" else None
    text = (
        f"Workspace restart completed: {ticket.get('new_version', 'unknown')} "
        f"(generation {ticket.get('new_generation', 'unknown')})"
        if state == "succeeded"
        else f"Workspace restart failed: {error}"
    )
    record.update(
        state=state,
        error=error,
        finished=time.time(),
        restart_id=ticket["id"],
        restart_finalized=ticket["id"],
        restart_result=text,
    )
    temporary = path.with_suffix(f".{os.getpid()}.tmp")
    temporary.write_text(json.dumps(record))
    os.replace(temporary, path)
    history = History(Path(workspace))
    try:
        history.record("execution", record, event=state)
    finally:
        history.close()


def poll_restart(workspace, exec_id, cursor=0, *, max_bytes=None):
    from .restart import read_ticket

    if (
        not isinstance(exec_id, str)
        or len(exec_id) != 32
        or any(char not in "0123456789abcdef" for char in exec_id)
    ):
        return None
    if type(cursor) is not int or cursor < 0:
        raise ValueError("Invalid output cursor")
    path = Path(workspace) / ".mypr" / "runs" / f"{exec_id}.json"
    try:
        record = json.loads(path.read_text())
    except FileNotFoundError:
        return None
    ident = record.get("restart_id")
    if not ident:
        return None
    ticket = read_ticket(workspace, ident)
    if not ticket or (ticket.get("origin") or {}).get("exec_id") != exec_id:
        return None
    budget = 32768 if max_bytes is None else max_bytes
    if type(budget) is not int or not 1024 <= budget <= 1048576:
        raise RPCError("max_bytes must be between 1024 and 1048576 bytes", code="invalid_request")
    error = ticket.get("error") if ticket["state"] == "failed" else None
    if error is not None and max_bytes is not None:
        error = error.encode(errors="replace")[:min(1024, budget // 4)].decode(errors="ignore")
    error_size = len(json.dumps(error, ensure_ascii=False).encode())
    evicted = _output_evicted(Path(workspace), exec_id)
    state = ticket["state"]
    events, count = [], cursor
    if state in {"succeeded", "failed"}:
        # The coordinator does not write the manager's output journal.
        _, total = read_page(path.with_suffix(".jsonl"), 0, 0) if not evicted else ([], 0)
        if not evicted and not 0 <= cursor <= total + 1:
            raise ValueError("Invalid output cursor")
        if cursor <= total and not evicted:
            events, count = read_page(path.with_suffix(".jsonl"), cursor, budget, error_size)
            if events and max_bytes is not None:
                _check_budget(events[0], error_size, budget, cursor)
            if cursor + len(events) == total and record.get("restart_result"):
                final = {"type": "result", "text": record["restart_result"]}
                if (
                    not events
                    or len(json.dumps([*events, final], ensure_ascii=False).encode())
                    + error_size <= budget
                ):
                    if not events and max_bytes is not None:
                        _check_budget(final, error_size, budget, cursor)
                    events.append(final)
        count = total + bool(record.get("restart_result")) if not evicted else cursor
    return {
        "exec_id": exec_id,
        "client_id": record.get("client_id"),
        "connection_id": record.get("connection_id"),
        "generation": ticket.get("new_generation") or record["generation"],
        "execution_generation": record["generation"],
        "state": state if state in {"succeeded", "failed"} else "running",
        "output": events,
        "cursor": cursor + len(events),
        "has_more": cursor + len(events) < count,
        "truncated": evicted or record.get("truncated", False),
        "error": error,
        **({"output_evicted": True, "warnings": [
            {"code": "output_expired", "text": "Restart execution output has expired"}
        ]} if evicted else {}),
        "restart": {
            key: ticket[key]
            for key in ("id", "state", "new_version", "new_generation")
            if key in ticket
        },
    }


def _check_budget(event, error_size, budget, cursor):
    needed = error_size + len(json.dumps(event, ensure_ascii=False).encode())
    if needed > budget:
        raise RPCError(
            f"Output event at cursor {cursor} requires {needed} bytes; "
            "increase max_bytes to continue (cursor unchanged)",
            code="invalid_request", operation="poll",
            details={"cursor": cursor, "required_bytes": needed},
        )


def _output_evicted(workspace, exec_id):
    database = workspace.resolve() / ".mypr" / "history.sqlite3"
    if not database.is_file():
        return False
    connection = sqlite3.connect(database.as_uri() + "?mode=ro", uri=True, timeout=30)
    try:
        row = connection.execute(
            "SELECT json_extract(data, '$.output_evicted') FROM entities WHERE id=?", (exec_id,)
        ).fetchone()
        return bool(row and row[0])
    finally:
        connection.close()
