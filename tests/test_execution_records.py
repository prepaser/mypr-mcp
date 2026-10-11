import pytest

import mypr_mcp.execution_records as execution_records
from mypr_mcp.execution_records import (
    MAX_EXECUTION_RECORD_BYTES,
    execution_record_bytes,
    read_execution_record,
)
from mypr_mcp.transport import MAX_MESSAGE


def test_execution_record_bound_accounts_for_record_envelope():
    record = {
        "id": "a" * 32,
        "state": "succeeded",
        "code": "x" * (MAX_MESSAGE - 16),
        "output": "body",
    }
    encoded = execution_record_bytes(record)

    assert len(encoded) > MAX_MESSAGE
    assert len(encoded) <= MAX_EXECUTION_RECORD_BYTES


def test_execution_record_bytes_is_compact_utf8():
    encoded = execution_record_bytes({"text": "한글", "state": "succeeded"})

    assert encoded == '{"text":"한글","state":"succeeded"}'.encode()


def test_read_execution_record_requires_an_object(tmp_path):
    path = tmp_path / "record.json"
    path.write_bytes(b"[]")

    with pytest.raises(ValueError, match="must be an object"):
        read_execution_record(path)


def test_execution_record_writer_and_reader_share_the_bound(tmp_path, monkeypatch):
    monkeypatch.setattr(execution_records, "MAX_EXECUTION_RECORD_BYTES", 16)
    record = {"value": "0123456789"}
    path = tmp_path / "record.json"
    path.write_bytes(b'{"value":"0123456789"}')

    with pytest.raises(ValueError, match="exceeds 16 bytes"):
        execution_records.execution_record_bytes(record)
    with pytest.raises(ValueError, match="exceeds 16 bytes"):
        execution_records.read_execution_record(path)
