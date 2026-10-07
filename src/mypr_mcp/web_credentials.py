"""Local validation shared by web readiness checks and transport."""

from __future__ import annotations

from typing import Literal

CredentialStatus = Literal["missing", "invalid", "valid"]
MAX_WEB_CREDENTIAL_LENGTH = 8192


def web_credential_status(value: object) -> CredentialStatus:
    if value is None or value == "":
        return "missing"
    if (
        not isinstance(value, str)
        or len(value) > MAX_WEB_CREDENTIAL_LENGTH
        or any(ord(char) < 0x20 or ord(char) > 0x7E for char in value)
    ):
        return "invalid"
    return "valid"


__all__ = ["MAX_WEB_CREDENTIAL_LENGTH", "web_credential_status"]
