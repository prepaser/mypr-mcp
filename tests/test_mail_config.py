from __future__ import annotations

import pytest

from mypr_mcp.config import ConfigError, ConfigStore, validate_config

ACCOUNT = {
    "from": "Me <me@example.com>",
    "imap": {
        "host": "imap.example.test",
        "username": "me",
        "password_from": "IMAP_PASSWORD",
    },
    "smtp": {
        "host": "smtp.example.test",
        "username": "me",
        "password_from": "SMTP_PASSWORD",
    },
}


def _write(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def test_mail_config_normalizes_endpoints_without_resolving_credentials():
    values = validate_config({"mail": {"accounts": {"work": ACCOUNT}}})
    account = values["mail"]["accounts"]["work"]
    assert account["imap"]["port"] == 993
    assert account["smtp"]["port"] == 465
    assert account["imap"]["password_from"] == "IMAP_PASSWORD"
    assert "password" not in account["imap"]


def test_mail_accounts_are_complete_replacements_and_tombstones(tmp_path):
    workspace = tmp_path / "workspace"
    global_path = tmp_path / "global.toml"
    _write(
        global_path,
        "[mail]\ndefault_account = 'work'\n"
        "[mail.accounts.work]\nfrom = 'me@example.test'\n"
        "[mail.accounts.work.imap]\nhost = 'imap.example.test'\n"
        "username = 'me'\npassword_from = 'I'\n"
        "[mail.accounts.work.smtp]\nhost = 'smtp.example.test'\n\n",
    )
    _write(
        workspace / ".mypr/config.toml",
        "[mail.accounts.work]\nfrom = 'other@example.test'\n"
        "[mail.accounts.work.imap]\nhost = 'imap.other.test'\n"
        "username = 'other'\npassword_from = 'I2'\n"
        "[mail.accounts.work.smtp]\nhost = 'smtp.other.test'\n\n",
    )
    store = ConfigStore(workspace, global_path)
    assert store.load().values["mail"]["accounts"]["work"]["from"] == "other@example.test"
    assert store.load().values["mail"]["accounts"]["work"]["imap"]["host"] == "imap.other.test"

    snapshot = store.load()
    store.save_mail_account("work", {"enabled": False}, snapshot.revision)
    assert store.load().values["mail"]["accounts"] == {}


def test_mail_default_is_cleared_when_inherited_account_is_tombstoned(tmp_path):
    workspace = tmp_path / "workspace"
    global_path = tmp_path / "global.toml"
    _write(
        global_path,
        "[mail]\ndefault_account = 'work'\n"
        "[mail.accounts.work]\nfrom = 'me@example.test'\n"
        "[mail.accounts.work.imap]\nhost = 'imap.example.test'\n"
        "username = 'me'\npassword_from = 'I'\n"
        "[mail.accounts.work.smtp]\nhost = 'smtp.example.test'\n",
    )
    _write(workspace / ".mypr/config.toml", "[mail.accounts.work]\nenabled = false\n")
    snapshot = ConfigStore(workspace, global_path).load()
    assert snapshot.values["mail"] == {"default_account": "", "accounts": {}}


def test_mail_validation_rejects_partial_or_inline_passwords():
    with pytest.raises(ConfigError, match="mail.accounts.work.imap"):
        validate_config({"mail": {"accounts": {"work": {"from": "me@example.test"}}}})
    with pytest.raises(ConfigError, match="unknown fields"):
        validate_config(
            {
                "mail": {
                    "accounts": {
                        "work": {
                            **ACCOUNT,
                            "imap": {**ACCOUNT["imap"], "password": "secret"},
                        }
                    }
                }
            }
        )
    with pytest.raises(ConfigError, match="security"):
        validate_config(
            {
                "mail": {
                    "accounts": {
                        "work": {
                            **ACCOUNT,
                            "imap": {**ACCOUNT["imap"], "security": "tls"},
                        }
                    }
                }
            }
        )


def test_mail_public_paths_only_allow_whole_account_replacements(tmp_path):
    store = ConfigStore(tmp_path, tmp_path / "global.toml")
    snapshot = store.load()
    with pytest.raises(ConfigError):
        store.set(
            "mail.accounts.work.imap.host",
            "other.example",
            expected_revision=snapshot.revision,
        )
    saved = store.set("mail.accounts.work", ACCOUNT, expected_revision=snapshot.revision)
    assert store.get("mail.accounts.work", snapshot=saved) == {
        "from": "Me <me@example.com>",
        "imap": {
            "host": "imap.example.test",
            "port": 993,
            "security": "ssl",
            "username": "me",
            "password_from": "IMAP_PASSWORD",
        },
        "smtp": {
            "host": "smtp.example.test",
            "port": 465,
            "security": "ssl",
            "username": "me",
            "password_from": "SMTP_PASSWORD",
        },
    }
