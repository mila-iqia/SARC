import json
import logging

import pytest
from serieux.features.encrypt import EncryptionKey, crypt_prefix


@pytest.fixture
def password(monkeypatch):
    pwd = "test-sarc-password"
    monkeypatch.setenv("SERIEUX_PASSWORD", pwd)
    return pwd


@pytest.fixture
def ek(password):
    return EncryptionKey(password=password)


# --- encrypt file ---


def test_encrypt_file_encrypts_json(cli_main, tmp_path, ek):
    data = {"key": "value", "num": 42}
    path = tmp_path / "secret.json"
    path.write_text(json.dumps(data))

    assert cli_main(["encrypt", "file", "--path", str(path)]) == 0

    content = path.read_text()
    assert content.startswith(crypt_prefix)
    assert ek.decrypt(content) == data


def test_encrypt_file_already_encrypted_returns_error(cli_main, tmp_path, ek, caplog):
    path = tmp_path / "secret.json"
    path.write_text(ek.encrypt({"key": "value"}))

    caplog.clear()
    with caplog.at_level(logging.ERROR):
        result = cli_main(["encrypt", "file", "--path", str(path)])

    assert result == -1
    assert "already encrypted" in caplog.text


# --- encrypt append ---


def test_encrypt_append_adds_key(cli_main, tmp_path, ek):
    path = tmp_path / "secret.json"
    path.write_text(ek.encrypt({"existing": "data"}))

    assert (
        cli_main(
            [
                "encrypt",
                "append",
                "--path",
                str(path),
                "--key",
                "new_key",
                "--value",
                "new_value",
            ]
        )
        == 0
    )

    decrypted = ek.decrypt(path.read_text())
    assert decrypted["existing"] == "data"
    assert decrypted["new_key"] == "new_value"


def test_encrypt_append_overwrites_existing_key(cli_main, tmp_path, ek):
    path = tmp_path / "secret.json"
    path.write_text(ek.encrypt({"key": "old_value"}))

    assert (
        cli_main(
            [
                "encrypt",
                "append",
                "--path",
                str(path),
                "--key",
                "key",
                "--value",
                "new_value",
            ]
        )
        == 0
    )

    decrypted = ek.decrypt(path.read_text())
    assert decrypted["key"] == "new_value"


def test_encrypt_append_result_is_still_encrypted(cli_main, tmp_path, ek):
    path = tmp_path / "secret.json"
    path.write_text(ek.encrypt({}))

    cli_main(["encrypt", "append", "--path", str(path), "--key", "k", "--value", "v"])

    assert path.read_text().startswith(crypt_prefix)
