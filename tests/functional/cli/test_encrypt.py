import json
import logging
import re
from pathlib import Path
from types import SimpleNamespace

import pytest
from cryptography.fernet import InvalidToken
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


# --- encrypt rotate ---

NEW_PASSWORD = "test-sarc-password-NEW"
blob_re = re.compile(r"~CRYPT~[^\s\"',]+")


def blobs(path):
    return blob_re.findall(Path(path).read_text())


@pytest.fixture
def config_tree(tmp_path, ek):
    """A mini config tree encrypted with the old password (`ek`), exercising:
    Secret-annotated fields, an included file, encrypted values inside plain
    JSON (user scrapers), a `sarc.client` section, and a whole-file encrypted
    mapping file in the `patches` directory."""
    (tmp_path / "pkey.yaml").write_text(
        "# the ssh key password\n"
        f'file: id_ed\npassword: "{ek.encrypt("pkey-password")}"\n'
    )
    (tmp_path / "users.yaml").write_text(
        "scrapers:\n"
        "  mymila:\n"
        "    client_id: plain-id\n"
        f'    client_secret: "{ek.encrypt("mymila-secret")}"\n'
    )
    mapping = tmp_path / "patches" / "scraping" / "mapping.json"
    mapping.parent.mkdir(parents=True)
    mapping.write_text(ek.encrypt({"drac": {"soumye": "2022-02-04"}}))
    (tmp_path / "patches" / "scraping" / "patch.py").write_text("")
    extra = tmp_path / "extra.json"
    extra.write_text(ek.encrypt({"secret": "extra-value"}))
    main = tmp_path / "sarc.yaml"
    main.write_text(
        "# main config\n"
        "sarc:\n"
        "  db:\n"
        "    host: localhost\n"
        "    name: db\n"
        "  patches: patches\n"
        "  users: users.yaml\n"
        "  clusters:\n"
        "    testc:\n"
        "      host: login.example.com\n"
        "      user_domain: test\n"
        "      private_key: pkey.yaml\n"
        "  client:\n"
        "    base_url: http://example.com/v0\n"
        f'    token: "{ek.encrypt("api-token")}"\n'
    )
    return SimpleNamespace(
        main=main,
        pkey=tmp_path / "pkey.yaml",
        users=tmp_path / "users.yaml",
        mapping=mapping,
        extra=extra,
        all=[main, tmp_path / "pkey.yaml", tmp_path / "users.yaml", mapping, extra],
    )


def test_encrypt_rotate_rotates_everything(cli_main, config_tree, ek, password):
    assert (
        cli_main(
            [
                "encrypt",
                "rotate",
                "--file",
                str(config_tree.main),
                "--extra",
                str(config_tree.extra),
                "--new_password",
                NEW_PASSWORD,
            ]
        )
        == 0
    )

    new_ek = EncryptionKey(NEW_PASSWORD)
    # every blob in the traversed config files can now be decrypted with the new key
    for path in (config_tree.main, config_tree.pkey, config_tree.users):
        assert blobs(path)
        for blob in blobs(path):
            new_ek.decrypt(blob)
            with pytest.raises(InvalidToken):
                ek.decrypt(blob)
    # whole files (patches directory auto-discovered, extra given manually)
    assert new_ek.decrypt(config_tree.mapping.read_text()) == {
        "drac": {"soumye": "2022-02-04"}
    }
    assert new_ek.decrypt(config_tree.extra.read_text()) == {"secret": "extra-value"}
    # comments and non-secret content are preserved
    assert "# main config" in config_tree.main.read_text()
    assert "# the ssh key password" in config_tree.pkey.read_text()
    assert "client_id: plain-id" in config_tree.users.read_text()


def test_encrypt_rotate_dry_run_changes_nothing(cli_main, config_tree):
    before = {path: path.read_text() for path in config_tree.all}

    assert (
        cli_main(
            [
                "encrypt",
                "rotate",
                "--file",
                str(config_tree.main),
                "--extra",
                str(config_tree.extra),
                "--new_password",
                NEW_PASSWORD,
                "--dry_run",
            ]
        )
        == 0
    )

    for path, content in before.items():
        assert path.read_text() == content


def test_encrypt_rotate_wrong_current_password_aborts(
    cli_main, config_tree, monkeypatch, caplog
):
    monkeypatch.setenv("SERIEUX_PASSWORD", "not-the-password")
    before = {path: path.read_text() for path in config_tree.all}

    with caplog.at_level(logging.ERROR):
        result = cli_main(
            [
                "encrypt",
                "rotate",
                "--file",
                str(config_tree.main),
                "--new_password",
                NEW_PASSWORD,
            ]
        )

    assert result == -1
    assert "wrong current password" in caplog.text
    for path, content in before.items():
        assert path.read_text() == content


def test_encrypt_rotate_unencrypted_extra_aborts_before_writing(
    cli_main, config_tree, tmp_path, caplog
):
    unencrypted = tmp_path / "plain.json"
    unencrypted.write_text('{"not": "encrypted"}')
    main_before = config_tree.main.read_text()

    with caplog.at_level(logging.ERROR):
        result = cli_main(
            [
                "encrypt",
                "rotate",
                "--file",
                str(config_tree.main),
                "--extra",
                str(unencrypted),
                "--new_password",
                NEW_PASSWORD,
            ]
        )

    assert result == -1
    assert "Could not re-encrypt" in caplog.text
    assert config_tree.main.read_text() == main_before


def test_encrypt_rotate_defaults_to_sarc_config_env(cli_main, config_tree, monkeypatch):
    monkeypatch.setenv("SARC_CONFIG", str(config_tree.main))

    assert cli_main(["encrypt", "rotate", "--new_password", NEW_PASSWORD]) == 0

    new_ek = EncryptionKey(NEW_PASSWORD)
    for blob in blobs(config_tree.pkey):
        new_ek.decrypt(blob)


# --- encrypt rotate with multiple roots ---


@pytest.fixture
def multi_root(config_tree, ek):
    """Adds a second `sarc:`-wrapped root next to the main one, sharing its
    `users.yaml` subfile and holding one encrypted value of its own."""
    (config_tree.main.parent / "second_pkey.yaml").write_text(
        f'file: id_sec\npassword: "{ek.encrypt("second-pkey-password")}"\n'
    )
    second = config_tree.main.parent / "second.yaml"
    second.write_text(
        "sarc:\n"
        "  db:\n"
        "    host: localhost\n"
        "    name: db\n"
        "  patches: patches\n"
        "  users: users.yaml\n"
        "  clusters:\n"
        "    testc:\n"
        "      host: login.example.com\n"
        "      user_domain: test\n"
        "      private_key: second_pkey.yaml\n"
    )
    return SimpleNamespace(
        second=second,
        pkey2=config_tree.main.parent / "second_pkey.yaml",
        all=[*config_tree.all, second, config_tree.main.parent / "second_pkey.yaml"],
    )


def test_encrypt_rotate_multi_root_shared_file_rotated_once(
    cli_main, multi_root, config_tree, ek
):
    assert (
        cli_main(
            [
                "encrypt",
                "rotate",
                "--file",
                str(config_tree.main),
                "--new_password",
                NEW_PASSWORD,
            ]
        )
        == 0
    )

    new_ek = EncryptionKey(NEW_PASSWORD)
    # both roots and all subfiles, including the shared one, are on the new password
    for path in (
        config_tree.main,
        config_tree.pkey,
        config_tree.users,
        multi_root.pkey2,
    ):
        assert blobs(path)
        for blob in blobs(path):
            new_ek.decrypt(blob)
            with pytest.raises(InvalidToken):
                ek.decrypt(blob)


def test_encrypt_rotate_multi_root_warns_on_broken_secondary(
    cli_main, multi_root, config_tree, ek, caplog
):
    # a root whose config references a nonexistent subfile cannot be loaded
    broken = config_tree.main.parent / "broken.yaml"
    broken.write_text(
        "sarc:\n"
        "  db:\n"
        "    host: localhost\n"
        "    name: db\n"
        "  patches: patches\n"
        "  clusters:\n"
        "    testc:\n"
        "      host: login.example.com\n"
        "      user_domain: test\n"
        "      private_key: does_not_exist.yaml\n"
    )

    with caplog.at_level(logging.WARNING):
        result = cli_main(
            [
                "encrypt",
                "rotate",
                "--file",
                str(config_tree.main),
                "--new_password",
                NEW_PASSWORD,
            ]
        )

    assert result == 0
    assert "skipping" in caplog.text
    new_ek = EncryptionKey(NEW_PASSWORD)
    for blob in blobs(config_tree.users):  # the shared file was still rotated
        new_ek.decrypt(blob)


def test_encrypt_rotate_single_root_skips_other_roots(
    cli_main, multi_root, config_tree, ek
):
    assert (
        cli_main(
            [
                "encrypt",
                "rotate",
                "--file",
                str(config_tree.main),
                "--single_root",
                "--new_password",
                NEW_PASSWORD,
            ]
        )
        == 0
    )

    # the main root and its subfiles are rotated...
    new_ek = EncryptionKey(NEW_PASSWORD)
    for path in (config_tree.main, config_tree.pkey, config_tree.users):
        for blob in blobs(path):
            new_ek.decrypt(blob)
    # ...but the other root and its private file are not
    for blob in blobs(multi_root.pkey2):
        ek.decrypt(blob)


def test_encrypt_rotate_identical_files_at_same_offset_both_rotate(
    cli_main, config_tree, tmp_path, ek
):
    # Two identical files put their encrypted value at the exact same character
    # offset; patches must not overwrite each other (they are keyed per file).
    (tmp_path / "cluster2").mkdir()
    twin = tmp_path / "cluster2" / "pkey.yaml"
    twin.write_text(config_tree.pkey.read_text())
    main = config_tree.main
    main.write_text(
        main.read_text().replace(
            "    testc:\n",
            "    testc2:\n"
            "      host: login2.example.com\n"
            "      user_domain: test2\n"
            "      private_key: cluster2/pkey.yaml\n"
            "    testc:\n",
        )
    )

    assert (
        cli_main(
            [
                "encrypt",
                "rotate",
                "--file",
                str(main),
                "--extra",
                str(config_tree.extra),
                "--new_password",
                NEW_PASSWORD,
            ]
        )
        == 0
    )

    new_ek = EncryptionKey(NEW_PASSWORD)
    for path in (config_tree.pkey, twin):
        assert blobs(path)
        for blob in blobs(path):
            new_ek.decrypt(blob)
