import shutil
import subprocess

import pytest

from sarc.config import config


def mock_shutil_which_none(*args, **kwargs):
    return None


def mock_shutil_which_valid(*args, **kwargs):
    return "mongodump"


def _setup_logging_do_nothing(*args, **kwargs):
    pass


@pytest.mark.usefixtures("empty_read_write_db", "isolated_cache")
def test_check_mongodump(cli_main, monkeypatch, caplog):
    monkeypatch.setattr("sarc.cli.setupLogging", _setup_logging_do_nothing)
    monkeypatch.setattr(shutil, "which", mock_shutil_which_none)
    assert cli_main(["db", "backup"]) == -1

    assert "Cannot find executable mongodump in environment paths" in caplog.text


@pytest.mark.usefixtures("empty_read_write_db", "isolated_cache")
@pytest.mark.freeze_time("2023-02-28")
def test_db_backup(cli_main, monkeypatch):
    """Only test if mongodump is called with expected arguments."""
    cfg = config()

    def mock_subprocess_run(command, *args, **kwargs):
        assert command == [
            "mongodump",
            "--gzip",
            f"--uri={cfg.mongo.connection_string}",
            f"--db={cfg.mongo.database_name}",
            f"--out={cfg.cache}/backup/2023-02-28T00h00m00s",
        ]
        mock_subprocess_run.called += 1
        return subprocess.CompletedProcess(
            args=args, returncode=0, stdout="", stderr=""
        )

    mock_subprocess_run.called = 0

    monkeypatch.setattr(shutil, "which", mock_shutil_which_valid)
    monkeypatch.setattr(subprocess, "run", mock_subprocess_run)
    assert cli_main(["db", "backup"]) == 0
    assert mock_subprocess_run.called == 1


@pytest.mark.usefixtures("empty_read_write_db", "isolated_cache")
@pytest.mark.freeze_time("2023-02-28")
def test_db_backup_explicit_folder(cli_main, monkeypatch, tmp_path):
    cfg = config()

    def mock_subprocess_run(command, *args, **kwargs):
        assert command == [
            "mongodump",
            "--gzip",
            f"--uri={cfg.mongo.connection_string}",
            f"--db={cfg.mongo.database_name}",
            f"--out={tmp_path}",
        ]
        mock_subprocess_run.called += 1
        return subprocess.CompletedProcess(
            args=args, returncode=0, stdout="", stderr=""
        )

    mock_subprocess_run.called = 0

    monkeypatch.setattr(shutil, "which", mock_shutil_which_valid)
    monkeypatch.setattr(subprocess, "run", mock_subprocess_run)
    assert cli_main(["db", "backup", "-o", str(tmp_path)]) == 0
    assert mock_subprocess_run.called == 1
