import shutil
import subprocess

import pytest

from sarc.config import config


def mock_shutil_which_none(*args, **kwargs):
    return None


def mock_shutil_which_valid(*args, **kwargs):
    return "mongorestore"


def _setup_logging_do_nothing(*args, **kwargs):
    pass


@pytest.mark.usefixtures("empty_read_write_db", "isolated_cache")
def test_check_mongorestore(cli_main, tmp_path, monkeypatch, caplog):
    tmp_dump = tmp_path / "backup"
    tmp_dump.mkdir()

    monkeypatch.setattr("sarc.cli.setupLogging", _setup_logging_do_nothing)
    monkeypatch.setattr(shutil, "which", mock_shutil_which_none)
    assert cli_main(["db", "restore", "-i", str(tmp_dump)]) == -1

    assert "Cannot find executable mongorestore in environment paths" in caplog.text


@pytest.mark.usefixtures("empty_read_write_db", "isolated_cache")
@pytest.mark.freeze_time("2023-02-28")
def test_db_restore(cli_main, tmp_path, monkeypatch):
    """Only test if mongorestore is called with expected arguments."""
    tmp_dump = tmp_path / "backup"
    tmp_dump.mkdir()
    inp_db_path = tmp_dump / "mydb"
    inp_db_path.mkdir()

    cfg = config()

    def mock_subprocess_run(command, *args, **kwargs):
        assert command == [
            "mongorestore",
            f"--uri={cfg.mongo.connection_string}",
            f"--dir={inp_db_path}",
            '--nsInclude="mydb.*"',
            '--nsFrom="mydb.*"',
            f'--nsTo="{cfg.mongo.database_name}.*"',
            "--gzip",
        ]
        mock_subprocess_run.called += 1
        return subprocess.CompletedProcess(
            args=args, returncode=0, stdout="", stderr=""
        )

    mock_subprocess_run.called = 0

    monkeypatch.setattr(shutil, "which", mock_shutil_which_valid)
    monkeypatch.setattr(subprocess, "run", mock_subprocess_run)
    assert cli_main(["db", "restore", "-i", str(tmp_dump)]) == 0
    assert mock_subprocess_run.called == 1


@pytest.mark.usefixtures("empty_read_write_db", "isolated_cache")
@pytest.mark.freeze_time("2023-02-28")
def test_db_restore_force(cli_main, tmp_path, monkeypatch):
    tmp_dump = tmp_path / "backup"
    tmp_dump.mkdir()
    inp_db_path = tmp_dump / "mydb"
    inp_db_path.mkdir()

    cfg = config()

    def mock_subprocess_run(command, *args, **kwargs):
        assert command == [
            "mongorestore",
            f"--uri={cfg.mongo.connection_string}",
            f"--dir={inp_db_path}",
            '--nsInclude="mydb.*"',
            '--nsFrom="mydb.*"',
            f'--nsTo="{cfg.mongo.database_name}.*"',
            "--gzip",
            "--drop",
        ]
        mock_subprocess_run.called += 1
        return subprocess.CompletedProcess(
            args=args, returncode=0, stdout="", stderr=""
        )

    mock_subprocess_run.called = 0

    monkeypatch.setattr(shutil, "which", mock_shutil_which_valid)
    monkeypatch.setattr(subprocess, "run", mock_subprocess_run)
    assert cli_main(["db", "restore", "-i", str(tmp_dump), "-f"]) == 0
    assert mock_subprocess_run.called == 1
