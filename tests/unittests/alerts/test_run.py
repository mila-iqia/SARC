import hashlib
import logging
import re

import gifnoc
import pytest

from sarc.config import config


@pytest.fixture(scope="function")
def empty_read_write_db(request):
    m = hashlib.md5()
    m.update(request.node.nodeid.encode())
    db_name = f"test-db-{m.hexdigest()}"
    with gifnoc.overlay({"sarc.mongo.database_name": db_name}):
        assert config().mongo.database_instance.name == db_name
        db = config().mongo.database_instance
        for collection_name in db.list_collection_names():
            db[collection_name].drop()
        yield db_name


@pytest.mark.usefixtures("empty_read_write_db")
def test_run_no_args_error(beans_config, cli_main, caplog):
    assert cli_main(["health", "run"]) == -1
    assert "No health checks to run" in caplog.text


@pytest.mark.usefixtures("empty_read_write_db")
def test_run_all_and_checks_error(beans_config, cli_main, caplog):
    assert cli_main(["health", "run", "--all", "--check", "many_beans"]) == -1
    assert "Arguments mutually exclusive" in caplog.text


@pytest.mark.usefixtures("empty_read_write_db")
def test_run_specific_check(beans_config, cli_main, caplog):
    with caplog.at_level(logging.INFO):
        assert cli_main(["health", "run", "--check", "many_beans"]) == 0
        assert re.search(r"INFO +.+\[many_beans] OK", caplog.text)
        assert re.search(
            r"INFO +.+Check complete: 1 checks run, 0 skipped", caplog.text
        )


@pytest.mark.usefixtures("empty_read_write_db")
def test_run_all_checks(beans_config, cli_main, caplog):
    with caplog.at_level(logging.DEBUG):
        assert cli_main(["health", "run", "--all"]) == 0
        assert re.search(r"INFO +.+\[many_beans] OK", caplog.text)
        assert re.search(
            r"WARNING +.+\[little_beans] FAILURE: little_beans", caplog.text
        )
        assert re.search(
            r"ERROR +.+\[evil_beans] ERROR: ValueError: What a beastly number",
            caplog.text,
        )
        assert re.search(r"DEBUG +.+Skipping 'sleepy_beans': inactive", caplog.text)
        assert re.search(
            r"INFO +.+Check complete: 3 checks run, 1 skipped", caplog.text
        )


@pytest.mark.usefixtures("empty_read_write_db")
def test_run_check_with_dep(deps_config, cli_main, caplog):
    with caplog.at_level(logging.INFO):
        assert cli_main(["health", "run", "--check", "many_beans"]) == 0
        assert re.search(
            r"WARNING +.+Skipping 'many_beans': dependency 'evil_beans' not OK",
            caplog.text,
        )
        assert re.search(
            r"INFO +.+Check complete: 0 checks run, 1 skipped", caplog.text
        )


@pytest.mark.usefixtures("empty_read_write_db")
def test_run_check_with_param_and_dep(params_config, cli_main, caplog):
    with caplog.at_level(logging.INFO):
        assert cli_main(["health", "run", "--check", "beanz_beta"]) == 0
        assert re.search(
            r"WARNING +.+Skipping 'beanz_beta': dependency 'isbeta_beta' not OK",
            caplog.text,
        )
        assert re.search(
            r"INFO +.+Check complete: 0 checks run, 1 skipped", caplog.text
        )

    with caplog.at_level(logging.INFO):
        assert cli_main(["health", "run", "--check", "isbeta_beta"]) == 0
        assert re.search(r"INFO +.+\[isbeta_beta] OK", caplog.text)
        assert re.search(
            r"INFO +.+Check complete: 1 checks run, 0 skipped", caplog.text
        )

    with caplog.at_level(logging.INFO):
        assert cli_main(["health", "run", "--check", "beanz_beta"]) == 0
        assert re.search(r"INFO +.+\[beanz_beta] OK", caplog.text)
        assert re.search(
            r"INFO +.+Check complete: 1 checks run, 0 skipped", caplog.text
        )


@pytest.mark.usefixtures("empty_read_write_db")
def test_run_check_with_param_and_dep_wrong_order(params_config, cli_main, caplog):
    with caplog.at_level(logging.INFO):
        assert cli_main(["health", "run", "--check", "beanz_beta", "isbeta_beta"]) == 0
        assert re.search(
            r"WARNING +.+Skipping 'beanz_beta': dependency 'isbeta_beta' not OK",
            caplog.text,
        )
        assert re.search(r"INFO +.+\[isbeta_beta] OK", caplog.text)
        assert re.search(
            r"INFO +.+Check complete: 1 checks run, 1 skipped", caplog.text
        )

    with caplog.at_level(logging.INFO):
        assert cli_main(["health", "run", "--check", "beanz_beta", "isbeta_beta"]) == 0
        assert re.search(r"INFO +.+\[beanz_beta] OK", caplog.text)
        assert re.search(r"INFO +.+\[isbeta_beta] OK", caplog.text)
        assert re.search(
            r"INFO +.+Check complete: 2 checks run, 0 skipped", caplog.text
        )
