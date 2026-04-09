import logging
import re

import pytest

from sarc.config import config

CHECK_NAME = "temporary_cache_file"


@pytest.mark.usefixtures("health_config", "enabled_cache", "read_write_db")
def test_ok_when_no_current_files(caplog, cli_main):
    cache = config().cache
    cache.mkdir(parents=True)

    with caplog.at_level(logging.INFO):
        assert cli_main(["health", "run", "--check", CHECK_NAME]) == 0
        assert CHECK_NAME in caplog.text
        assert not re.search(r"ERROR .+Found temporary cache file", caplog.text)


@pytest.mark.usefixtures("health_config", "enabled_cache", "read_write_db")
def test_fail_when_current_file_exists(caplog, cli_main):
    cache = config().cache
    cache.mkdir(parents=True)
    (cache / "something.current").touch()

    with caplog.at_level(logging.INFO):
        assert cli_main(["health", "run", "--check", CHECK_NAME]) == 0
        assert CHECK_NAME in caplog.text
        assert re.search(r"ERROR .+Found temporary cache file", caplog.text)
        assert "something.current" in caplog.text


@pytest.mark.usefixtures("health_config", "enabled_cache", "read_write_db")
def test_fail_when_current_file_in_subdirectory(caplog, cli_main):
    cache = config().cache
    subdir = cache / "2024" / "01" / "01"
    subdir.mkdir(parents=True)
    (subdir / "data.current").touch()

    with caplog.at_level(logging.INFO):
        assert cli_main(["health", "run", "--check", CHECK_NAME]) == 0
        assert CHECK_NAME in caplog.text
        assert re.search(r"ERROR .+Found temporary cache file", caplog.text)
        assert "data.current" in caplog.text


@pytest.mark.usefixtures("health_config", "enabled_cache", "read_write_db")
def test_fail_logs_each_file(caplog, cli_main):
    cache = config().cache
    cache.mkdir(parents=True)
    (cache / "a.current").touch()
    (cache / "b.current").touch()

    with caplog.at_level(logging.INFO):
        assert cli_main(["health", "run", "--check", CHECK_NAME]) == 0
        assert re.search(r"ERROR .+Found temporary cache file", caplog.text)
        assert "a.current" in caplog.text
        assert "b.current" in caplog.text


@pytest.mark.usefixtures("health_config", "enabled_cache", "read_write_db")
def test_ok_ignores_non_current_files(caplog, cli_main):
    cache = config().cache
    cache.mkdir(parents=True)
    (cache / "data.json").touch()
    (cache / "data.zip").touch()

    with caplog.at_level(logging.INFO):
        assert cli_main(["health", "run", "--check", CHECK_NAME]) == 0
        assert CHECK_NAME in caplog.text
        assert not re.search(r"ERROR .+Found temporary cache file", caplog.text)


@pytest.mark.usefixtures("health_config", "disabled_cache", "read_write_db")
def test_ok_when_cache_is_none(caplog, cli_main):
    with caplog.at_level(logging.INFO):
        assert cli_main(["health", "run", "--check", CHECK_NAME]) == 0
        assert CHECK_NAME in caplog.text
        assert not re.search(r"ERROR .+Found temporary cache file", caplog.text)
