import logging
import re

import pytest

from sarc.alerts.usage_alerts.disk_space import _compute_db_disk_usage
from sarc.config import config


@pytest.mark.usefixtures("read_write_db")
def test_read_write_size():
    db_size_bytes = _compute_db_disk_usage()
    print("db_size_bytes", db_size_bytes)
    assert db_size_bytes > 0
    assert db_size_bytes <= 1_000_000


@pytest.mark.parametrize(
    "check_name,expected",
    [
        ("db_size_check_0", "[mongodb] size exceeded: max 0.00 B"),
        ("db_size_check_10k", "[mongodb] size exceeded: max 9.77 KiB (10000 B)"),
        ("db_size_check_1m", ""),
    ],
)
@pytest.mark.usefixtures("health_config", "read_write_db")
def test_health_check_disk_space_db(caplog, cli_main, check_name, expected):
    with caplog.at_level(logging.INFO):
        assert (
            cli_main(
                [
                    "health",
                    "run",
                    "--check",
                    check_name,
                ]
            )
            == 0
        )
        assert check_name in caplog.text
        if expected:
            assert re.search(r"ERROR +.+\[mongodb] size exceeded", caplog.text)
            assert expected in caplog.text
        else:
            assert not re.search(r"ERROR +.+\[mongodb] size exceeded", caplog.text)


@pytest.mark.parametrize(
    "check_name,expected",
    [
        ("cache_size_check_0", "[sarc-cache] size exceeded: max 0.00 B"),
        ("cache_size_check_10k", "[sarc-cache] size exceeded: max 9.77 KiB (10000 B)"),
        ("cache_size_check_1m", ""),
    ],
)
@pytest.mark.usefixtures("health_config", "enabled_cache", "read_write_db")
def test_health_check_disk_space_cache(caplog, cli_main, check_name, expected):
    cache = config().cache
    assert cache
    sizes = (499_999, 500_000)
    cache.mkdir()
    for i, size in enumerate(sizes):
        with open(cache / f"{i}.txt", mode="w", encoding="utf-8") as f:
            f.write(" " * size)
        assert (cache / f"{i}.txt").is_file()

    with caplog.at_level(logging.INFO):
        assert (
            cli_main(
                [
                    "health",
                    "run",
                    "--check",
                    check_name,
                ]
            )
            == 0
        )
        assert check_name in caplog.text
        if expected:
            assert re.search(r"ERROR +.+\[sarc-cache] size exceeded", caplog.text)
            assert expected in caplog.text
        else:
            assert not re.search(r"ERROR +.+\[sarc-cache] size exceeded", caplog.text)
