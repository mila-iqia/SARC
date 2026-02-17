import logging
import re

import pytest

from sarc.alerts.usage_alerts.disk_space import (
    check_disk_space_for_db,
    check_disk_space_for_cache,
    _compute_db_disk_usage,
    _get_human_readable_file_size,
    _get_physical_size,
)
from sarc.config import config


@pytest.mark.usefixtures("read_only_db")
def test_alert_disk_space_db(caplog):
    db_size_bytes = _compute_db_disk_usage()
    assert db_size_bytes > 40_000
    db_size_str = _get_human_readable_file_size(db_size_bytes)

    with caplog.at_level(logging.INFO):
        check_disk_space_for_db(max_size_bytes=0)
        assert re.search(r"WARNING +.+\[mongodb] size exceeded", caplog.text)
        assert (
            f"[mongodb] size exceeded: max 0.00 B, current: {db_size_str}"
            in caplog.text
        )
        caplog.clear()

    with caplog.at_level(logging.INFO):
        limit_bytes = db_size_bytes - 1
        limit_str = _get_human_readable_file_size(limit_bytes)
        check_disk_space_for_db(max_size_bytes=limit_bytes)
        assert re.search(r"WARNING +.+\[mongodb] size exceeded", caplog.text)
        assert (
            f"[mongodb] size exceeded: max {limit_str}, current: {db_size_str}"
            in caplog.text
        )
        caplog.clear()

    with caplog.at_level(logging.INFO):
        check_disk_space_for_db(max_size_bytes=db_size_bytes)
        assert not re.search(r"WARNING +.+\[mongodb] size exceeded", caplog.text)
        caplog.clear()

    with caplog.at_level(logging.INFO):
        check_disk_space_for_db(max_size_bytes=db_size_bytes + 1)
        assert not re.search(r"WARNING +.+\[mongodb] size exceeded", caplog.text)


@pytest.mark.usefixtures("enabled_cache")
def test_alert_disk_space_cache(caplog):
    cache = config().cache
    assert _get_physical_size(cache) == 0

    with caplog.at_level(logging.INFO):
        check_disk_space_for_cache(max_size_bytes=0)
        assert not re.search(r"WARNING +.+\[sarc-cache] size exceeded", caplog.text)
        caplog.clear()

    sizes = (3 * 1024**2, 100 * 1025)
    cache.mkdir()
    for i, size in enumerate(sizes):
        with open(cache / f"{i}.txt", mode="w", encoding="utf-8") as f:
            f.write(" " * size)

    cache_size_bytes = _get_physical_size(cache)
    expected_size = sum(sizes)
    assert cache_size_bytes == expected_size
    expected_str = _get_human_readable_file_size(expected_size)

    with caplog.at_level(logging.INFO):
        check_disk_space_for_cache(max_size_bytes=0)
        assert re.search(r"WARNING +.+\[sarc-cache] size exceeded", caplog.text)
        assert (
            f"[sarc-cache] size exceeded: max 0.00 B, current: {expected_str}"
            in caplog.text
        )
        caplog.clear()

    with caplog.at_level(logging.INFO):
        limit_bytes = cache_size_bytes - 1
        limit_str = _get_human_readable_file_size(limit_bytes)
        check_disk_space_for_cache(max_size_bytes=limit_bytes)
        assert re.search(r"WARNING +.+\[sarc-cache] size exceeded", caplog.text)
        assert (
            f"[sarc-cache] size exceeded: max {limit_str}, current: {expected_str}"
            in caplog.text
        )
        caplog.clear()

    with caplog.at_level(logging.INFO):
        check_disk_space_for_cache(max_size_bytes=cache_size_bytes)
        assert not re.search(r"WARNING +.+\[sarc-cache] size exceeded", caplog.text)
        caplog.clear()

    with caplog.at_level(logging.INFO):
        check_disk_space_for_cache(max_size_bytes=cache_size_bytes + 1)
        assert not re.search(r"WARNING +.+\[sarc-cache] size exceeded", caplog.text)
        caplog.clear()
