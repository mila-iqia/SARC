from datetime import date, datetime, timedelta

import pytest
from fabric.testing.base import Command

from sarc.cache import Cache
from sarc.config import UTC
from sarc.core.scraping.jobs import fetch_jobs
from sarc.core.scraping.jobs_utils import (
    JobConversionError,
    _convert_json_job,
    fetch_raw,
)
from tests.common.dateutils import MTL, _dtfmt


@pytest.mark.usefixtures("no_pkey")
@pytest.mark.parametrize(
    "test_config",
    [
        {
            "clusters": {
                "test": {
                    "host": "patate",
                    "private_key": {"file": "tests/id_test", "password": "12345"},
                }
            }
        }
    ],
    indirect=True,
)
<<<<<<< HEAD
def test_fetch_raw(test_config, remote):
=======
def test_SAcctScraper_fetch_raw(test_config, remote, monkeypatch):
    scraper = SAcctScraper(
        cluster=test_config.clusters["test"],
        start=datetime(2023, 2, 28, tzinfo=MTL).astimezone(UTC),
        end=datetime(2023, 3, 1, tzinfo=MTL).astimezone(UTC),
    )

>>>>>>> 2d05a7f (Convert repeated code to a fixture)
    remote.expect(
        host="patate",
        cmd=f"export TZ=UTC && sacct -X -S {_dtfmt(2023, 2, 28)} -E {_dtfmt(2023, 3, 1)} --allusers --json",
        out=b"{}",
    )
    assert fetch_raw(
        cluster=test_config.clusters["test"],
        start=datetime(2023, 2, 28, tzinfo=MTL).astimezone(UTC),
        end=datetime(2023, 3, 1, tzinfo=MTL).astimezone(UTC),
    ) == "{}".encode("utf-8")


@pytest.mark.usefixtures("no_pkey")
@pytest.mark.parametrize(
    "test_config",
    [
        {
            "clusters": {
                "test": {
                    "host": "test",
                    "private_key": {"file": "tests/id_test", "password": "12345"},
                }
            }
        }
    ],
    indirect=True,
)
<<<<<<< HEAD
def test_fetch_raw2(test_config, remote):
=======
def test_SAcctScraper_fetch_raw2(test_config, remote, monkeypatch):
    scraper = SAcctScraper(
        cluster=test_config.clusters["test"],
        start=datetime(2023, 2, 28, tzinfo=MTL).astimezone(UTC),
        end=datetime(2023, 3, 1, tzinfo=MTL).astimezone(UTC),
    )

>>>>>>> 2d05a7f (Convert repeated code to a fixture)
    remote.expect(
        commands=[
            Command(
                f"export TZ=UTC && sacct -X -S {_dtfmt(2023, 2, 28)} -E {_dtfmt(2023, 3, 1)} --allusers --json",
                out=b"{}",
            ),
            Command(
                f"export TZ=UTC && sacct -X -S {_dtfmt(2023, 2, 28)} -E {_dtfmt(2023, 3, 1)} --allusers --json",
                out=b'{ "value": 2 }',
            ),
        ]
    )
    assert fetch_raw(
        cluster=test_config.clusters["test"],
        start=datetime(2023, 2, 28, tzinfo=MTL).astimezone(UTC),
        end=datetime(2023, 3, 1, tzinfo=MTL).astimezone(UTC),
    ) == "{}".encode("utf-8")
    assert fetch_raw(
        cluster=test_config.clusters["test"],
        start=datetime(2023, 2, 28, tzinfo=MTL).astimezone(UTC),
        end=datetime(2023, 3, 1, tzinfo=MTL).astimezone(UTC),
    ) == '{ "value": 2 }'.encode("utf-8")


@pytest.mark.usefixtures("no_pkey")
@pytest.mark.parametrize(
    "test_config",
    [
        {
            "clusters": {
                "test": {
                    "host": "patate",
                    "private_key": {"file": "tests/id_test", "password": "12345"},
                }
            }
        }
    ],
    indirect=True,
)
@pytest.mark.freeze_time(datetime(2023, 2, 28, tzinfo=MTL))
def test_fetch_jobs_get_cache(test_config, enabled_cache, remote):
    today = datetime.combine(date.today(), datetime.min.time(), tzinfo=MTL).astimezone(
        UTC
    )
    yesterday = today - timedelta(days=1)

    # we ask for yesterday, today and tomorrow
    fmt = "%Y-%m-%dT%H:%M"

    remote.expect(
        commands=[
            Command(
                f"export TZ=UTC && sacct -X -S {yesterday.strftime(fmt)} -E {today.strftime(fmt)} --allusers --json",
                out=b'{"value": 2}',
            ),
        ]
    )

    fetch_jobs(
        ["test"],
        test_config.clusters,
        [f"{yesterday.strftime(fmt)}-{today.strftime(fmt)}"],
        None,
    )

    # Retrieve from the cache
    cache = Cache(subdirectory="jobs")
    cache_entries = list(cache.read_from(from_time=yesterday))
    assert len(cache_entries) == 1
    items = list(cache_entries[0].items())
    assert len(items) == 1
    key, value = items[0]
    assert key == f"test_{yesterday.strftime(fmt)}_{today.strftime(fmt)}"
    assert value == b'{"value": 2}'


@pytest.mark.parametrize(
    "test_config",
    [
        {
            "clusters": {
                "test": {
                    "host": "test",
                    "private_key": {"file": "tests/id_test", "password": "12345"},
                }
            }
        }
    ],
    indirect=True,
)
def test_convert_version_supported(test_config, monkeypatch, caplog):
    version_supported = {"major": "24", "micro": "1", "minor": "11"}
    version_unsupported = {"major": "124", "micro": "1", "minor": "11"}

    entry = {
        "job_id": 123456,
        "array": {
            "job_id": 0,
            "limits": {"max": {"running": {"tasks": 0}}},
            "task_id": {"set": False, "infinite": False, "number": 0},
            "task": "",
        },
        "name": "my_job_name",
        "user": "toto",
        "group": "toto_group",
        "account": "toto_account",
        "state": {"current": ["TIMEOUT"], "reason": "None"},
        "exit_code": {
            "status": ["SUCCESS"],
            "return_code": {"set": True, "infinite": False, "number": 0},
            "signal": {
                "id": {"set": False, "infinite": False, "number": 0},
                "name": "",
            },
        },
        "time": {
            "elapsed": 259223,
            "eligible": 1747064484,
            "end": 1751866893,
            "planned": {"set": True, "infinite": False, "number": 4543186},
            "start": 1751607670,
            "submission": 1747064484,
            "suspended": 0,
            "system": {"seconds": 0, "microseconds": 0},
            "limit": {"set": True, "infinite": False, "number": 4320},
            "total": {"seconds": 0, "microseconds": 0},
            "user": {"seconds": 0, "microseconds": 0},
        },
        "nodes": "node123",
        "partition": "partition123",
        "constraints": "[cascade|milan]",
        "priority": {"set": True, "infinite": False, "number": 489206},
        "qos": "normal",
        "working_directory": "/home/toto/my_job_name",
        "tres": {
            "allocated": [
                {"type": "cpu", "name": "", "id": 1, "count": 8},
                {"type": "mem", "name": "", "id": 2, "count": 16000},
                {"type": "node", "name": "", "id": 4, "count": 1},
                {"type": "billing", "name": "", "id": 5, "count": 8000},
            ],
            "requested": [
                {"type": "cpu", "name": "", "id": 1, "count": 8},
                {"type": "mem", "name": "", "id": 2, "count": 16000},
                {"type": "node", "name": "", "id": 4, "count": 1},
                {"type": "billing", "name": "", "id": 5, "count": 8000},
            ],
        },
        "flags": ["STARTED_ON_BACKFILL", "START_RECEIVED"],
        "cluster": "test",
    }

    # test version supported
    slurmjob = _convert_json_job(entry, "test", version_supported)

    assert slurmjob is not None
    assert slurmjob.job_id == 123456
    assert slurmjob.user == "toto"
    assert slurmjob.group == "toto_group"
    assert slurmjob.account == "toto_account"
    assert slurmjob.partition == "partition123"
    assert slurmjob.job_state == "TIMEOUT"
    assert slurmjob.work_dir == "/home/toto/my_job_name"

    # test version unsupported
    with pytest.raises(JobConversionError):
        slurmjob = _convert_json_job(entry, "test", version_unsupported)
