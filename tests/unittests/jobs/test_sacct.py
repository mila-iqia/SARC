from datetime import date, datetime, timedelta
from os.path import isfile

import pytest
from fabric.testing.base import Command

from sarc.config import config, UTC
from sarc.jobs.sacct import JobConversionError, SAcctScraper
from tests.common.dateutils import MTL, _dtfmt


@pytest.mark.parametrize(
    "test_config", [{"clusters": {"test": {"host": "patate"}}}], indirect=True
)
def test_SAcctScraper_fetch_raw(test_config, remote):
    scraper = SAcctScraper(
        cluster=test_config.clusters["test"],
        start=datetime(2023, 2, 28, tzinfo=MTL).astimezone(UTC),
        end=datetime(2023, 3, 1, tzinfo=MTL).astimezone(UTC),
    )
    remote.expect(
        host="patate",
        cmd=f"export TZ=UTC && sacct -X -S {_dtfmt(2023, 2, 28)} -E {_dtfmt(2023, 3, 1)} --allusers --json",
        out=b"{}",
    )
    assert scraper.fetch_raw() == {}


@pytest.mark.parametrize(
    "test_config", [{"clusters": {"test": {"host": "test"}}}], indirect=True
)
def test_SAcctScraper_fetch_raw2(test_config, remote):
    scraper = SAcctScraper(
        cluster=test_config.clusters["test"],
        start=datetime(2023, 2, 28, tzinfo=MTL).astimezone(UTC),
        end=datetime(2023, 3, 1, tzinfo=MTL).astimezone(UTC),
    )
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
    assert scraper.fetch_raw() == {}
    assert scraper.fetch_raw() == {"value": 2}


@pytest.mark.parametrize(
    "test_config", [{"clusters": {"test": {"host": "patate"}}}], indirect=True
)
@pytest.mark.freeze_time(datetime(2023, 2, 28, tzinfo=MTL))
def test_SAcctScraper_get_cache(test_config, enabled_cache, remote):
    today = datetime.combine(date.today(), datetime.min.time(), tzinfo=MTL).astimezone(
        UTC
    )
    yesterday = today - timedelta(days=1)
    tomorrow = today + timedelta(days=1)

    scraper_today = SAcctScraper(
        cluster=test_config.clusters["test"],
        start=today,
        end=tomorrow,
    )
    scraper_yesterday = SAcctScraper(
        cluster=test_config.clusters["test"],
        start=yesterday,
        end=today,
    )

    # we ask for yesterday, today and tomorrow
    fmt = "%Y-%m-%dT%H:%M"
    remote.expect(
        commands=[
            Command(
                f"export TZ=UTC && sacct -X -S {yesterday.strftime(fmt)} -E {today.strftime(fmt)} --allusers --json",
                out=b'{"value": 2}',
            ),
            Command(
                f"export TZ=UTC && sacct -X -S {today.strftime(fmt)} -E {tomorrow.strftime(fmt)} --allusers --json",
                out=b'{"value": 2}',
            ),
        ]
    )

    cachedir = config().cache
    cachedir = cachedir / "sacct"
    cachefile = (
        cachedir
        / f"test.{yesterday.strftime('%Y-%m-%dT%H:%M')}.{today.strftime('%Y-%m-%dT%H:%M')}.json"
    )
    assert not isfile(cachefile)
    scraper_yesterday.get_raw()
    assert isfile(cachefile)

    cachedir = config().cache
    cachedir = cachedir / "sacct"
    cachefile = (
        cachedir
        / f"test.{today.strftime('%Y-%m-%dT%H:%M')}.{tomorrow.strftime('%Y-%m-%dT%H:%M')}.json"
    )
    assert not isfile(cachefile)
    scraper_today.get_raw()
    assert not isfile(cachefile)


@pytest.mark.parametrize(
    "test_config", [{"clusters": {"test": {"host": "test"}}}], indirect=True
)
def test_SAcctScraper_convert_version_supported(test_config, monkeypatch, caplog):
    scraper = SAcctScraper(
        cluster=test_config.clusters["test"],
        start=datetime(2023, 2, 28, tzinfo=MTL).astimezone(UTC),
        end=datetime(2023, 3, 1, tzinfo=MTL).astimezone(UTC),
    )
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
    slurmjob = scraper.convert(entry, version_supported)

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
        slurmjob = scraper.convert(entry, version_unsupported)
