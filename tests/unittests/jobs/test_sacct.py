from datetime import date, datetime, timedelta
from os.path import isfile

import pytest
from fabric.testing.base import Command, MockRemote, Session

from sarc.config import config
from sarc.jobs.sacct import SAcctScraper


@pytest.mark.parametrize(
    "test_config", [{"clusters": {"test": {"host": "patate"}}}], indirect=True
)
def test_SAcctScraper_fetch_raw(test_config, remote):
    scraper = SAcctScraper(
        cluster=test_config.clusters["test"],
        day=datetime(2023, 2, 28),
    )
    channel = remote.expect(
        host="patate",
        cmd="sacct  -X -S 2023-02-28T00:00 -E 2023-03-01T00:00 --allusers --json",
        out=b"{}",
    )
    assert scraper.fetch_raw() == {}


@pytest.mark.parametrize("test_config", [{"clusters": {"test": {}}}], indirect=True)
def test_SAcctScraper_fetch_raw2(test_config, remote):
    scraper = SAcctScraper(
        cluster=test_config.clusters["test"],
        day=datetime(2023, 2, 28),
    )
    channel = remote.expect(
        commands=[
            Command(
                "sacct  -X -S 2023-02-28T00:00 -E 2023-03-01T00:00 --allusers --json",
                out=b"{}",
            ),
            Command(
                "sacct  -X -S 2023-02-28T00:00 -E 2023-03-01T00:00 --allusers --json",
                out=b'{ "value": 2 }',
            ),
        ]
    )
    assert scraper.fetch_raw() == {}
    assert scraper.fetch_raw() == {"value": 2}


@pytest.mark.parametrize(
    "test_config", [{"clusters": {"test": {"host": "patate"}}}], indirect=True
)
@pytest.mark.freeze_time("2023-02-28")
def test_SAcctScraper_get_cache(test_config, remote):
    today = datetime.combine(date.today(), datetime.min.time())
    yesterday = today - timedelta(days=1)
    tomorrow = today + timedelta(days=1)

    scraper_today = SAcctScraper(
        cluster=test_config.clusters["test"],
        day=today,
    )
    scraper_yesterday = SAcctScraper(
        cluster=test_config.clusters["test"],
        day=yesterday,
    )

    # we ask for yesterday, today and tomorrow
    fmt = "%Y-%m-%dT%H:%M"
    channel = remote.expect(
        commands=[
            Command(
                f"sacct  -X -S {yesterday.strftime(fmt)} -E {today.strftime(fmt)} --allusers --json",
                out=b'{"value": 2}',
            ),
            Command(
                f"sacct  -X -S {today.strftime(fmt)} -E {tomorrow.strftime(fmt)} --allusers --json",
                out=b'{"value": 2}',
            ),
        ]
    )

    cachedir = config().cache
    cachedir = cachedir / "sacct"
    cachefile = cachedir / f"test.{yesterday.strftime('%Y-%m-%d')}.json"
    assert not isfile(cachefile)
    scraper_yesterday.get_raw()
    assert isfile(cachefile)

    cachedir = config().cache
    cachedir = cachedir / "sacct"
    cachefile = cachedir / f"test.{today.strftime('%Y-%m-%d')}.json"
    assert not isfile(cachefile)
    scraper_today.get_raw()
    assert not isfile(cachefile)
