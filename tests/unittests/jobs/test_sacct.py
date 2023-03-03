from datetime import datetime

import pytest
from fabric.testing.base import Command, MockRemote, Session

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
        cmd="sacct  -X -S '2023-02-28T00:00' -E '2023-03-01T00:00' --json",
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
                "sacct  -X -S '2023-02-28T00:00' -E '2023-03-01T00:00' --json",
                out=b"{}",
            ),
            Command(
                "sacct  -X -S '2023-02-28T00:00' -E '2023-03-01T00:00' --json",
                out=b'{ "value": 2 }',
            ),
        ]
    )
    assert scraper.fetch_raw() == {}
    assert scraper.fetch_raw() == {"value": 2}
