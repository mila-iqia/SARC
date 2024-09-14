import functools

import pytest

from sarc.alerts.usage_alerts.cluster_scraping import check_nb_jobs_per_cluster_per_time

from ..jobs.test_func_load_job_series import MOCK_TIME
from .common import _get_warnings

get_warnings = functools.partial(
    _get_warnings,
    module="sarc.alerts.usage_alerts.cluster_scraping:cluster_scraping.py",
)


@pytest.mark.freeze_time(MOCK_TIME)
@pytest.mark.usefixtures("read_only_db", "tzlocal_is_mtl")
@pytest.mark.parametrize(
    "params,expected",
    [
        # Check with default params. In last 7 days from now (mock time: 2023-11-22),
        # there is only 2 jobs from 1 cluster in 1 timestamp. So, threshold will be 0
        # and no warning will be printed.
        (dict(), []),
        # Check with no time interval (i.e. all jobs).
        # Only cluster-timestamp with 0 jobs will produce warnings.
        (
            dict(time_interval=None),
            [
                "[fromage][2023-02-14 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.15327969134614228 (0.90625 - 2 * 0.37648515432692886); time unit: 1 day, 0:00:00",
                "[fromage][2023-02-15 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.15327969134614228 (0.90625 - 2 * 0.37648515432692886); time unit: 1 day, 0:00:00",
                "[fromage][2023-02-16 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.15327969134614228 (0.90625 - 2 * 0.37648515432692886); time unit: 1 day, 0:00:00",
                "[fromage][2023-02-18 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.15327969134614228 (0.90625 - 2 * 0.37648515432692886); time unit: 1 day, 0:00:00",
                "[fromage][2023-02-19 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.15327969134614228 (0.90625 - 2 * 0.37648515432692886); time unit: 1 day, 0:00:00",
                "[fromage][2023-02-20 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.15327969134614228 (0.90625 - 2 * 0.37648515432692886); time unit: 1 day, 0:00:00",
                "[fromage][2023-11-21 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.15327969134614228 (0.90625 - 2 * 0.37648515432692886); time unit: 1 day, 0:00:00",
                "[mila][2023-02-14 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.15327969134614228 (0.90625 - 2 * 0.37648515432692886); time unit: 1 day, 0:00:00",
                "[mila][2023-02-15 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.15327969134614228 (0.90625 - 2 * 0.37648515432692886); time unit: 1 day, 0:00:00",
                "[mila][2023-02-16 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.15327969134614228 (0.90625 - 2 * 0.37648515432692886); time unit: 1 day, 0:00:00",
                "[mila][2023-02-17 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.15327969134614228 (0.90625 - 2 * 0.37648515432692886); time unit: 1 day, 0:00:00",
                "[mila][2023-11-21 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.15327969134614228 (0.90625 - 2 * 0.37648515432692886); time unit: 1 day, 0:00:00",
                "[patate][2023-02-14 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.15327969134614228 (0.90625 - 2 * 0.37648515432692886); time unit: 1 day, 0:00:00",
                "[patate][2023-02-15 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.15327969134614228 (0.90625 - 2 * 0.37648515432692886); time unit: 1 day, 0:00:00",
                "[patate][2023-02-16 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.15327969134614228 (0.90625 - 2 * 0.37648515432692886); time unit: 1 day, 0:00:00",
                "[patate][2023-02-19 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.15327969134614228 (0.90625 - 2 * 0.37648515432692886); time unit: 1 day, 0:00:00",
                "[patate][2023-02-20 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.15327969134614228 (0.90625 - 2 * 0.37648515432692886); time unit: 1 day, 0:00:00",
                "[patate][2023-11-21 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.15327969134614228 (0.90625 - 2 * 0.37648515432692886); time unit: 1 day, 0:00:00",
                "[raisin][2023-02-20 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.15327969134614228 (0.90625 - 2 * 0.37648515432692886); time unit: 1 day, 0:00:00",
            ],
        ),
        # Check with a supplementary cluster `another_cluster` which is not in data frame.
        # As there are 1 more clusters, stats (average, std-dev, threshold) will be slightly different,
        # and warnings will also include all cluster-timestamp cases for supplementary cluster.
        (
            dict(
                time_interval=None,
                cluster_names=[
                    "fromage",
                    "mila",
                    "patate",
                    "raisin",
                    "another_cluster",
                ],
            ),
            [
                "[fromage][2023-02-14 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.12262375307691387 (0.7250000000000001 - 2 * 0.3011881234615431); time unit: 1 day, 0:00:00",
                "[fromage][2023-02-15 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.12262375307691387 (0.7250000000000001 - 2 * 0.3011881234615431); time unit: 1 day, 0:00:00",
                "[fromage][2023-02-16 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.12262375307691387 (0.7250000000000001 - 2 * 0.3011881234615431); time unit: 1 day, 0:00:00",
                "[fromage][2023-02-18 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.12262375307691387 (0.7250000000000001 - 2 * 0.3011881234615431); time unit: 1 day, 0:00:00",
                "[fromage][2023-02-19 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.12262375307691387 (0.7250000000000001 - 2 * 0.3011881234615431); time unit: 1 day, 0:00:00",
                "[fromage][2023-02-20 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.12262375307691387 (0.7250000000000001 - 2 * 0.3011881234615431); time unit: 1 day, 0:00:00",
                "[fromage][2023-11-21 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.12262375307691387 (0.7250000000000001 - 2 * 0.3011881234615431); time unit: 1 day, 0:00:00",
                "[mila][2023-02-14 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.12262375307691387 (0.7250000000000001 - 2 * 0.3011881234615431); time unit: 1 day, 0:00:00",
                "[mila][2023-02-15 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.12262375307691387 (0.7250000000000001 - 2 * 0.3011881234615431); time unit: 1 day, 0:00:00",
                "[mila][2023-02-16 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.12262375307691387 (0.7250000000000001 - 2 * 0.3011881234615431); time unit: 1 day, 0:00:00",
                "[mila][2023-02-17 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.12262375307691387 (0.7250000000000001 - 2 * 0.3011881234615431); time unit: 1 day, 0:00:00",
                "[mila][2023-11-21 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.12262375307691387 (0.7250000000000001 - 2 * 0.3011881234615431); time unit: 1 day, 0:00:00",
                "[patate][2023-02-14 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.12262375307691387 (0.7250000000000001 - 2 * 0.3011881234615431); time unit: 1 day, 0:00:00",
                "[patate][2023-02-15 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.12262375307691387 (0.7250000000000001 - 2 * 0.3011881234615431); time unit: 1 day, 0:00:00",
                "[patate][2023-02-16 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.12262375307691387 (0.7250000000000001 - 2 * 0.3011881234615431); time unit: 1 day, 0:00:00",
                "[patate][2023-02-19 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.12262375307691387 (0.7250000000000001 - 2 * 0.3011881234615431); time unit: 1 day, 0:00:00",
                "[patate][2023-02-20 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.12262375307691387 (0.7250000000000001 - 2 * 0.3011881234615431); time unit: 1 day, 0:00:00",
                "[patate][2023-11-21 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.12262375307691387 (0.7250000000000001 - 2 * 0.3011881234615431); time unit: 1 day, 0:00:00",
                "[raisin][2023-02-20 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.12262375307691387 (0.7250000000000001 - 2 * 0.3011881234615431); time unit: 1 day, 0:00:00",
                # warnings for another_cluster
                "[another_cluster][2023-02-14 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.12262375307691387 (0.7250000000000001 - 2 * 0.3011881234615431); time unit: 1 day, 0:00:00",
                "[another_cluster][2023-02-15 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.12262375307691387 (0.7250000000000001 - 2 * 0.3011881234615431); time unit: 1 day, 0:00:00",
                "[another_cluster][2023-02-16 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.12262375307691387 (0.7250000000000001 - 2 * 0.3011881234615431); time unit: 1 day, 0:00:00",
                "[another_cluster][2023-02-17 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.12262375307691387 (0.7250000000000001 - 2 * 0.3011881234615431); time unit: 1 day, 0:00:00",
                "[another_cluster][2023-02-18 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.12262375307691387 (0.7250000000000001 - 2 * 0.3011881234615431); time unit: 1 day, 0:00:00",
                "[another_cluster][2023-02-19 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.12262375307691387 (0.7250000000000001 - 2 * 0.3011881234615431); time unit: 1 day, 0:00:00",
                "[another_cluster][2023-02-20 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.12262375307691387 (0.7250000000000001 - 2 * 0.3011881234615431); time unit: 1 day, 0:00:00",
                "[another_cluster][2023-11-21 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.12262375307691387 (0.7250000000000001 - 2 * 0.3011881234615431); time unit: 1 day, 0:00:00",
            ],
        ),
        # Check above case with 2 clusters ignored.
        # Stats will be a little different again.
        (
            dict(
                time_interval=None,
                cluster_names=[
                    "mila",
                    "raisin",
                    "another_cluster",
                ],
            ),
            [
                "[mila][2023-02-14 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.2288400740511256 (1.0833333333333333 - 2 * 0.4272466296411038); time unit: 1 day, 0:00:00",
                "[mila][2023-02-15 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.2288400740511256 (1.0833333333333333 - 2 * 0.4272466296411038); time unit: 1 day, 0:00:00",
                "[mila][2023-02-16 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.2288400740511256 (1.0833333333333333 - 2 * 0.4272466296411038); time unit: 1 day, 0:00:00",
                "[mila][2023-02-17 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.2288400740511256 (1.0833333333333333 - 2 * 0.4272466296411038); time unit: 1 day, 0:00:00",
                "[mila][2023-11-21 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.2288400740511256 (1.0833333333333333 - 2 * 0.4272466296411038); time unit: 1 day, 0:00:00",
                "[raisin][2023-02-20 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.2288400740511256 (1.0833333333333333 - 2 * 0.4272466296411038); time unit: 1 day, 0:00:00",
                "[another_cluster][2023-02-14 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.2288400740511256 (1.0833333333333333 - 2 * 0.4272466296411038); time unit: 1 day, 0:00:00",
                "[another_cluster][2023-02-15 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.2288400740511256 (1.0833333333333333 - 2 * 0.4272466296411038); time unit: 1 day, 0:00:00",
                "[another_cluster][2023-02-16 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.2288400740511256 (1.0833333333333333 - 2 * 0.4272466296411038); time unit: 1 day, 0:00:00",
                "[another_cluster][2023-02-17 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.2288400740511256 (1.0833333333333333 - 2 * 0.4272466296411038); time unit: 1 day, 0:00:00",
                "[another_cluster][2023-02-18 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.2288400740511256 (1.0833333333333333 - 2 * 0.4272466296411038); time unit: 1 day, 0:00:00",
                "[another_cluster][2023-02-19 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.2288400740511256 (1.0833333333333333 - 2 * 0.4272466296411038); time unit: 1 day, 0:00:00",
                "[another_cluster][2023-02-20 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.2288400740511256 (1.0833333333333333 - 2 * 0.4272466296411038); time unit: 1 day, 0:00:00",
                "[another_cluster][2023-11-21 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required: 0.2288400740511256 (1.0833333333333333 - 2 * 0.4272466296411038); time unit: 1 day, 0:00:00",
            ],
        ),
    ],
)
def test_check_nb_jobs_per_cluster_per_time(params, expected, caplog):
    check_nb_jobs_per_cluster_per_time(**params)
    assert get_warnings(caplog.text) == expected
