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
        # there is only 2 jobs from 1 cluster in 1 timestamp. So, threshold will be 0.
        (
            dict(verbose=True),
            [
                "[raisin] threshold 0 (2.0 - 2 * nan). Only 1 timestamp found. Either time_interval (7 days, 0:00:00) is too short, or this cluster should not be currently checked"
            ],
        ),
        # Check with no time interval (i.e. all jobs).
        (
            dict(time_interval=None, verbose=True),
            [
                "[fromage] threshold 0 (0.125 - 2 * 0.3535533905932738). Either nb_stddev is too high, time_interval (None) is too short, or this cluster should not be currently checked",
                "[mila] threshold 0 (0.375 - 2 * 0.5175491695067657). Either nb_stddev is too high, time_interval (None) is too short, or this cluster should not be currently checked",
                "[patate] threshold 0 (0.25 - 2 * 0.4629100498862757). Either nb_stddev is too high, time_interval (None) is too short, or this cluster should not be currently checked",
                "[raisin][2023-02-20 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required for this cluster: 0.162594636278925 (2.875 - 2 * 1.3562026818605375); time unit: 1 day, 0:00:00",
            ],
        ),
        # Check with a supplementary cluster `another_cluster` which is not in data frame.
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
                verbose=True,
            ),
            [
                "[another_cluster] threshold 0 (0.0 - 2 * 0.0). Either nb_stddev is too high, time_interval (None) is too short, or this cluster should not be currently checked",
                "[fromage] threshold 0 (0.125 - 2 * 0.3535533905932738). Either nb_stddev is too high, time_interval (None) is too short, or this cluster should not be currently checked",
                "[mila] threshold 0 (0.375 - 2 * 0.5175491695067657). Either nb_stddev is too high, time_interval (None) is too short, or this cluster should not be currently checked",
                "[patate] threshold 0 (0.25 - 2 * 0.4629100498862757). Either nb_stddev is too high, time_interval (None) is too short, or this cluster should not be currently checked",
                "[raisin][2023-02-20 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required for this cluster: 0.162594636278925 (2.875 - 2 * 1.3562026818605375); time unit: 1 day, 0:00:00",
            ],
        ),
        # Check above case with 2 clusters ignored.
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
                "[another_cluster] threshold 0 (0.0 - 2 * 0.0). Either nb_stddev is too high, time_interval (None) is too short, or this cluster should not be currently checked",
                # "[fromage] threshold 0 (0.125 - 2 * 0.3535533905932738). Either nb_stddev is too high, time_interval (None) is too short, or this cluster should not be currently checked",
                "[mila] threshold 0 (0.375 - 2 * 0.5175491695067657). Either nb_stddev is too high, time_interval (None) is too short, or this cluster should not be currently checked",
                # "[patate] threshold 0 (0.25 - 2 * 0.4629100498862757). Either nb_stddev is too high, time_interval (None) is too short, or this cluster should not be currently checked",
                "[raisin][2023-02-20 00:01:00-05:00] insufficient cluster scraping: 0 jobs / cluster / time unit; minimum required for this cluster: 0.162594636278925 (2.875 - 2 * 1.3562026818605375); time unit: 1 day, 0:00:00",
            ],
        ),
    ],
)
def test_check_nb_jobs_per_cluster_per_time(params, expected, caplog):
    check_nb_jobs_per_cluster_per_time(**params)
    print(caplog.text)
    assert get_warnings(caplog.text) == expected
