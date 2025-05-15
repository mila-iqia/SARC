"""
Test alert function `check_same_job_id`.

NB:
In testing DB, there are currently 2 jobs with same job ID 1000000
First job:
submit_time: 2023-02-19 05:00:00+00:00
end_time:    2023-02-19 17:01:00+00:00

Second job:
submit_time: 2023-02-19 11:00:00+00:00
end_time:    2023-02-19 23:01:00+00:00
"""

import re
from datetime import datetime, timedelta

import pytest

from sarc.alerts.usage_alerts.same_job_id import check_same_job_id
from sarc.config import MTL, UTC
from tests.functional.jobs.test_func_load_job_series import MOCK_TIME

PARAMETERS = {
    # only 7 days before now (which is MOCK_TIME), cannot see duplicates
    "default": {},
    # all jobs in testing DB are covered
    "all": {"since": None, "time_interval": None},
    # `since` covers both jobs
    "since_before": {
        "since": datetime(2023, 2, 19, 2, tzinfo=UTC),
        "time_interval": None,
    },
    # `since` covers both jobs
    "since_in": {"since": datetime(2023, 2, 19, 5, tzinfo=UTC), "time_interval": None},
    # `since` covers only 2nd job, cannot detect duplicate
    "since_after_first": {
        "since": datetime(2023, 2, 19, 18, tzinfo=UTC),
        "time_interval": None,
    },
    # `since` after both jobs, cannot detect duplicates
    "since_after_both": {
        "since": datetime(2023, 2, 19, 23, 10, tzinfo=UTC),
        "time_interval": None,
    },
    # [since, since + time_interval] before jobs, cannot detect duplicates
    "interval_before": {
        "since": datetime(2023, 2, 19, 2, tzinfo=UTC),
        "time_interval": timedelta(hours=3),
    },
    # [since, since + time_interval] covers only 1st job, cannot detect duplicate
    "interval_on_first": {
        "since": datetime(2023, 2, 19, 2, tzinfo=UTC),
        "time_interval": timedelta(hours=4),
    },
    # [since, since + time_interval] covers both jobs
    "interval_on_both": {
        "since": datetime(2023, 2, 19, 2, tzinfo=UTC),
        "time_interval": timedelta(hours=10),
    },
    # [since, since + time_interval] covers both jobs beyond end of 2nd job
    "interval_full": {
        "since": datetime(2023, 2, 19, 2, tzinfo=UTC),
        "time_interval": timedelta(hours=22),
    },
    # [since, since + time_interval] after jobs, cannot detect duplicates
    "interval_after": {
        "since": datetime(2023, 2, 19, 23, 10, tzinfo=UTC),
        "time_interval": timedelta(hours=22),
    },
    # time_interval not wide enough from now (which is MOCK_TIME), cannot detect duplicates
    "time_interval_too_short": {"since": None, "time_interval": timedelta(days=275)},
    # time_interval wide enough to see both jobs
    "time_interval_long_enough": {"since": None, "time_interval": timedelta(days=276)},
}


@pytest.mark.freeze_time(MOCK_TIME)
@pytest.mark.usefixtures("read_only_db", "client_mode", "tzlocal_is_mtl")
@pytest.mark.parametrize("params", PARAMETERS.values(), ids=PARAMETERS.keys())
def test_check_same_job_id(params, caplog, file_regression):
    check_same_job_id(**params)
    file_regression.check(
        re.sub(
            r"WARNING +sarc\.alerts\.usage_alerts\.same_job_id:same_job_id.py:[0-9]+ +",
            "",
            caplog.text,
        )
    )
