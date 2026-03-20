"""
Test alert function `check_old_running_jobs`.

In testing DB, there is 1 RUNNING job (job_id=7):
    cluster_name: raisin
    submit_time:  ~2023-02-15T17:00:00+00:00
    start_time:   ~2023-02-15T17:01:00+00:00
    time_limit:   43200 (12 hours)
    end_time:     None

With MOCK_TIME = 2023-11-22T00:00:00+00:00, this job is well past its
maximum allowed end time (~2023-02-16T05:01:00+00:00).
No re-submitted entry exists for job_id=7.
"""

import re
from datetime import datetime, timedelta

import pytest
import time_machine

from sarc.client import get_jobs
from sarc.client.job import SlurmState
from tests.functional.jobs.test_func_load_job_series import MOCK_TIME

PARAMETERS: dict[str, tuple[str, list[str]]] = {
    # No `since` filter: checks all RUNNING jobs => finds old RUNNING job
    "all": (
        "old_running_jobs_all",
        [
            "Found 1 RUNNING job entries which should have already finished, "
            "from which 1 not re-submitted",
            "[old_running_jobs_all] FAILURE: old_running_jobs_all",
        ],
    ),
    # `since` before submit_time => finds old RUNNING job
    "since_before": (
        "old_running_jobs_since_before",
        [
            "Found 1 RUNNING job entries, submitted since 2023-02-15 00:00:00+00:00, "
            "which should have already finished, "
            "from which 1 not re-submitted",
            "[old_running_jobs_since_before] FAILURE: old_running_jobs_since_before",
        ],
    ),
    # `since` after submit_time => does NOT find old RUNNING job
    "since_after": ("old_running_jobs_since_after", []),
}


def _get_error_logs(text: str) -> list:
    """Parse error logs from given text (typically caplog.text)"""
    errors = []
    for line in text.splitlines():
        if line.startswith("ERROR "):
            error_msg = re.sub(r"^ERROR +sarc\..+\.py:[0-9]+ +", "", line.lstrip())
            assert error_msg
            errors.append(error_msg)
    return errors


@time_machine.travel(MOCK_TIME, tick=False)
@pytest.mark.usefixtures("read_only_db", "health_config")
@pytest.mark.parametrize(
    "check_name,expected", PARAMETERS.values(), ids=PARAMETERS.keys()
)
def test_check_old_running_jobs(caplog, cli_main, check_name, expected):
    assert cli_main(["health", "run", "--check", check_name]) == 0
    assert _get_error_logs(caplog.text) == expected


PARAMETERS_WITH_A_JOB_RESUBMITTED: dict[str, tuple[str, list[str]]] = {
    # No `since` filter: checks all RUNNING jobs => finds old RUNNING job
    "all": (
        "old_running_jobs_all",
        [
            "Found 1 RUNNING job entries which should have already finished, "
            "from which 0 not re-submitted, 1 with a latest entry COMPLETED",
            "[old_running_jobs_all] FAILURE: old_running_jobs_all",
        ],
    ),
    # `since` before submit_time => finds old RUNNING job
    "since_before": (
        "old_running_jobs_since_before",
        [
            "Found 1 RUNNING job entries, submitted since 2023-02-15 00:00:00+00:00, "
            "which should have already finished, "
            "from which 0 not re-submitted, 1 with a latest entry COMPLETED",
            "[old_running_jobs_since_before] FAILURE: old_running_jobs_since_before",
        ],
    ),
    # `since` after submit_time => does NOT find old RUNNING job
    "since_after": ("old_running_jobs_since_after", []),
}


@time_machine.travel(MOCK_TIME, tick=False)
@pytest.mark.usefixtures("read_write_db", "health_config")
@pytest.mark.parametrize(
    "check_name,expected",
    PARAMETERS_WITH_A_JOB_RESUBMITTED.values(),
    ids=PARAMETERS.keys(),
)
def test_check_old_running_jobs_with_resubmitted(
    caplog, cli_main, check_name, expected
):
    # Create a re-submitted entry of RUNNING job
    (job,) = get_jobs(job_state=SlurmState.RUNNING)
    job.submit_time += timedelta(hours=1)
    job.job_state = SlurmState.COMPLETED
    job.end_time = datetime.now()
    job.id = None
    job.save()

    assert cli_main(["health", "run", "--check", check_name]) == 0
    assert _get_error_logs(caplog.text) == expected
