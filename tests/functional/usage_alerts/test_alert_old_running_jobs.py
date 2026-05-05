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
from datetime import datetime, timedelta, UTC

import pytest
import time_machine
from sqlmodel import select

from sarc.config import config
from sarc.db.job import SlurmJobDB
from sarc.models.job import SlurmState

MOCK_TIME = datetime(2023, 11, 22, tzinfo=UTC)


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
    with config().db.session() as sess:
        base_job: SlurmJobDB
        (base_job,) = sess.exec(
            select(SlurmJobDB).where(SlurmJobDB.job_state == SlurmState.RUNNING)
        ).all()
        assert base_job.start_time is not None
        job_data = base_job.model_dump()
        job_data["id"] = None
        job_data["submit_time"] = base_job.submit_time + timedelta(hours=1)
        job_data["start_time"] = base_job.start_time + timedelta(hours=1)
        job_data["job_state"] = SlurmState.COMPLETED
        job_data["end_time"] = datetime.now(UTC)
        sess.add(SlurmJobDB.model_validate(job_data))
        sess.commit()

    assert cli_main(["health", "run", "--check", check_name]) == 0
    assert _get_error_logs(caplog.text) == expected
