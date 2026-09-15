import functools
from datetime import timedelta

import pytest
import time_machine
from sqlalchemy import text

from sarc.alerts.usage_alerts.job_series_coherence import (
    DERIVED,
    STATS,
    check_job_series_recent,
    check_job_series_whole,
)
from sarc.db.job import JobStatisticDB
from sarc.db.job_series import CPU_STAT
from tests.functional.common import MOCK_TIME, _get_warnings

get_warnings = functools.partial(
    _get_warnings,
    modules=[
        "sarc.alerts.usage_alerts.job_series_coherence:job_series_coherence.py",
        "sarc.alerts.common:common.py",
    ],
)

# JobStatisticDB requires every quantile field; only `mean` matters here.
STAT_VALUES = {
    "mean": 0.5,
    "std": 0.0,
    "q05": 0.5,
    "q25": 0.5,
    "median": 0.5,
    "q75": 0.5,
    "max": 0.5,
}

# Wide enough that the column comparison covers the whole fixture.
WHOLE_HISTORY = timedelta(days=100_000)


def _scalar(sess, sql: str):
    return sess.exec(text(sql)).one()[0]  # ty: ignore[no-matching-overload]


def _first_job(sess) -> int:
    return _scalar(sess, "SELECT min(job_db_id) FROM job_series")


def _corrupt(sess, sql: str) -> None:
    """Write straight to job_series, which no trigger guards: how drift looks."""
    sess.exec(text(sql))  # ty: ignore[no-matching-overload]
    sess.commit()


@pytest.fixture
def db_with_statistics(read_write_db):
    """The seeded database, plus the statistics the waste columns consume.

    Without them every `*_waste` is NULL and the arithmetic goes untested.
    """
    sess = read_write_db
    gpu_job = _scalar(
        sess,
        "SELECT min(job_db_id) FROM job_series WHERE gpu_type_rgu_drac IS NOT NULL",
    )
    for name in (CPU_STAT, "gpu_sm_occupancy"):
        sess.add(JobStatisticDB(job_id=gpu_job, name=name, **STAT_VALUES))
    sess.commit()
    assert (
        _scalar(
            sess,
            "SELECT count(*) FROM job_series WHERE allocated_gpu_waste IS NOT NULL"
            " AND allocated_cpu_waste IS NOT NULL",
        )
        == 1
    )
    return sess


def test_seeded_db_is_coherent(db_with_statistics, caplog):
    assert check_job_series_whole()
    assert check_job_series_recent(time_interval=WHOLE_HISTORY)
    assert "job_series" not in caplog.text


@pytest.mark.usefixtures("read_write_db")
def test_missing_row_detected(read_write_db, caplog):
    job_db_id = _first_job(read_write_db)
    _corrupt(read_write_db, f"DELETE FROM job_series WHERE job_db_id = {job_db_id}")

    assert not check_job_series_whole()
    assert "1 jobs have no job_series row" in caplog.text
    assert f"job_db_id {job_db_id}" in caplog.text


@pytest.mark.usefixtures("read_write_db")
def test_missing_rgu_detected(read_write_db, caplog):
    _corrupt(
        read_write_db,
        "UPDATE job_series SET gpu_type_rgu_drac = NULL "
        "WHERE gpu_type_rgu_drac IS NOT NULL",
    )

    assert not check_job_series_whole()
    assert "1 jobs have an RGU in job_series but not in slurm_jobs" in caplog.text


@pytest.mark.usefixtures("read_write_db")
def test_compensating_rgu_errors_detected(read_write_db, caplog):
    """Two opposite errors leave the totals equal; comparing the sets does not."""
    gpu_job = _scalar(
        read_write_db,
        "SELECT min(job_db_id) FROM job_series WHERE gpu_type_rgu_drac IS NOT NULL",
    )
    other = _first_job(read_write_db)
    _corrupt(
        read_write_db,
        f"UPDATE job_series SET gpu_type_rgu_drac = NULL WHERE job_db_id = {gpu_job};"
        f"UPDATE job_series SET gpu_type_rgu_drac = 1 WHERE job_db_id = {other}",
    )

    assert not check_job_series_whole()
    assert "2 jobs have an RGU in job_series but not in slurm_jobs" in caplog.text
    assert f"job_db_id {other}, {gpu_job}" in caplog.text


def test_lost_sm_occupancy_detected(db_with_statistics, caplog):
    # The trigger filled the column; drop it as a lost write would.
    _corrupt(db_with_statistics, "UPDATE job_series SET gpu_sm_occupancy_mean = NULL")

    assert not check_job_series_whole()
    assert (
        "1 jobs have a gpu_sm_occupancy in job_series but not in jobstatisticdb"
        in caplog.text
    )


@pytest.mark.usefixtures("read_write_db")
def test_stale_value_detected_only_by_column_comparison(read_write_db, caplog):
    """A wrong-but-filled value is invisible to the whole-table comparison."""
    job_db_id = _first_job(read_write_db)
    _corrupt(
        read_write_db,
        f"UPDATE job_series SET display_name = 'Stale' WHERE job_db_id = {job_db_id}",
    )

    assert check_job_series_whole()

    assert not check_job_series_recent(time_interval=WHOLE_HISTORY)
    assert "1 of the jobs submitted since" in caplog.text
    assert f"job_db_id {job_db_id}" in caplog.text


@pytest.mark.parametrize("column", [*DERIVED, *(column for _, _, column in STATS)])
def test_wrong_value_detected(db_with_statistics, caplog, column):
    _corrupt(
        db_with_statistics,
        f"UPDATE job_series SET {column} = coalesce({column}, 0) + 1",
    )

    assert not check_job_series_recent(time_interval=WHOLE_HISTORY)
    assert "disagree with expected values recomputed from source tables" in caplog.text


@pytest.mark.usefixtures("read_write_db", "health_config")
def test_check_runs_from_config(read_write_db, caplog, cli_main):
    """The check is reachable through the health_monitor config."""
    job_db_id = _first_job(read_write_db)
    _corrupt(read_write_db, f"DELETE FROM job_series WHERE job_db_id = {job_db_id}")

    assert cli_main(["health", "run", "--check", "job_series_whole"]) == 0
    assert "FAILURE" in caplog.text


# Each writes one kind of drift into job_series; most cascade into the other
# check's alert too, which is what an operator would really see. The two
# row-scoped ones take different jobs, so `all` shows both of their alerts.
CORRUPTIONS = {
    "missing_row": "DELETE FROM job_series WHERE job_db_id = {first}",
    "missing_rgu": "UPDATE job_series SET gpu_type_rgu_drac = NULL",
    "lost_sm_occupancy": "UPDATE job_series SET gpu_sm_occupancy_mean = NULL",
    "stale_value": "UPDATE job_series SET display_name = 'Stale' WHERE job_db_id = {second}",
}


@time_machine.travel(MOCK_TIME, tick=False)
@pytest.mark.usefixtures("health_config")
@pytest.mark.parametrize("scenario", [*CORRUPTIONS, "all"])
def test_messages(db_with_statistics, caplog, file_regression, cli_main, scenario):
    """Every message the two checks can log, as an operator gets them."""
    first = _first_job(db_with_statistics)
    second = _scalar(
        db_with_statistics,
        f"SELECT min(job_db_id) FROM job_series WHERE job_db_id > {first}",
    )
    statements = (
        list(CORRUPTIONS.values()) if scenario == "all" else [CORRUPTIONS[scenario]]
    )
    for statement in statements:
        _corrupt(db_with_statistics, statement.format(first=first, second=second))

    caplog.clear()
    argv = [
        "health",
        "run",
        "--check",
        "job_series_whole",
        "job_series_recent_365_days",
    ]
    assert cli_main(argv) == 0
    file_regression.check("\n".join(get_warnings(caplog.text)))
