import functools
from datetime import timedelta

import pytest
import sqlmodel
import time_machine

from sarc.config import config
from sarc.db.cluster import SlurmClusterDB
from sarc.db.job import JobStatisticDB, JobStatisticsFetchDateDB, SlurmJobDB
from sarc.db.runstate import set_parsed_date
from tests.functional.common import MOCK_TIME, _get_warnings

get_warnings = functools.partial(
    _get_warnings,
    modules=[
        "sarc.alerts.usage_alerts.prometheus_stats_coverage:prometheus_stats_coverage.py",
        "sarc.alerts.common:common.py",
    ],
)

# cluster -> (attempts inside the 1-day window, of which with stats, attempts before it).
# The older raisin attempts all have stats, so a check ignoring the window bounds would
# read 10 of 19 (52.6 %) instead of 5 of 14 (35.7 %) and pass the default threshold.
SEED = {
    "raisin": (14, 5, 5),
    "mila": (1, 1, 0),
    "patate": (1, 1, 0),
    "fromage": (1, 0, 0),
}

# JobStatisticDB requires every quantile field; the check only looks at the row existing.
STAT_VALUES = {
    "mean": 1.0,
    "std": 0.0,
    "q05": 1.0,
    "q25": 1.0,
    "median": 1.0,
    "q75": 1.0,
    "max": 1.0,
}


def _record_attempt(sess, job, fetch_date, *, with_stats):
    """Record one Prometheus fetch attempt for `job`, as `fetch_prometheus` would."""
    stat_id = None
    if with_stats:
        stat = JobStatisticDB(job_id=job.id, name="cpu_utilization", **STAT_VALUES)
        sess.add(stat)
        sess.flush()
        stat_id = stat.id
    sess.add(
        JobStatisticsFetchDateDB(
            job_id=job.id, fetch_date=fetch_date, jobstatistic_id=stat_id
        )
    )


def _jobs_of(sess, cluster_name):
    """The cluster's jobs, in a stable order."""
    return sess.exec(
        sqlmodel.select(SlurmJobDB)
        .join(
            SlurmClusterDB,
            sqlmodel.col(SlurmJobDB.cluster_id) == sqlmodel.col(SlurmClusterDB.id),
        )
        .where(SlurmClusterDB.name == cluster_name)
        .order_by(sqlmodel.col(SlurmJobDB.submit_time), sqlmodel.col(SlurmJobDB.job_id))
    ).all()


def _seed_attempts(sess, parsed_date):
    """Record the fetch attempts described by SEED, and the parsed date they are windowed on."""
    inside = parsed_date - timedelta(hours=12)
    before = parsed_date - timedelta(days=8)
    for cluster_name, (nb_inside, nb_with_stats, nb_before) in SEED.items():
        jobs = _jobs_of(sess, cluster_name)
        assert len(jobs) >= nb_inside + nb_before, cluster_name
        for i, job in enumerate(jobs[: nb_inside + nb_before]):
            recent = i < nb_inside
            _record_attempt(
                sess,
                job,
                inside if recent else before,
                with_stats=i < nb_with_stats or not recent,
            )
    set_parsed_date(sess, "prometheus", parsed_date)
    sess.commit()


PARAMS = {
    # dict()
    "default": "prometheus_stats_coverage_default",
    # dict(min_coverage=0.3)
    "coverage_030": "prometheus_stats_coverage_coverage_030",
    # dict(min_coverage=0.0)
    "coverage_0": "prometheus_stats_coverage_coverage_0",
    # dict(cluster_names=["raisin", "mila", "invisible-cluster"])
    "clusters": "prometheus_stats_coverage_clusters",
    # dict(time_interval=timedelta(days=30))
    "interval_30d": "prometheus_stats_coverage_interval_30d",
    # dict(time_interval=timedelta(seconds=1))
    "interval_1s": "prometheus_stats_coverage_interval_1s",
    # dict(min_coverage=1.5)
    "invalid_coverage": "prometheus_stats_coverage_invalid_coverage",
    # dict(time_interval=timedelta(0))
    "invalid_interval": "prometheus_stats_coverage_invalid_interval",
}


@time_machine.travel(MOCK_TIME, tick=False)
@pytest.mark.usefixtures("read_write_db", "health_config")
@pytest.mark.parametrize("check_name", PARAMS.values(), ids=PARAMS.keys())
def test_check_prometheus_stats_coverage(check_name, caplog, file_regression, cli_main):
    with config.db.session() as sess:
        _seed_attempts(sess, MOCK_TIME)
    caplog.clear()
    assert cli_main(["health", "run", "--check", check_name]) == 0
    file_regression.check("\n".join(get_warnings(caplog.text)))


@time_machine.travel(MOCK_TIME, tick=False)
@pytest.mark.usefixtures("read_write_db", "health_config")
def test_no_parsed_date(caplog, cli_main):
    assert (
        cli_main(["health", "run", "--check", "prometheus_stats_coverage_default"]) == 0
    )
    assert "No parsed date for Prometheus" in caplog.text
    assert "FAILURE" in caplog.text


@time_machine.travel(MOCK_TIME, tick=False)
@pytest.mark.usefixtures("read_write_db", "health_config")
def test_stale_window(caplog, cli_main):
    """A parsed date older than the window still yields a verdict, plus a staleness warning."""
    stale_date = MOCK_TIME - timedelta(days=10)
    with config.db.session() as sess:
        _seed_attempts(sess, stale_date)
    caplog.clear()
    assert (
        cli_main(["health", "run", "--check", "prometheus_stats_coverage_default"]) == 0
    )
    assert f"Latest Prometheus data parsed at {stale_date}" in caplog.text
    assert (
        "[raisin] insufficient Prometheus stats coverage: 5 of 14 jobs" in caplog.text
    )


@time_machine.travel(MOCK_TIME, tick=False)
@pytest.mark.usefixtures("read_write_db", "health_config")
def test_last_parsed_run_included(caplog, cli_main):
    """The run the parsed date itself comes from must be inside the window.

    `fetch_prometheus` stores `datetime.now(UTC)` with its microseconds, while the
    parsed date is read back from the cache entry filename, truncated to the
    millisecond: an upper bound of `<= end` would drop the whole run.
    """
    fetch_date = MOCK_TIME + timedelta(microseconds=419330)
    parsed_date = MOCK_TIME + timedelta(microseconds=419000)
    with config.db.session() as sess:
        for job in _jobs_of(sess, "raisin")[:4]:
            _record_attempt(sess, job, fetch_date, with_stats=True)
        set_parsed_date(sess, "prometheus", parsed_date)
        sess.commit()
    caplog.clear()
    assert (
        cli_main(["health", "run", "--check", "prometheus_stats_coverage_raisin"]) == 0
    )
    assert "[raisin]" not in caplog.text
    assert "FAILURE" not in caplog.text


@time_machine.travel(MOCK_TIME, tick=False)
@pytest.mark.usefixtures("read_write_db", "health_config")
def test_default_clusters_are_those_with_a_prometheus_url(caplog, cli_main):
    """With no cluster_names, the expected set is what `sarc fetch prometheus` scrapes."""
    with config.db.session() as sess:
        set_parsed_date(sess, "prometheus", MOCK_TIME)
        sess.commit()
    caplog.clear()
    assert (
        cli_main(["health", "run", "--check", "prometheus_stats_coverage_default"]) == 0
    )
    for cluster_name in ["fromage", "gerudo", "hyrule", "mila", "patate", "raisin"]:
        assert f"[{cluster_name}] no Prometheus fetch attempt" in caplog.text
    # No prometheus_url, so no attempt is expected and none is missing.
    for cluster_name in ["local", "raisin_no_prometheus"]:
        assert f"[{cluster_name}]" not in caplog.text
