import functools
import re
from datetime import timedelta

import pytest
import sqlmodel
import time_machine

from sarc.config import config
from sarc.db.cluster import SlurmClusterDB
from sarc.db.job import JobStatisticDB, SlurmJobDB
from tests.functional.common import MOCK_TIME, _get_warnings

get_warnings = functools.partial(
    _get_warnings,
    modules=[
        "sarc.alerts.usage_alerts.zero_sm_occupancy_power:zero_sm_occupancy_power.py",
        "sarc.alerts.common:common.py",
    ],
)

_EXAMPLE_IDS = re.compile(r"e\.g\. job_id (\d+)")

# JobStatisticDB requires every quantile field; the check only looks at max.
STAT_FIELDS = ("mean", "std", "q05", "q25", "median", "q75", "max")


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


def _add_stat(sess, job, name, max_value):
    """Add one statistic row for `job`, every quantile at `max_value`."""
    values = {field: max_value for field in STAT_FIELDS}
    sess.add(JobStatisticDB(job_id=job.id, name=name, **values))


def _seed_stats(sess):
    """Seed the anomalies described in health-test.yaml on the fixture jobs.

    Also pins the seeded jobs' `submit_time`: the fixture jobs are from
    February 2023, so without this the windowed checks could not tell a job
    outside the window from a job that was never touched.
    """
    raisin = _jobs_of(sess, "raisin")
    fromage = _jobs_of(sess, "fromage")
    assert len(raisin) >= 6
    recent = MOCK_TIME - timedelta(hours=2)
    old = MOCK_TIME - timedelta(days=8)

    # Anomaly, inside the default 7-day window: 250 W at zero occupancy.
    raisin[0].submit_time = recent
    _add_stat(sess, raisin[0], "gpu_sm_occupancy", 0.0)
    _add_stat(sess, raisin[0], "gpu_power", 250_000.0)

    # Anomaly, outside the default 7-day window: 350 W at zero occupancy.
    raisin[1].submit_time = old
    _add_stat(sess, raisin[1], "gpu_sm_occupancy", 0.0)
    _add_stat(sess, raisin[1], "gpu_power", 350_000.0)

    # Zero occupancy, but under the 100 W floor.
    raisin[2].submit_time = recent
    _add_stat(sess, raisin[2], "gpu_sm_occupancy", 0.0)
    _add_stat(sess, raisin[2], "gpu_power", 99_000.0)

    # High power, but a nonzero occupancy max.
    raisin[3].submit_time = recent
    _add_stat(sess, raisin[3], "gpu_sm_occupancy", 0.25)
    _add_stat(sess, raisin[3], "gpu_power", 200_000.0)

    # Only one of the two statistics each: the anomaly needs both.
    raisin[4].submit_time = recent
    _add_stat(sess, raisin[4], "gpu_sm_occupancy", 0.0)
    raisin[5].submit_time = recent
    _add_stat(sess, raisin[5], "gpu_power", 200_000.0)

    # fromage's anomaly sits exactly on the default 100 W threshold (>=).
    fromage[0].submit_time = recent
    _add_stat(sess, fromage[0], "gpu_sm_occupancy", 0.0)
    _add_stat(sess, fromage[0], "gpu_power", 100_000.0)

    sess.commit()


PARAMS = {
    # dict()
    "default": "zero_sm_occupancy_power_default",
    # dict(time_interval=None)
    "all": "zero_sm_occupancy_power_all",
    # dict(time_interval=None, min_power_watts=300)
    "threshold_300": "zero_sm_occupancy_power_threshold_300",
    # dict(min_power_watts=300)
    "threshold_300_recent": "zero_sm_occupancy_power_threshold_300_recent",
    # dict(time_interval=None, cluster_names=["raisin", "invisible-cluster"])
    "clusters": "zero_sm_occupancy_power_clusters",
    # dict(time_interval=None, report_limit=1)
    "report_limit_1": "zero_sm_occupancy_power_report_limit_1",
    # dict(time_interval=timedelta(0))
    "invalid_interval": "zero_sm_occupancy_power_invalid_interval",
    # dict(min_power_watts=0)
    "invalid_threshold": "zero_sm_occupancy_power_invalid_threshold",
    # dict(report_limit=0)
    "invalid_report_limit": "zero_sm_occupancy_power_invalid_report_limit",
}


@time_machine.travel(MOCK_TIME, tick=False)
@pytest.mark.usefixtures("read_write_db", "health_config")
@pytest.mark.parametrize("check_name", PARAMS.values(), ids=PARAMS.keys())
def test_check_zero_sm_occupancy_power(check_name, caplog, file_regression, cli_main):
    with config.db.session() as sess:
        _seed_stats(sess)
    caplog.clear()
    assert cli_main(["health", "run", "--check", check_name]) == 0
    file_regression.check("\n".join(get_warnings(caplog.text)))


@time_machine.travel(MOCK_TIME, tick=False)
@pytest.mark.usefixtures("read_write_db", "health_config")
def test_examples_are_the_newest_of_their_group(caplog, cli_main):
    """The named job is the group's newest anomaly."""
    with config.db.session() as sess:
        # The newest anomaly is raisin's first job before seeding pins it to the
        # recent time: re-querying after the seed would sort by the new times.
        newest, _ = _jobs_of(sess, "raisin")[:2]
        newest_id = newest.job_id
        _seed_stats(sess)
    caplog.clear()
    assert (
        cli_main(["health", "run", "--check", "zero_sm_occupancy_power_report_limit_1"])
        == 0
    )
    (line,) = [line for line in caplog.text.splitlines() if "[raisin]" in line]
    named = _EXAMPLE_IDS.search(line)
    assert named is not None, line
    assert int(named.group(1)) == newest_id


@time_machine.travel(MOCK_TIME, tick=False)
@pytest.mark.usefixtures("read_write_db", "health_config")
def test_near_misses_are_ignored(caplog, cli_main):
    """Under the power floor, above zero occupancy, or missing one statistic: none is an anomaly.

    raisin's two anomalies are the only jobs the full-window check may name.
    """
    with config.db.session() as sess:
        # Captured before seeding: the seed rewrites submit times, hence the order.
        near_miss_ids = {job.job_id for job in _jobs_of(sess, "raisin")[2:6]}
        _seed_stats(sess)
    caplog.clear()
    assert cli_main(["health", "run", "--check", "zero_sm_occupancy_power_all"]) == 0
    raisin_lines = [line for line in caplog.text.splitlines() if "[raisin]" in line]
    assert len(raisin_lines) == 1
    assert "2 jobs in database have gpu_sm_occupancy.max = 0" in raisin_lines[0]
    for job_id in near_miss_ids:
        assert f"job_id {job_id} (" not in caplog.text


@time_machine.travel(MOCK_TIME, tick=False)
@pytest.mark.usefixtures("read_write_db", "health_config")
def test_window_excludes_the_old_anomaly(caplog, cli_main):
    """The default 7-day window keeps the 8-day-old anomaly out, fromage's stays in."""
    with config.db.session() as sess:
        # Captured before seeding: the seed rewrites submit times, hence the order.
        (old,) = _jobs_of(sess, "raisin")[1:2]
        old_id = old.job_id
        _seed_stats(sess)
    caplog.clear()
    assert (
        cli_main(["health", "run", "--check", "zero_sm_occupancy_power_default"]) == 0
    )
    assert f"job_id {old_id} (" not in caplog.text
    assert "[raisin] 1 job submitted since" in caplog.text


@time_machine.travel(MOCK_TIME, tick=False)
@pytest.mark.usefixtures("read_write_db", "health_config")
def test_unknown_cluster_is_reported(caplog, cli_main):
    """A typo in cluster_names would otherwise narrow the check to nothing, silently."""
    assert (
        cli_main(["health", "run", "--check", "zero_sm_occupancy_power_clusters"]) == 0
    )
    assert "[invisible-cluster] unknown cluster, nothing to check" in caplog.text
