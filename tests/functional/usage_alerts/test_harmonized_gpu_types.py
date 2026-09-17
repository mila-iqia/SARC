import functools
import re

import pytest
import sqlmodel
import time_machine

from sarc.config import config
from sarc.db.cluster import SlurmClusterDB
from sarc.db.job import SlurmJobDB
from tests.functional.common import MOCK_TIME, _get_warnings

get_warnings = functools.partial(
    _get_warnings,
    modules=[
        "sarc.alerts.usage_alerts.harmonized_gpu_types:harmonized_gpu_types.py",
        "sarc.alerts.common:common.py",
    ],
)

_EXAMPLE_IDS = re.compile(r"(e\.g\. job_id )(\d+(?:, \d+)*)")


def _count_examples(line: str) -> str:
    """Replace the example ids with how many there are.

    The check does not order them -- which jobs come back is the planner's choice,
    so only their number is the check's own behaviour to pin down.
    """
    return _EXAMPLE_IDS.sub(
        lambda m: f"{m.group(1)}x{len(m.group(2).split(', '))}", line
    )


# Raw GPU names given to raisin's first GPU jobs, in submit order, as
# (allocated_gpu_type, harmonized_gpu_type). The fixture's other GPU jobs keep
# allocated_gpu_type NULL, so the check ignores them: nothing to harmonize.
SEED = [
    ("a100l", None),
    ("a100l", None),
    ("a100l", None),
    ("v100", None),
    # Known to GpuRguDB, so these two are the harmonized ones.
    ("a100", "A100-SXM4-80GB"),
    ("a100", "A100-SXM4-80GB"),
]


def _gpu_jobs_of(sess, cluster_name):
    """The cluster's GPU jobs, in a stable order."""
    return sess.exec(
        sqlmodel.select(SlurmJobDB)
        .join(
            SlurmClusterDB,
            sqlmodel.col(SlurmJobDB.cluster_id) == sqlmodel.col(SlurmClusterDB.id),
        )
        .where(
            SlurmClusterDB.name == cluster_name,
            sqlmodel.col(SlurmJobDB.allocated_gres_gpu) > 0,
        )
        .order_by(sqlmodel.col(SlurmJobDB.submit_time), sqlmodel.col(SlurmJobDB.job_id))
    ).all()


def _seed_gpu_types(sess):
    """Apply SEED to raisin's first GPU jobs."""
    jobs = _gpu_jobs_of(sess, "raisin")
    assert len(jobs) >= len(SEED)
    for job, (gpu_type, harmonized) in zip(jobs, SEED):
        assert job.allocated_gpu_type is None, job.job_id
        job.allocated_gpu_type = gpu_type
        job.harmonized_gpu_type = harmonized
    sess.commit()


PARAMS = {
    # dict(time_interval=None)
    "all": "harmonized_gpu_types_all",
    # dict()
    "default": "harmonized_gpu_types_default",
    # dict(time_interval=timedelta(days=365))
    "interval_365d": "harmonized_gpu_types_interval_365d",
    # dict(time_interval=None, cluster_names=["raisin", "mila", "invisible-cluster"])
    "clusters": "harmonized_gpu_types_clusters",
    # dict(time_interval=None, report_limit=1)
    "report_limit_1": "harmonized_gpu_types_report_limit_1",
    # dict(time_interval=None, report_limit=20)
    "report_limit_20": "harmonized_gpu_types_report_limit_20",
    # dict(time_interval=timedelta(0))
    "invalid_interval": "harmonized_gpu_types_invalid_interval",
    # dict(report_limit=0)
    "invalid_report_limit": "harmonized_gpu_types_invalid_report_limit",
}


@time_machine.travel(MOCK_TIME, tick=False)
@pytest.mark.usefixtures("read_write_db", "health_config")
@pytest.mark.parametrize("check_name", PARAMS.values(), ids=PARAMS.keys())
def test_check_harmonized_gpu_types(check_name, caplog, file_regression, cli_main):
    with config.db.session() as sess:
        _seed_gpu_types(sess)
    caplog.clear()
    assert cli_main(["health", "run", "--check", check_name]) == 0
    file_regression.check(
        "\n".join(_count_examples(line) for line in get_warnings(caplog.text))
    )


@time_machine.travel(MOCK_TIME, tick=False)
@pytest.mark.usefixtures("read_write_db", "health_config")
def test_jobs_without_raw_name_are_ignored(caplog, cli_main):
    """No `allocated_gpu_type` is sacct, not a gap in `gpus_per_nodes`.

    raisin has 12 such GPU jobs and they are the only GPU jobs fromage and patate
    have; none of them is the harmonization failure this check looks for.
    """
    with config.db.session() as sess:
        _seed_gpu_types(sess)
    caplog.clear()
    assert cli_main(["health", "run", "--check", "harmonized_gpu_types_all"]) == 0
    assert "no allocated_gpu_type" not in caplog.text
    assert "[fromage]" not in caplog.text
    assert "[patate]" not in caplog.text
    assert (
        "[raisin] allocated_gpu_type 'a100l': 3 GPU jobs in database "
        "have no harmonized GPU type" in caplog.text
    )
    assert "FAILURE" in caplog.text


@time_machine.travel(MOCK_TIME, tick=False)
@pytest.mark.usefixtures("read_write_db", "health_config")
def test_examples_belong_to_their_group(caplog, cli_main):
    """Whichever jobs are named, they must be jobs the alert is about."""
    with config.db.session() as sess:
        _seed_gpu_types(sess)
        a100l = {
            job.job_id
            for job in _gpu_jobs_of(sess, "raisin")
            if job.allocated_gpu_type == "a100l"
        }
    caplog.clear()
    assert (
        cli_main(["health", "run", "--check", "harmonized_gpu_types_report_limit_1"])
        == 0
    )
    (line,) = [
        line
        for line in caplog.text.splitlines()
        if "allocated_gpu_type 'a100l'" in line
    ]
    named = _EXAMPLE_IDS.search(line)
    assert named is not None, line
    assert {int(job_id) for job_id in named.group(2).split(", ")} <= a100l


@time_machine.travel(MOCK_TIME, tick=False)
@pytest.mark.usefixtures("read_write_db", "health_config")
def test_cpu_jobs_are_not_counted(caplog, cli_main):
    """mila's only job has allocated_gres_gpu = 0, so mila has nothing to report.

    It is given a raw GPU name here so that only `allocated_gres_gpu` keeps it out.
    """
    with config.db.session() as sess:
        (job,) = sess.exec(
            sqlmodel.select(SlurmJobDB)
            .join(
                SlurmClusterDB,
                sqlmodel.col(SlurmJobDB.cluster_id) == sqlmodel.col(SlurmClusterDB.id),
            )
            .where(SlurmClusterDB.name == "mila")
        ).all()
        assert job.allocated_gres_gpu == 0
        job.allocated_gpu_type = "a100l"
        sess.commit()
    caplog.clear()
    assert cli_main(["health", "run", "--check", "harmonized_gpu_types_clusters"]) == 0
    assert "[mila]" not in caplog.text


@time_machine.travel(MOCK_TIME, tick=False)
@pytest.mark.usefixtures("read_write_db", "health_config")
def test_unknown_cluster_is_reported(caplog, cli_main):
    """A typo in cluster_names would otherwise narrow the check to nothing, silently."""
    assert cli_main(["health", "run", "--check", "harmonized_gpu_types_clusters"]) == 0
    assert "[invisible-cluster] unknown cluster, nothing to check" in caplog.text


@time_machine.travel(MOCK_TIME, tick=False)
@pytest.mark.usefixtures("read_write_db", "health_config")
def test_empty_window_passes(caplog, cli_main):
    """No GPU job submitted in the window is nothing to fault harmonization for."""
    assert cli_main(["health", "run", "--check", "harmonized_gpu_types_default"]) == 0
    assert "[raisin]" not in caplog.text
    assert "FAILURE" not in caplog.text


@time_machine.travel(MOCK_TIME, tick=False)
@pytest.mark.usefixtures("read_write_db", "health_config")
def test_all_harmonized_passes(caplog, cli_main):
    """A cluster whose GPU jobs all have a harmonized name raises no alert."""
    with config.db.session() as sess:
        for job in _gpu_jobs_of(sess, "raisin"):
            job.harmonized_gpu_type = "A100-SXM4-80GB"
        sess.commit()
    caplog.clear()
    assert cli_main(["health", "run", "--check", "harmonized_gpu_types_clusters"]) == 0
    assert "[raisin]" not in caplog.text
    assert "FAILURE" not in caplog.text
