from datetime import datetime

import pytest

from sarc.client.job import count_jobs, get_job, get_jobs
from sarc.config import MTL, config

parameters = {
    "no_cluster": {},
    "cluster_str": {"cluster": "patate"},
    "job_state": {"job_state": "COMPLETED"},
    "one_job": {"job_id": 10},
    "one_job_wrong_cluster": {"job_id": 10, "cluster": "patate"},
    "many_jobs": {"job_id": [8, 9]},
    "no_jobs": {"job_id": []},
    "start_only": {"start": datetime(2023, 2, 19, tzinfo=MTL)},
    "end_only": {"end": datetime(2023, 2, 16, tzinfo=MTL)},
    "start_str_only": {"start": "2023-02-19"},
    "end_str_only": {"end": "2023-02-16"},
    "start_and_end": {
        "start": datetime(2023, 2, 15, tzinfo=MTL),
        "end": datetime(2023, 2, 18, tzinfo=MTL),
    },
    "user": {"user": "beaubonhomme"},
    "resubmitted": {"job_id": 1_000_000},
}


@pytest.mark.usefixtures("read_only_db_client", "tzlocal_is_mtl")
@pytest.mark.parametrize("params", parameters.values(), ids=parameters.keys())
def test_count_jobs(params, file_regression):
    jobs = list(get_jobs(**params))
    assert len(jobs) == count_jobs(**params)
    file_regression.check(
        f"Found {len(jobs)} job(s):\n"
        + "\n".join([job.json(exclude={"id": True}, indent=4) for job in jobs])
    )


@pytest.mark.usefixtures("read_only_db", "tzlocal_is_mtl")
def test_count_jobs_cluster_cfg(file_regression):
    jobs = list(get_jobs(cluster=config().clusters["fromage"]))
    assert len(jobs) == count_jobs(cluster=config().clusters["fromage"])
    file_regression.check(
        f"Found {len(jobs)} job(s):\n"
        + "\n".join([job.json(exclude={"id": True}, indent=4) for job in jobs])
    )


@pytest.mark.usefixtures("read_only_db_client", "tzlocal_is_mtl")
def test_count_jobs_wrong_job_id():
    with pytest.raises(TypeError, match="job_id must be an int or a list of ints"):
        count_jobs(job_id="wrong id")


@pytest.mark.usefixtures("read_only_db_client", "tzlocal_is_mtl")
def test_count_job():
    jbs = list(get_jobs(cluster="patate"))
    assert len(jbs) == count_jobs(cluster="patate")
    jb = get_job(cluster="patate")
    assert jb in jbs


@pytest.mark.usefixtures("read_only_db_client", "tzlocal_is_mtl")
def test_get_job_resubmitted():
    assert count_jobs(job_id=1_000_000) == 2
    jb1, jb2 = get_jobs(job_id=1_000_000)
    jb = get_job(job_id=1_000_000)

    assert jb is not None
    assert jb1.submit_time != jb2.submit_time
    assert jb.submit_time == max(jb1.submit_time, jb2.submit_time)
