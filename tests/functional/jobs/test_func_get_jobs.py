from datetime import datetime

import pytest

from sarc.config import MTL, config
from sarc.jobs import get_jobs

print(config().clusters["fromage"])

parameters = {
    "no_cluster": ({}, 20),
    "cluster_str": ({"cluster": "patate"}, 1),
    "cluster_cfg": ({"cluster": config().clusters["fromage"]}, 1),
    "one_job": ({"job_id": 10}, 1),
    "one_job_wrong_cluster": ({"job_id": 10, "cluster": "patate"}, 0),
    "many_jobs": ({"job_id": [8, 9]}, 2),
    "start_only": ({"start": datetime(2023, 2, 19, tzinfo=MTL)}, 4),
    "end_only": ({"end": datetime(2023, 2, 16, tzinfo=MTL)}, 8),
    "start_str_only": ({"start": "2023-02-19"}, 4),
    "end_str_only": ({"end": "2023-02-16"}, 8),
    "start_and_end": (
        {
            "start": datetime(2023, 2, 15, tzinfo=MTL),
            "end": datetime(2023, 2, 18, tzinfo=MTL),
        },
        14,
    ),
    "username": (
        {
            "username": "beaubonhomme",
        },
        1,
    ),
}


@pytest.mark.usefixtures("init_db_with_jobs")
@pytest.mark.parametrize(
    "params,jobs_count", parameters.values(), ids=parameters.keys()
)
def test_get_jobs(params, jobs_count, data_regression):
    jobs = list(get_jobs(**params))
    assert len(jobs) == jobs_count
    data_regression.check([job.json(exclude={"id": True}) for job in jobs])
