from __future__ import annotations

import copy
from datetime import datetime, timedelta

from sarc.config import MTL, UTC

# elapsed_time = 60 * 60 * 12
# end_time = datetime(2023, 2, 14, 23, 48, 54, tzinfo=MTL).astimezone(UTC)
base_job = {
    "CLEAR_SCHEDULING": True,
    "STARTED_ON_BACKFILL": True,
    "STARTED_ON_SCHEDULE": False,
    "STARTED_ON_SUBMIT": False,
    "account": "mila",
    "allocated": {"billing": 1, "cpu": 4, "gres_gpu": 1, "mem": 49152, "node": 1},
    "array_job_id": None,
    "cluster_name": "raisin",
    "constraints": "x86_64&(48gb|80gb)",
    "elapsed_time": 60 * 60 * 12,
    "end_time": datetime(2023, 2, 14, 23, 48, 54, tzinfo=MTL).astimezone(UTC),
    "exit_code": 0,
    "group": "petitbonhomme",
    "job_id": 2831220,
    "job_state": "CANCELLED",
    "name": "main.sh",
    "nodes": ["cn-c021"],
    "partition": "long",
    "priority": 7152,
    "qos": "normal",
    "requested": {"billing": 1, "cpu": 4, "gres_gpu": 1, "mem": 49152, "node": 1},
    "signal": None,
    "start_time": datetime(2023, 2, 14, 19, 1, 19, tzinfo=MTL).astimezone(UTC),
    "submit_time": datetime(2023, 2, 14, 18, 59, 18, tzinfo=MTL).astimezone(UTC),
    "task_id": None,
    "time_limit": 43200,
    "user": "petitbonhomme",
    "work_dir": "/network/scratch/p/petitbonhomme/experience-demente",
}


class JobFactory:
    def __init__(
        self, first_submit_time: None | datetime = None, first_job_id: int = 1
    ):
        self.jobs = []
        self._first_submit_time = first_submit_time or datetime(
            2023, 2, 14, tzinfo=MTL
        ).astimezone(UTC)
        self._first_job_id = first_job_id

    @property
    def next_job_id(self):
        return self._first_job_id + len(self.jobs)

    @property
    def next_submit_time(self):
        return timedelta(hours=len(self.jobs) * 6) + self._first_submit_time

    def create_job(self, **kwargs):
        job = copy.deepcopy(base_job)
        elapsed_time = kwargs.get("elapsed_time", job["elapsed_time"])
        job.update(kwargs)
        job["submit_time"] = kwargs.get("elapsed_time", self.next_submit_time)
        job["start_time"] = kwargs.get(
            "start_time", job["submit_time"] + timedelta(seconds=60)
        )

        if job["job_state"] in ["RUNNING", "PENDING"]:
            default_end_time = None
        else:
            default_end_time = job["start_time"] + timedelta(seconds=elapsed_time)
        job["end_time"] = kwargs.get("end_time", default_end_time)

        if job["end_time"] is not None:
            default_elapsed_time = (job["end_time"] - job["start_time"]).total_seconds()
        else:
            default_elapsed_time = 0
        job["elapsed_time"] = kwargs.get("elapsed_time", default_elapsed_time)

        job["job_id"] = kwargs.get("job_id", self.next_job_id)

        return job

    def add_job(self, **kwargs):
        self.jobs.append(self.create_job(**kwargs))

    def add_job_array(
        self, task_ids, job_id: None | int = None, submit_time: None | datetime = None
    ):
        job_id = self.next_job_id
        submit_time = self.next_submit_time
        for job_array_id_offset, task_id in enumerate(task_ids):
            self.add_job(
                submit_time=submit_time,
                job_id=job_array_id_offset + job_id,
                array_job_id=job_id,
                task_id=task_id,
            )


def create_jobs():
    job_factory = JobFactory()

    for status in [
        "CANCELLED",
        "COMPLETED",
        "FAILED",
        "NODE_FAIL",
        "PREEMPTED",
        "TIMEOUT",
        "RUNNING",
        "PENDING",
    ]:
        job_factory.add_job(job_state=status)

    job_factory.add_job_array(task_ids=[1, 10, 13])

    for nodes in [["bart"], ["cn-c021", "cn-c022"]]:
        job_factory.add_job(nodes=nodes)

    for cluster_name in ["raisin", "fromage", "patate"]:
        job_factory.add_job(cluster_name=cluster_name)

    for user in ["bonhomme", "petitbonhomme", "grosbonhomme", "beaubonhomme"]:
        job_factory.add_job(user=user)

    job_factory.add_job(job_id=1_000_000, nodes=["cn-c017"], job_state="PREEMPTED")
    job_factory.add_job(job_id=1_000_000, nodes=["cn-b099"], job_state="OUT_OF_MEMORY")

    return job_factory.jobs
