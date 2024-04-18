from __future__ import annotations

import copy
from datetime import datetime, timedelta
from typing import Optional

from flatten_dict import flatten, unflatten

from sarc.config import MTL, UTC, config
from sarc.jobs.sacct import parse_in_timezone

elapsed_time = 60 * 60 * 12
end_time = datetime(2023, 2, 14, 23, 48, 54, tzinfo=MTL).astimezone(UTC)
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
    "elapsed_time": elapsed_time,
    "end_time": end_time,
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
    "start_time": end_time - timedelta(seconds=elapsed_time),
    "submit_time": end_time - timedelta(seconds=elapsed_time + 60),
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

    def format_kwargs(self, kwargs):
        kwargs.setdefault("elapsed_time", base_job["elapsed_time"])
        kwargs.setdefault("submit_time", self.next_submit_time)
        kwargs.setdefault("start_time", kwargs["submit_time"] + timedelta(seconds=60))
        kwargs.setdefault("job_state", base_job["job_state"])

        if kwargs["job_state"] in ["RUNNING", "PENDING"]:
            kwargs.setdefault("end_time", None)
        else:
            kwargs.setdefault(
                "end_time",
                kwargs["start_time"]
                + timedelta(
                    seconds=kwargs.get("elapsed_time", base_job["elapsed_time"])
                ),
            )

        # Override elapsed_time to be coherent.
        if "elapsed_time" not in kwargs and kwargs["end_time"] is not None:
            kwargs["elapsed_time"] = int(
                (kwargs["end_time"] - kwargs["start_time"]).total_seconds()
            )
        elif "elapsed_time" not in kwargs and kwargs["job_state"] == "RUNNING":
            kwargs["elapsed_time"] = (
                datetime.now(tz=UTC) - kwargs["start_time"]
            ).total_seconds()

        kwargs.setdefault("elapsed_time", base_job["elapsed_time"])

        kwargs.setdefault("job_id", self.next_job_id)

        return kwargs

    def create_job(self, **kwargs):
        job = copy.deepcopy(base_job)
        job.update(self.format_kwargs(kwargs))

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


def create_users():
    users = []
    for username, has_drac_account in [
        # Do not set a DRAC account for "bonhomme".
        # Thus, job from user `bonhomme` on a non-mila cluster
        # won't find associated user info.
        ("bonhomme", False),
        ("petitbonhomme", True),
        # ("grosbonhomme", True),  # not added, so related jobs cannot find him.
        ("beaubonhomme", True),
    ]:
        users.append(_create_user(username=username, with_drac=has_drac_account))
    return users


def _create_user(username: str, with_drac=True):
    name = f"M/Ms {username[0].upper()}{username[1:]}"
    mila_email_username = f"{username}@mila.quebec"

    drac = None
    drac_members = None
    drac_roles = None
    if with_drac:
        drac_email = f"{username}@example.com"
        drac_username = username
        drac = {
            "active": True,
            "email": drac_email,
            "username": drac_username,
        }
        drac_members = {
            "activation_status": "activated",
            "email": drac_email,
            "name": name,
            "permission": "Manager",
            "sponsor": "BigProf",
            "username": drac_username,
        }
        drac_roles = {
            "email": drac_email,
            "nom": name,
            "status": "Activated",
            "username": drac_username,
            "état du compte": "activé",
        }

    return {
        "drac": drac,
        "drac_members": drac_members,
        "drac_roles": drac_roles,
        "mila": {
            "active": True,
            "email": mila_email_username,
            # Set a different username for mila
            "username": f"{username}_mila",
        },
        "mila_ldap": {
            "co_supervisor": None,
            "display_name": name,
            "mila_cluster_gid": "1500000003",
            "mila_cluster_uid": "1500000003",
            "mila_cluster_username": username,
            "mila_email_username": mila_email_username,
            "status": "enabled",
            "supervisor": None,
        },
        "name": name,
        "record_end": None,
        "record_start": datetime(2024, 4, 11, 0, 0),
    }


def create_jobs(job_factory: JobFactory | None = None):
    if job_factory is None:
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

    for nodes in [["bart"], sorted(["cn-d001", "cn-c021", "cn-c022"])]:
        job_factory.add_job(nodes=nodes)

    for cluster_name in ["raisin", "fromage", "patate"]:
        job_factory.add_job(cluster_name=cluster_name)

    for user in ["bonhomme", "petitbonhomme"]:
        job_factory.add_job(user=user)

    # Add this job separately to set a specific cluster name.
    # Note that user `grosbonhomme` won't be added to testing database.
    # Thus, this job belongs to a non-existent user.
    for user in ["grosbonhomme"]:
        job_factory.add_job(user=user, cluster_name="mila")

    for user in ["beaubonhomme"]:
        job_factory.add_job(user=user)

    job_factory.add_job(job_id=1_000_000, nodes=["cn-c017"], job_state="PREEMPTED")
    job_factory.add_job(job_id=1_000_000, nodes=["cn-b099"], job_state="OUT_OF_MEMORY")

    job_factory.add_job(
        allocated={
            "billing": 2,
            "cpu": 12,
            "gres_gpu": 1,
            "gpu_type": "A100",
            "mem": 39152,
            "node": 1,
        },
        requested={
            "billing": 2,
            "cpu": 12,
            "gres_gpu": 1,
            "mem": 59152,
            "node": 1,
        },
    )

    # Add a job with requested and allocated GPU to 0.
    job_factory.add_job(
        job_id=999_999_999,
        elapsed_time=elapsed_time * 1.5,
        cluster_name="mila",
        user="petitbonhomme_mila",
        allocated={
            "billing": 14,
            "cpu": 12,
            "gres_gpu": 0,
            "gpu_type": None,
            "mem": 39152,
            "node": 1,
        },
        requested={
            "billing": 14,
            "cpu": 12,
            "gres_gpu": 0,
            "gpu_type": None,
            "mem": 59152,
            "node": 1,
        },
    )

    return job_factory.jobs


json_raw = {
    "metadata": {
        "plugin": {"type": "openapi/dbv0.0.37", "name": "Slurm OpenAPI DB v0.0.37"},
        "Slurm": {
            "version": {"major": 21, "micro": 8, "minor": 8},
            "release": "21.08.8-2",
        },
    },
    "errors": [],
    "jobs": [],
}

base_sacct_job = {
    "account": base_job["account"],
    "comment": {"administrator": None, "job": None, "system": None},
    "allocation_nodes": len(base_job["account"]),
    "array": {
        "job_id": base_job["array_job_id"] or None,
        "limits": {"max": {"running": {"tasks": 0}}},
        "task": None,
        "task_id": base_job["task_id"],
    },
    "association": {
        "account": base_job["account"],
        "cluster": base_job["cluster_name"],
        "partition": None,
        "user": base_job["user"],
    },
    "cluster": base_job["cluster_name"],
    "constraints": base_job["constraints"],
    "derived_exit_code": {"status": "SUCCESS", "return_code": 0},
    "time": {
        "elapsed": base_job["elapsed_time"],
        "eligible": 1641003593,
        "end": int(base_job["end_time"].timestamp()),
        "start": int(base_job["start_time"].timestamp()),
        "submission": int(base_job["submit_time"].timestamp()),
        "suspended": 0,
        "system": {"seconds": 0, "microseconds": 0},
        "limit": base_job["time_limit"] / 60,
        "total": {"seconds": 0, "microseconds": 0},
        "user": {"seconds": 0, "microseconds": 0},
    },
    "exit_code": {"status": "SUCCESS", "return_code": base_job["exit_code"]},
    "flags": [
        flag
        for flag in [
            "CLEAR_SCHEDULING",
            "STARTED_ON_BACKFILL",
            "STARTED_ON_SCHEDULE",
            "STARTED_ON_SUBMIT",
        ]
        if base_job[flag]
    ],
    "group": base_job["group"],
    "het": {"job_id": 0, "job_offset": None},
    "job_id": base_job["job_id"],
    "name": base_job["name"],
    "mcs": {"label": ""},
    "nodes": base_job["nodes"][0],
    "partition": base_job["partition"],
    "priority": base_job["priority"],
    "qos": base_job["qos"],
    "required": {"CPUs": 16, "memory": 8192},
    "kill_request_user": None,
    "reservation": {"id": 0, "name": 0},
    "state": {"current": base_job["job_state"], "reason": "Dependency"},
    "steps": [],
    "tres": {
        "allocated": [
            {
                "type": "cpu",
                "name": None,
                "id": 1,
                "count": base_job["allocated"]["cpu"],
            },
            {
                "type": "mem",
                "name": None,
                "id": 2,
                "count": base_job["allocated"]["mem"],
            },
            {"type": "energy", "name": None, "id": 3, "count": None},
            {
                "type": "node",
                "name": None,
                "id": 4,
                "count": base_job["allocated"]["node"],
            },
            {
                "type": "billing",
                "name": None,
                "id": 5,
                "count": base_job["allocated"]["billing"],
            },
            {
                "type": "gres",
                "name": "gpu",
                "id": 1001,
                "count": base_job["allocated"]["gres_gpu"],
            },
        ],
        "requested": [
            {
                "type": "cpu",
                "name": None,
                "id": 1,
                "count": base_job["requested"]["cpu"],
            },
            {
                "type": "mem",
                "name": None,
                "id": 2,
                "count": base_job["requested"]["mem"],
            },
            {
                "type": "node",
                "name": None,
                "id": 4,
                "count": base_job["requested"]["node"],
            },
            {
                "type": "billing",
                "name": None,
                "id": 5,
                "count": base_job["requested"]["billing"],
            },
            {
                "type": "gres",
                "name": "gpu",
                "id": 1001,
                "count": base_job["requested"]["gres_gpu"],
            },
        ],
    },
    "user": base_job["user"],
    "wckey": {"wckey": "", "flags": []},
    "working_directory": base_job["work_dir"],
}


class JsonJobFactory(JobFactory):
    def add_job_array(
        self,
        task_ids,
        job_id: None | int = None,
        submit_time: None | datetime = None,
        cluster_name: str = "raisin",
    ):
        job_id = self.next_job_id
        submit_time = self.format_dt_tz(cluster_name, self.next_submit_time)
        for job_array_id_offset, task_id in enumerate(task_ids):
            self.add_job(
                submit_time=submit_time,
                job_id=job_array_id_offset + job_id,
                array={
                    "job_id": job_id,
                    "limits": {"max": {"running": {"tasks": 0}}},
                    "task": None,
                    "task_id": task_id,
                },
            )

    def format_dt_tz(
        self, cluster_name: str, dt: datetime | int | None
    ) -> Optional[int]:
        if dt is None or isinstance(dt, int):
            return dt
        cluster_tz = config().clusters[cluster_name].timezone
        date_in_cluster_tz = dt.astimezone(cluster_tz)
        return int(date_in_cluster_tz.timestamp())

    def format_kwargs(self, kwargs):
        cluster_name = kwargs.get("cluster_name", base_sacct_job["cluster"])

        # Convert time and job it to flat format
        flat_kwargs = {}
        time = kwargs.get("time", {})
        json_to_flat_keys = {
            "submission": "submit_time",
            "start": "start_time",
            "end": "end_time",
            "elapsed": "elapsed_time",
        }
        if time:
            for json_key, flat_key in json_to_flat_keys.items():
                if json_key in time:
                    if json_key == "elapsed":
                        flat_kwargs[flat_key] = time[json_key]
                    else:
                        flat_kwargs[flat_key] = parse_in_timezone(time[json_key])

        if "job_id" in kwargs:
            flat_kwargs["job_id"] = kwargs["job_id"]

        if "current" in kwargs.get("state", {}):
            flat_kwargs["job_state"] = kwargs["state"]["current"]

        formated_kwargs = super().format_kwargs(flat_kwargs)

        # Convert time and job id back to json format
        kwargs["time"] = kwargs.get("time", {}) | {
            json_key: self.format_dt_tz(cluster_name, formated_kwargs[flat_key])
            for json_key, flat_key in json_to_flat_keys.items()
        }
        kwargs["job_id"] = formated_kwargs["job_id"]

        return kwargs

    def create_job(self, **kwargs):
        sacct_job = copy.deepcopy(base_sacct_job)

        flattened_sacct_job = flatten(sacct_job)
        kwargs = self.format_kwargs(kwargs)
        flattened_sacct_job.update(flatten(kwargs))

        return unflatten(flattened_sacct_job)
