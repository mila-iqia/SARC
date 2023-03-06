from __future__ import annotations

import copy
import json
from datetime import datetime, timedelta

import pytest
from flatten_dict import flatten, unflatten
from hostlist import collect_hostlist

from sarc.config import MTL, UTC, config, using_config


def create_allocations():
    return [
        {
            "start": datetime(year=2017, month=4, day=1),
            "end": datetime(year=2018, month=4, day=1),
            "timestamp": datetime(year=2023, month=2, day=1),
            "cluster_name": "fromage",
            "resource_name": "fromage-gpu",
            "group_name": "rrg-bonhomme-ad",
            "resources": {
                "compute": {
                    "cpu_year": None,
                    "gpu_year": 100,
                    "vcpu_year": None,
                    "vgpu_year": None,
                },
                "storage": {
                    "project_size": None,
                    "project_inodes": None,
                    "nearline": None,
                },
            },
        },
        {
            "start": datetime(year=2017, month=4, day=1),
            "end": datetime(year=2018, month=4, day=1),
            "timestamp": datetime(year=2023, month=2, day=1),
            "cluster_name": "fromage",
            "resource_name": "fromage-storage",
            "group_name": "rrg-bonhomme-ad",
            "resources": {
                "compute": {
                    "cpu_year": None,
                    "gpu_year": None,
                    "vcpu_year": None,
                    "vgpu_year": None,
                },
                "storage": {
                    "project_size": "50TB",
                    "project_inodes": "5e6",
                    "nearline": "15TB",
                },
            },
        },
        {
            "start": datetime(year=2018, month=4, day=1),
            "end": datetime(year=2019, month=4, day=1),
            "timestamp": datetime(year=2023, month=2, day=1),
            "cluster_name": "fromage",
            "resource_name": "fromage-gpu",
            "group_name": "rrg-bonhomme-ad",
            "resources": {
                "compute": {
                    "cpu_year": None,
                    "gpu_year": 100,
                    "vcpu_year": None,
                    "vgpu_year": None,
                },
                "storage": {
                    "project_size": None,
                    "project_inodes": None,
                    "nearline": None,
                },
            },
        },
        {
            "start": datetime(year=2018, month=4, day=1),
            "end": datetime(year=2019, month=4, day=1),
            "timestamp": datetime(year=2023, month=2, day=1),
            "cluster_name": "fromage",
            "resource_name": "fromage-storage",
            "group_name": "rrg-bonhomme-ad",
            "resources": {
                "compute": {
                    "cpu_year": None,
                    "gpu_year": None,
                    "vcpu_year": None,
                    "vgpu_year": None,
                },
                "storage": {
                    "project_size": "70TB",
                    "project_inodes": None,
                    "nearline": None,
                },
            },
        },
        {
            "start": datetime(year=2019, month=4, day=1),
            "end": datetime(year=2020, month=4, day=1),
            "timestamp": datetime(year=2023, month=2, day=1),
            "cluster_name": "patate",
            "resource_name": "patate-gpu",
            "group_name": "rrg-bonhomme-ad",
            "resources": {
                "compute": {
                    "cpu_year": None,
                    "gpu_year": 190,
                    "vcpu_year": None,
                    "vgpu_year": None,
                },
                "storage": {
                    "project_size": None,
                    "project_inodes": None,
                    "nearline": None,
                },
            },
        },
        {
            "start": datetime(year=2019, month=4, day=1),
            "end": datetime(year=2020, month=4, day=1),
            "timestamp": datetime(year=2023, month=2, day=1),
            "cluster_name": "patate",
            "resource_name": "patate-storage",
            "group_name": "rrg-bonhomme-ad",
            "resources": {
                "compute": {
                    "cpu_year": None,
                    "gpu_year": None,
                    "vcpu_year": None,
                    "vgpu_year": None,
                },
                "storage": {
                    "project_size": "90TB",
                    "project_inodes": "5e6",
                    "nearline": "90TB",
                },
            },
        },
        {
            "start": datetime(year=2020, month=4, day=1),
            "end": datetime(year=2021, month=4, day=1),
            "timestamp": datetime(year=2023, month=2, day=1),
            "cluster_name": "fromage",
            "resource_name": "fromage-gpu",
            "group_name": "rrg-bonhomme-ad",
            "resources": {
                "compute": {
                    "cpu_year": None,
                    "gpu_year": 130,
                    "vcpu_year": None,
                    "vgpu_year": None,
                },
                "storage": {
                    "project_size": None,
                    "project_inodes": None,
                    "nearline": None,
                },
            },
        },
        {
            "start": datetime(year=2020, month=4, day=1),
            "end": datetime(year=2021, month=4, day=1),
            "timestamp": datetime(year=2023, month=2, day=1),
            "cluster_name": "fromage",
            "resource_name": "fromage-storage",
            "group_name": "rrg-bonhomme-ad",
            "resources": {
                "compute": {
                    "cpu_year": None,
                    "gpu_year": None,
                    "vcpu_year": None,
                    "vgpu_year": None,
                },
                "storage": {
                    "project_size": "30TB",
                    "project_inodes": "5e6",
                    "nearline": None,
                },
            },
        },
        {
            "start": datetime(year=2020, month=4, day=1),
            "end": datetime(year=2021, month=4, day=1),
            "timestamp": datetime(year=2023, month=2, day=1),
            "cluster_name": "patate",
            "resource_name": "patate-compute",
            "group_name": "rrg-bonhomme-ad",
            "resources": {
                "compute": {
                    "cpu_year": 219,
                    "gpu_year": None,
                    "vcpu_year": None,
                    "vgpu_year": None,
                },
                "storage": {
                    "project_size": None,
                    "project_inodes": None,
                    "nearline": None,
                },
            },
        },
        {
            "start": datetime(year=2020, month=4, day=1),
            "end": datetime(year=2021, month=4, day=1),
            "timestamp": datetime(year=2023, month=2, day=1),
            "cluster_name": "patate",
            "resource_name": "patate-gpu",
            "group_name": "rrg-bonhomme-ad",
            "resources": {
                "compute": {
                    "cpu_year": None,
                    "gpu_year": 200,
                    "vcpu_year": None,
                    "vgpu_year": None,
                },
                "storage": {
                    "project_size": None,
                    "project_inodes": None,
                    "nearline": None,
                },
            },
        },
        {
            "start": datetime(year=2020, month=4, day=1),
            "end": datetime(year=2021, month=4, day=1),
            "timestamp": datetime(year=2023, month=2, day=1),
            "cluster_name": "patate",
            "resource_name": "patate-storage",
            "group_name": "rrg-bonhomme-ad",
            "resources": {
                "compute": {
                    "cpu_year": None,
                    "gpu_year": None,
                    "vcpu_year": None,
                    "vgpu_year": None,
                },
                "storage": {
                    "project_size": "70TB",
                    "project_inodes": "5e6",
                    "nearline": "80TB",
                },
            },
        },
    ]


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
                kwargs["start_time"] + timedelta(seconds=kwargs["elapsed_time"]),
            )

        # Override elapsed_time to be coherent.
        if kwargs["end_time"] is not None:
            kwargs["elapsed_time"] = (
                kwargs["end_time"] - kwargs["start_time"]
            ).total_seconds()

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
    def format_requested(self, requested: dict) -> list[dict]:
        json_requested = []
        for info_id, (key, value) in enumerate(requested.items()):
            if key == "gpu_type":
                continue

            name = ""
            if key == "gres_gpu":
                key = "gres"
                name = "gpu"

                if requested.get("gpu_type", None):
                    name += f'_{requested["gpu_type"]}'

            # NOTE: If the key is `gres`, then the id may be different than info_id
            # For instance A100's have id 1001. But we don't use this information in
            # `SlurmJob`.
            json_requested.append(
                {"type": key, "id": info_id, "count": value, "name": name}
            )

        return json_requested

    def format_dt_tz(self, cluster_name: str, dt: datetime) -> int:
        cluster_tz = config().clusters[cluster_name].timezone
        print(dt.tzinfo)
        date_in_cluster_tz = dt.astimezone(cluster_tz)
        print(date_in_cluster_tz)
        return int(date_in_cluster_tz.timestamp())

    def format_kwargs(self, kwargs):
        formated_kwargs = super().format_kwargs(kwargs)

        cluster_name = formated_kwargs.pop("cluster_name", base_sacct_job["cluster"])
        user = formated_kwargs.pop("user", base_job["user"])

        json_kwargs = {
            "cluster": cluster_name,
            "user": user,
            "association": {
                "account": formated_kwargs.pop("account", base_job["account"]),
                "cluster": cluster_name,
                "partition": None,
                "user": user,
            },
            "array": {
                "task_id": formated_kwargs.pop("task_id", None),
                "job_id": formated_kwargs.pop("array_job_id", 0),
            },
            "time": {
                key: int(
                    self.format_dt_tz(cluster_name, formated_kwargs.pop(formated_key))
                )
                for key, formated_key in [
                    ("start", "start_time"),
                    ("end", "end_time"),
                    ("submission", "submit_time"),
                ]
            }
            | {"elapsed": formated_kwargs.pop("elapsed_time")},
        }

        if "job_state" in formated_kwargs:
            json_kwargs["state"] = {
                "current": formated_kwargs.pop("job_state"),
                "reason": "Because!",
            }

        if "time_limit" in formated_kwargs:
            json_kwargs["time"]["limit"] = formated_kwargs.pop("time_limit") / 60

        if "nodes" in formated_kwargs:
            json_kwargs["nodes"] = collect_hostlist(formated_kwargs.pop("nodes"))

        if "requested" in formated_kwargs:
            json_kwargs["tres"] = {
                "requested": self.format_requested(formated_kwargs.pop("requested"))
            }
        if "allocated" in formated_kwargs:
            json_kwargs.setdefault("tres", {})
            json_kwargs["tres"]["allocated"] = self.format_requested(
                formated_kwargs.pop("allocated")
            )

        json_kwargs.update(formated_kwargs)

        import pprint

        pprint.pprint(json_kwargs)

        return json_kwargs

    def create_job(self, **kwargs):
        sacct_job = copy.deepcopy(base_sacct_job)

        flattened_sacct_job = flatten(sacct_job)
        kwargs = self.format_kwargs(kwargs)
        flattened_sacct_job.update(flatten(kwargs))

        return unflatten(flattened_sacct_job)


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

    for user in ["bonhomme", "petitbonhomme", "grosbonhomme", "beaubonhomme"]:
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

    return job_factory.jobs


@pytest.fixture
def json_job(request):
    return JsonJobFactory().create_job(**request.param)


@pytest.fixture
def json_jobs():
    job_factory = JsonJobFactory()
    create_jobs(job_factory)
    job_factory.add_job(group=None)
    return job_factory.jobs


@pytest.fixture
def sacct_json(json_jobs):
    tmp_json_raw = copy.deepcopy(json_raw)
    tmp_json_raw["jobs"] = json_jobs
    return json.dumps(tmp_json_raw)


@pytest.fixture
def db_allocations():
    return create_allocations()


@pytest.fixture
def db_jobs():
    return create_jobs()


def custom_db_config(cfg, db_name):
    assert "test" in db_name
    new_cfg = cfg.replace(mongo=cfg.mongo.replace(database=db_name))
    db = new_cfg.mongo.instance
    # Ensure we do not use and thus wipe the production database
    assert db.name == db_name
    return new_cfg


def clear_db(db):
    db.allocations.drop()
    db.jobs.drop()


def fill_db(db):
    db.allocations.insert_many(create_allocations())
    db.jobs.insert_many(create_jobs())


def create_db_configuration_fixture(db_name, empty=False, scope="function"):
    @pytest.fixture(scope=scope)
    def fixture(standard_config_object):
        cfg = custom_db_config(standard_config_object, db_name)
        db = cfg.mongo.instance
        clear_db(db)
        if not empty:
            fill_db(db)
        yield

    return fixture


empty_read_write_db_config_object = create_db_configuration_fixture(
    db_name="sarc-read-write-test",
    empty=True,
    scope="function",
)


read_write_db_config_object = create_db_configuration_fixture(
    db_name="sarc-read-write-test",
    scope="function",
)


read_only_db_config_object = create_db_configuration_fixture(
    db_name="sarc-read-only-test",
    scope="session",
)


@pytest.fixture
def empty_read_write_db(standard_config, empty_read_write_db_config_object):
    cfg = custom_db_config(standard_config, "sarc-read-write-test")
    with using_config(cfg) as cfg:
        yield cfg.mongo.instance


@pytest.fixture
def read_write_db(standard_config, read_write_db_config_object):
    cfg = custom_db_config(standard_config, "sarc-read-write-test")
    with using_config(cfg) as cfg:
        yield cfg.mongo.instance


@pytest.fixture
def read_only_db(standard_config, read_only_db_config_object):
    cfg = custom_db_config(standard_config, "sarc-read-only-test")
    with using_config(cfg) as cfg:
        yield cfg.mongo.instance
