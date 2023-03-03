from __future__ import annotations

import copy
import json
from datetime import datetime, timedelta

import pytest
from flatten_dict import flatten, unflatten
from hostlist import collect_hostlist

from sarc.config import MTL, UTC, config, using_config

from .allocations.factory import create_allocations
from .diskusage.factory import create_diskusages
from .jobs.factory import create_jobs

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
    db.diskusage.drop()


def fill_db(db):
    db.allocations.insert_many(create_allocations())
    db.jobs.insert_many(create_jobs())
    db.diskusage.insert_many(create_diskusages())


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
