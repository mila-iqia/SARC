import json
from datetime import date, datetime, timedelta

import pytest
from fabric.testing.base import Command

from sarc.cache import Cache
from sarc.config import UTC
from sarc.scraping.jobs import fetch_jobs
from sarc.scraping.jobs_utils import (
    JobConversionError,
    _convert_json_fast,
    _convert_json_job,
    fetch_raw,
    parse_raw,
)
from tests.common.dateutils import MTL, _dtfmt


@pytest.mark.usefixtures("no_pkey")
@pytest.mark.parametrize(
    "test_config",
    [
        {
            "clusters": {
                "test": {
                    "host": "patate",
                    "private_key": {"file": "tests/id_test", "password": "12345"},
                    "user_domain": "drac",
                }
            }
        }
    ],
    indirect=True,
)
def test_fetch_raw(test_config, remote):
    remote.expect(
        host="patate",
        cmd=f"export TZ=UTC && sacct -X -S {_dtfmt(2023, 2, 28)} -E {_dtfmt(2023, 3, 1)} --allusers --json --duplicates",
        out=b"{}",
    )
    assert fetch_raw(
        cluster=test_config.clusters["test"],
        start=datetime(2023, 2, 28, tzinfo=MTL).astimezone(UTC),
        end=datetime(2023, 3, 1, tzinfo=MTL).astimezone(UTC),
    ) == "{}".encode("utf-8")


@pytest.mark.usefixtures("no_pkey")
@pytest.mark.parametrize(
    "test_config",
    [
        {
            "clusters": {
                "test": {
                    "host": "test",
                    "private_key": {"file": "tests/id_test", "password": "12345"},
                    "user_domain": "drac",
                }
            }
        }
    ],
    indirect=True,
)
def test_fetch_raw2(test_config, remote):
    remote.expect(
        commands=[
            Command(
                f"export TZ=UTC && sacct -X -S {_dtfmt(2023, 2, 28)} -E {_dtfmt(2023, 3, 1)} --allusers --json --duplicates",
                out=b"{}",
            ),
            Command(
                f"export TZ=UTC && sacct -X -S {_dtfmt(2023, 2, 28)} -E {_dtfmt(2023, 3, 1)} --allusers --json --duplicates",
                out=b'{ "value": 2 }',
            ),
        ]
    )
    assert fetch_raw(
        cluster=test_config.clusters["test"],
        start=datetime(2023, 2, 28, tzinfo=MTL).astimezone(UTC),
        end=datetime(2023, 3, 1, tzinfo=MTL).astimezone(UTC),
    ) == "{}".encode("utf-8")
    assert fetch_raw(
        cluster=test_config.clusters["test"],
        start=datetime(2023, 2, 28, tzinfo=MTL).astimezone(UTC),
        end=datetime(2023, 3, 1, tzinfo=MTL).astimezone(UTC),
    ) == '{ "value": 2 }'.encode("utf-8")


@pytest.mark.usefixtures("no_pkey")
@pytest.mark.parametrize(
    "test_config",
    [
        {
            "clusters": {
                "test": {
                    "host": "patate",
                    "private_key": {"file": "tests/id_test", "password": "12345"},
                    "user_domain": "drac",
                }
            }
        }
    ],
    indirect=True,
)
@pytest.mark.time_machine(datetime(2023, 2, 28, tzinfo=MTL), tick=False)
def test_fetch_jobs_get_cache(test_config, enabled_cache, remote):
    today = datetime.combine(date.today(), datetime.min.time(), tzinfo=MTL).astimezone(
        UTC
    )
    yesterday = today - timedelta(days=1)

    # we ask for yesterday, today and tomorrow
    fmt = "%Y-%m-%dT%H:%M"

    remote.expect(
        commands=[
            Command(
                f"export TZ=UTC && sacct -X -S {yesterday.strftime(fmt)} -E {today.strftime(fmt)} --allusers --json --duplicates",
                out=b'{"value": 2}',
            )
        ]
    )

    fetch_jobs(
        ["test"],
        test_config.clusters,
        [f"{yesterday.strftime(fmt)}-{today.strftime(fmt)}"],
        None,
    )

    # Retrieve from the cache
    cache = Cache(subdirectory="jobs")
    cache_entries = list(cache.read_from(from_time=yesterday))
    assert len(cache_entries) == 1
    items = list(cache_entries[0].items())
    assert len(items) == 1
    key, value = items[0]
    assert key == f"test_{yesterday.strftime(fmt)}_{today.strftime(fmt)}"
    assert value == b'{"value": 2}'


def test_convert_gres_gpu():
    # This might not be a full entry, just enough to pass the _convert_job_json function.
    entry = {
        "job_id": 123,
        "nodes": "node123",
        "name": "test_job",
        "user": "toto",
        "group": "toto_group",
        "account": "toto_account",
        "state": {"current": ["COMPLETED"], "reason": "None"},
        "tres": {
            "allocated": [{"type": "gres", "name": "gpu", "id": 1001, "count": 2}],
            "requested": [{"type": "gres", "name": "gpu:v100", "id": 1001, "count": 2}],
        },
        "exit_code": {
            "status": ["SUCCESS"],
            "return_code": {"set": True, "infinite": False, "number": 0},
            "signal": {
                "id": {"set": False, "infinite": False, "number": 0},
                "name": "",
            },
        },
        "array": {
            "job_id": 0,
            "task_id": {"set": False, "infinite": False, "number": 0},
        },
        "partition": "partition123",
        "constraints": "[cascade|milan]",
        "priority": {"set": True, "infinite": False, "number": 489206},
        "qos": "normal",
        "working_directory": "/home/toto/my_job_name",
        "time": {
            "elapsed": 259223,
            "eligible": 1747064484,
            "end": 1751866893,
            "planned": {"set": True, "infinite": False, "number": 4543186},
            "start": 1751607670,
            "submission": 1747064484,
            "suspended": 0,
            "system": {"seconds": 0, "microseconds": 0},
            "limit": {"set": True, "infinite": False, "number": 4320},
            "total": {"seconds": 0, "microseconds": 0},
            "user": {"seconds": 0, "microseconds": 0},
        },
        "cluster": "test",
        "flags": [],
    }

    job = _convert_json_job(entry, "test", {"major": "24", "micro": "1", "minor": "11"})
    assert job is not None
    assert job["allocated_gres_gpu"] == 2
    assert job["requested_gres_gpu"] == 2
    assert job["requested_gpu_type"] == "v100"


def test_convert_version_supported():
    version_supported = {"major": "24", "micro": "1", "minor": "11"}
    version_unsupported = {"major": "124", "micro": "1", "minor": "11"}

    entry = {
        "job_id": 123456,
        "array": {
            "job_id": 0,
            "limits": {"max": {"running": {"tasks": 0}}},
            "task_id": {"set": False, "infinite": False, "number": 0},
            "task": "",
        },
        "name": "my_job_name",
        "user": "toto",
        "group": "toto_group",
        "account": "toto_account",
        "state": {"current": ["TIMEOUT"], "reason": "None"},
        "exit_code": {
            "status": ["SUCCESS"],
            "return_code": {"set": True, "infinite": False, "number": 0},
            "signal": {
                "id": {"set": False, "infinite": False, "number": 0},
                "name": "",
            },
        },
        "time": {
            "elapsed": 259223,
            "eligible": 1747064484,
            "end": 1751866893,
            "planned": {"set": True, "infinite": False, "number": 4543186},
            "start": 1751607670,
            "submission": 1747064484,
            "suspended": 0,
            "system": {"seconds": 0, "microseconds": 0},
            "limit": {"set": True, "infinite": False, "number": 4320},
            "total": {"seconds": 0, "microseconds": 0},
            "user": {"seconds": 0, "microseconds": 0},
        },
        "nodes": "node123",
        "partition": "partition123",
        "constraints": "[cascade|milan]",
        "priority": {"set": True, "infinite": False, "number": 489206},
        "qos": "normal",
        "working_directory": "/home/toto/my_job_name",
        "tres": {
            "allocated": [
                {"type": "cpu", "name": "", "id": 1, "count": 8},
                {"type": "mem", "name": "", "id": 2, "count": 16000},
                {"type": "node", "name": "", "id": 4, "count": 1},
                {"type": "billing", "name": "", "id": 5, "count": 8000},
            ],
            "requested": [
                {"type": "cpu", "name": "", "id": 1, "count": 8},
                {"type": "mem", "name": "", "id": 2, "count": 16000},
                {"type": "node", "name": "", "id": 4, "count": 1},
                {"type": "billing", "name": "", "id": 5, "count": 8000},
            ],
        },
        "flags": ["STARTED_ON_BACKFILL", "START_RECEIVED"],
        "cluster": "test",
    }

    # test version supported
    slurmjob = _convert_json_job(entry, "test", version_supported)

    assert slurmjob is not None
    assert slurmjob["job_id"] == 123456
    assert slurmjob["cluster_user"] == "toto"
    assert slurmjob["group"] == "toto_group"
    assert slurmjob["account"] == "toto_account"
    assert slurmjob["partition"] == "partition123"
    assert slurmjob["job_state"] == "TIMEOUT"
    assert slurmjob["work_dir"] == "/home/toto/my_job_name"

    # test version unsupported
    with pytest.raises(JobConversionError):
        slurmjob = _convert_json_job(entry, "test", version_unsupported)


def test_convert_fast_gres_gpu():
    # This might not be a full entry, just enough to pass the _convert_json_fast function.
    entry = {
        "job_id": 123,
        "array_job_id": 0,
        "array_task_id": None,
        "nodes": "node123",
        "name": "test_job",
        "user": "toto",
        "group": "toto_group",
        "account": "toto_account",
        "state": ["COMPLETED"],
        "requested_gres_gpu": 2,
        "requested_gres_gpu_type": "v100",
        "allocated_gres_gpu": 2,
        "allocated_gres_gpu_type": "v100",
        "exitcode_return_code": 0,
        "exitcode_signal": None,
        "partition": "partition123",
        "constraints": "[cascade|milan]",
        "priority": 489206,
        "qos": "normal",
        "work_dir": "/home/toto/my_job_name",
        "submit_line": "sbatch my_job.sh",
        "time_elapsed": 259223,
        "time_end": 1751866893,
        "time_start": 1751607670,
        "time_submit": 1747064484,
        "time_timelimit": 4320,
        "cluster": "test",
        "flags": [],
    }

    scraped_start = datetime(2025, 5, 12, tzinfo=UTC)
    scraped_end = datetime(2025, 7, 7, tzinfo=UTC)

    job = _convert_json_fast(entry, "test", scraped_start, scraped_end)
    assert job is not None
    assert job["allocated_gres_gpu"] == 2
    assert job["requested_gres_gpu"] == 2
    assert job["requested_gpu_type"] == "v100"
    assert job["allocated_gpu_type"] == "v100"


def test_convert_fast_basic():
    entry = {
        "job_id": 123456,
        "array_job_id": 66857546,
        "array_task_id": 1393,
        "name": "my_job_name",
        "user": "toto",
        "group": "toto_group",
        "account": "toto_account",
        "state": ["TIMEOUT"],
        "exitcode_return_code": 0,
        "exitcode_signal": None,
        "time_elapsed": 259223,
        "time_end": 1751866893,
        "time_start": 1751607670,
        "time_submit": 1747064484,
        "time_timelimit": 4320,
        "nodes": "node123",
        "partition": "partition123",
        "constraints": "[cascade|milan]",
        "priority": 489206,
        "qos": "normal",
        "work_dir": "/home/toto/my_job_name",
        "submit_line": "sbatch my_job_name.sh",
        "requested_cpu": 8,
        "requested_mem": 16000,
        "requested_node": 1,
        "requested_billing": 8000,
        "allocated_cpu": 8,
        "allocated_mem": 16000,
        "allocated_node": 1,
        "allocated_billing": 8000,
        "flags": ["STARTED_ON_BACKFILL", "START_RECEIVED"],
        "cluster": "test",
    }

    scraped_start = datetime(2025, 5, 12, tzinfo=UTC)
    scraped_end = datetime(2025, 7, 7, tzinfo=UTC)

    job = _convert_json_fast(entry, "test", scraped_start, scraped_end)

    assert job is not None
    assert job["job_id"] == 123456
    assert job["array_job_id"] == 66857546
    assert job["task_id"] == 1393
    assert job["cluster_user"] == "toto"
    assert job["group"] == "toto_group"
    assert job["account"] == "toto_account"
    assert job["partition"] == "partition123"
    assert job["job_state"] == "TIMEOUT"
    assert job["work_dir"] == "/home/toto/my_job_name"
    assert job["exit_code"] == 0
    assert job["signal"] is None
    assert job["time_limit"] == 4320 * 60
    assert job["requested_cpu"] == 8
    assert job["allocated_cpu"] == 8
    assert job["STARTED_ON_BACKFILL"] is True
    assert job["latest_scraped_start"] == scraped_start
    assert job["latest_scraped_end"] == scraped_end


def test_convert_fast_array_job_id_zero():
    # array_job_id of 0 means the job is not part of an array, so it should
    # be normalized to None just like _convert_json_job does.
    entry = {
        "job_id": 123,
        "array_job_id": 0,
        "array_task_id": None,
        "name": "test_job",
        "user": "toto",
        "group": "toto_group",
        "account": "toto_account",
        "state": ["COMPLETED"],
        "exitcode_return_code": 0,
        "exitcode_signal": None,
        "time_elapsed": 0,
        "time_end": 0,
        "time_start": 0,
        "time_submit": 1747064484,
        "time_timelimit": 0,
        "nodes": "None assigned",
        "partition": "partition123",
        "constraints": "",
        "priority": 1,
        "qos": "normal",
        "work_dir": "/home/toto/my_job_name",
        "submit_line": "salloc",
        "flags": [],
        "cluster": "test",
    }

    scraped_start = datetime(2025, 5, 12, tzinfo=UTC)
    scraped_end = datetime(2025, 7, 7, tzinfo=UTC)

    job = _convert_json_fast(entry, "test", scraped_start, scraped_end)
    assert job is not None
    assert job["array_job_id"] is None
    assert job["nodes"] == []


def test_convert_fast_none_values():
    # fastsacct reports absent values as None instead of 0 or "" for fields
    # such as array_job_id, array_task_id, time_start, time_end,
    # time_timelimit, priority and constraints (e.g. for a pending job).
    entry = {
        "job_id": 123,
        "array_job_id": None,
        "array_task_id": None,
        "name": "test_job",
        "user": "toto",
        "group": "toto_group",
        "account": "toto_account",
        "state": ["PENDING"],
        "exitcode_return_code": 0,
        "exitcode_signal": None,
        "time_elapsed": 0,
        "time_end": None,
        "time_start": None,
        "time_submit": 1747064484,
        "time_timelimit": None,
        "nodes": "None assigned",
        "partition": "partition123",
        "constraints": None,
        "priority": None,
        "qos": "normal",
        "work_dir": "/home/toto/my_job_name",
        "submit_line": "salloc",
        "flags": [],
        "cluster": "test",
    }

    scraped_start = datetime(2025, 5, 12, tzinfo=UTC)
    scraped_end = datetime(2025, 7, 7, tzinfo=UTC)

    job = _convert_json_fast(entry, "test", scraped_start, scraped_end)
    assert job is not None
    assert job["array_job_id"] is None
    assert job["task_id"] is None
    assert job["start_time"] is None
    assert job["end_time"] is None
    assert job["time_limit"] is None
    assert job["constraints"] is None
    assert job["priority"] is None
    assert job["nodes"] == []


def test_convert_fast_cluster_mismatch_warning(caplog):
    entry = {
        "job_id": 123,
        "array_job_id": 0,
        "array_task_id": None,
        "name": "test_job",
        "user": "toto",
        "group": "toto_group",
        "account": "toto_account",
        "state": ["COMPLETED"],
        "exitcode_return_code": 0,
        "exitcode_signal": None,
        "time_elapsed": 0,
        "time_end": 0,
        "time_start": 0,
        "time_submit": 1747064484,
        "time_timelimit": 0,
        "nodes": "None assigned",
        "partition": "partition123",
        "constraints": "",
        "priority": 1,
        "qos": "normal",
        "work_dir": "/home/toto/my_job_name",
        "submit_line": "salloc",
        "flags": [],
        "cluster": "other_cluster",
    }

    scraped_start = datetime(2025, 5, 12, tzinfo=UTC)
    scraped_end = datetime(2025, 7, 7, tzinfo=UTC)

    caplog.clear()
    job = _convert_json_fast(entry, "test", scraped_start, scraped_end)
    assert job is not None
    assert job["cluster_name"] == "test"
    assert any("different cluster name" in record.message for record in caplog.records)


def test_parse_raw_dispatches_to_fast_converter():
    # parse_raw must route to _convert_json_fast (instead of _convert_json_job)
    # when the payload declares the fastsacct-flat-v1 schema, regardless of
    # slurm version metadata.
    entry = {
        "job_id": 123,
        "array_job_id": 0,
        "array_task_id": None,
        "name": "test_job",
        "user": "toto",
        "group": "toto_group",
        "account": "toto_account",
        "state": ["COMPLETED"],
        "exitcode_return_code": 0,
        "exitcode_signal": None,
        "time_elapsed": 0,
        "time_end": 0,
        "time_start": 0,
        "time_submit": 1747064484,
        "time_timelimit": 0,
        "nodes": "None assigned",
        "partition": "partition123",
        "constraints": "",
        "priority": 1,
        "qos": "normal",
        "work_dir": "/home/toto/my_job_name",
        "submit_line": "salloc",
        "flags": [],
        "cluster": "test",
    }
    raw = json.dumps(
        {
            "meta": {
                "source": "fastsacct",
                "schema_version": "fastsacct-flat-v1",
                "slurm_abi_version": "25.05",
            },
            "jobs": [entry],
        }
    ).encode("utf-8")

    scraped_start = datetime(2025, 5, 12, tzinfo=UTC)
    scraped_end = datetime(2025, 7, 7, tzinfo=UTC)

    jobs = list(parse_raw(raw, "test", scraped_start, scraped_end))
    assert len(jobs) == 1
    (job,) = jobs
    assert job == _convert_json_fast(entry, "test", scraped_start, scraped_end)


def test_parse_raw_dispatches_to_job_converter_without_fast_schema():
    # Regular (non-fastsacct) payloads must keep going through _convert_json_job.
    entry = {
        "job_id": 123,
        "nodes": "node123",
        "name": "test_job",
        "user": "toto",
        "group": "toto_group",
        "account": "toto_account",
        "state": {"current": ["COMPLETED"], "reason": "None"},
        "tres": {"allocated": [], "requested": []},
        "exit_code": {
            "status": ["SUCCESS"],
            "return_code": {"set": True, "infinite": False, "number": 0},
            "signal": {
                "id": {"set": False, "infinite": False, "number": 0},
                "name": "",
            },
        },
        "array": {
            "job_id": 0,
            "task_id": {"set": False, "infinite": False, "number": 0},
        },
        "partition": "partition123",
        "constraints": "",
        "priority": {"set": True, "infinite": False, "number": 1},
        "qos": "normal",
        "working_directory": "/home/toto/my_job_name",
        "time": {
            "elapsed": 0,
            "eligible": 1747064484,
            "end": 0,
            "start": 0,
            "submission": 1747064484,
            "suspended": 0,
            "limit": {"set": True, "infinite": False, "number": 0},
        },
        "cluster": "test",
        "flags": [],
    }
    raw = json.dumps(
        {
            "meta": {"slurm": {"version": {"major": "24", "minor": "11"}}},
            "jobs": [entry],
        }
    ).encode("utf-8")

    scraped_start = datetime(2025, 5, 12, tzinfo=UTC)
    scraped_end = datetime(2025, 7, 7, tzinfo=UTC)

    jobs = list(parse_raw(raw, "test", scraped_start, scraped_end))
    assert len(jobs) == 1
    (job,) = jobs
    assert job["job_id"] == 123
    assert "exitcode_return_code" not in job
