from __future__ import annotations

import json
import subprocess
import time
from datetime import datetime, timedelta
from difflib import unified_diff
from pathlib import Path
from unittest.mock import patch

import pytest
from fabric.testing.base import Command, Session
from opentelemetry.trace import StatusCode
from sqlmodel import select

from sarc.cache import Cache
from sarc.config import UTC, config
from sarc.core.scraping.jobs_utils import _convert_json_job, fetch_raw, parse_raw
from sarc.db.cluster import SlurmClusterDB
from sarc.db.job import SlurmJobDB
from sarc.models.job import SlurmState
from tests.common.dateutils import MTL, PST, _dtfmt


def _dump(job):
    return SlurmJobDB(**job, cluster_id=-1, sarc_user_id=-1).model_dump_json(indent=4)


parameters = {
    "user": {"user": "longbonhomme"},
    "job_state": {"state": {"current": ["OUT_OF_MEMORY"], "reason": "None"}},
    "signal": {
        "exit_code": {
            "status": "SIGNALED",
            "return_code": {"number": None},
            "signal": {"id": {"number": 9}, "name": "Killed"},
        }
    },
    "exit_code": {"exit_code": {"return_code": {"number": 1}, "status": "FAILED"}},
    "time_limit": {"time": {"limit": {"number": 12345}}},  # 12345 * 60 = 740700 secs
    "submit_time": {
        "time": {
            "submission": int(
                datetime(2023, 2, 24, tzinfo=MTL).astimezone(UTC).timestamp()
            )
        }
    },
    "end_time": {
        "time": {
            "end": int(datetime(2023, 2, 15, tzinfo=MTL).astimezone(UTC).timestamp())
        }
    },
    "no_end_time": {"time": {"end": None}},
    "nodes": {"nodes": "node1,node[3-5]"},
    "flags": {"flags": ["CLEAR_SCHEDULING", "STARTED_ON_SUBMIT"]},
    "tres": {
        "tres": {
            "allocated": [
                {"count": 2, "id": 1, "name": None, "type": "cpu"},
                {"count": 10000, "id": 2, "name": None, "type": "mem"},
                {"count": 1, "id": 4, "name": None, "type": "node"},
                {"count": 1, "id": 5, "name": None, "type": "billing"},
                {"count": 1, "id": 1001, "name": "gpu", "type": "gres"},
                {"count": 1, "id": 1002, "name": "gpu:p100", "type": "gres"},
            ],
            "requested": [
                {"count": 4, "id": 1, "name": None, "type": "cpu"},
                {"count": 16384, "id": 2, "name": None, "type": "mem"},
                {"count": 2, "id": 4, "name": None, "type": "node"},
                {"count": 3, "id": 5, "name": None, "type": "billing"},
                {"count": 4, "id": 1001, "name": "gpu", "type": "gres"},
                {"count": 4, "id": 1002, "name": "gpu:p100", "type": "gres"},
            ],
        }
    },
    "array": {
        "array": {
            "job_id": 29036715,
            "limits": {"max": {"running": {"tasks": 0}}},
            "task": None,
            "task_id": {"number": 10},
        },
        "job_id": 29036725,
    },
}


@pytest.mark.usefixtures("tzlocal_is_mtl")
@pytest.mark.parametrize(
    "patched_job", parameters.values(), ids=parameters.keys(), indirect=True
)
def test_parse_json_job(patched_job, base_job, slurm_version, file_regression):
    sstart = datetime(2024, 3, 25, tzinfo=MTL).astimezone(UTC)
    send = datetime(2024, 3, 26, tzinfo=MTL).astimezone(UTC)

    orig = _convert_json_job(base_job, "raisin", slurm_version, sstart, send)
    patched = _convert_json_job(patched_job, "raisin", slurm_version, sstart, send)

    diff = "".join(
        unified_diff(
            _dump(orig).splitlines(True),
            _dump(patched).splitlines(True),
            fromfile="base job",
            tofile="patched job",
        )
    )
    file_regression.check(diff)


@pytest.mark.usefixtures("tzlocal_is_mtl")
@pytest.mark.parametrize(
    "json_jobs",
    [
        {
            "tres": {
                "allocated": [
                    {"requested": {"quossé ça fait icitte ça?": "ché pas"}},
                    {"count": 2, "id": 1, "name": None, "type": "cpu"},
                    {"count": 10000, "id": 2, "name": None, "type": "mem"},
                    {"count": 1, "id": 4, "name": None, "type": "node"},
                    {"count": 1, "id": 5, "name": None, "type": "billing"},
                    {"count": 1, "id": 1001, "name": "gpu", "type": "gres"},
                    {"count": 1, "id": 1002, "name": "gpu:p100", "type": "gres"},
                ],
                "requested": [
                    {"count": 4, "id": 1, "name": None, "type": "cpu"},
                    {"count": 16384, "id": 2, "name": None, "type": "mem"},
                    {"count": 2, "id": 4, "name": None, "type": "node"},
                    {"count": 3, "id": 5, "name": None, "type": "billing"},
                    {"count": 4, "id": 1001, "name": "gpu", "type": "gres"},
                    {"count": 4, "id": 1002, "name": "gpu:p100", "type": "gres"},
                ],
            }
        }
    ],
    indirect=True,
)
def test_parse_malformed_jobs(sacct_json, slurm_version, captrace):
    with pytest.raises(KeyError):
        _convert_json_job(json.loads(sacct_json)["jobs"][0], "mila", slurm_version)
    spans = captrace.get_finished_spans()
    assert len(spans) > 0
    # Just check the span that should have got an error.
    error_spans = [
        span for span in spans if span.status.status_code == StatusCode.ERROR
    ]
    assert len(error_spans) == 1
    (error_span,) = error_spans
    assert error_span.name == "_convert_json_job"


@pytest.mark.usefixtures("tzlocal_is_mtl")
@pytest.mark.parametrize("json_jobs", [{"group": None}], indirect=True)
def test_parse_no_group_jobs(sacct_json, caplog):
    jobs = parse_raw(
        sacct_json.encode("utf-8"),
        "cedar",
        datetime(2023, 2, 14, tzinfo=MTL).astimezone(UTC),
        datetime(2023, 2, 15, tzinfo=MTL).astimezone(UTC),
    )
    with caplog.at_level("DEBUG"):
        assert list(jobs) == [None]
    assert 'Skipping job with group "None": 1' in caplog.text


@pytest.mark.usefixtures("tzlocal_is_mtl")
@pytest.mark.parametrize("json_jobs", [{"cluster": "patate"}], indirect=True)
def test_scrape_lost_job_on_wrong_cluster(sacct_json, caplog):
    parsed_jobs = parse_raw(
        sacct_json.encode("utf-8"),
        "raisin",
        datetime(2023, 2, 14, tzinfo=MTL).astimezone(UTC),
        datetime(2023, 2, 15, tzinfo=MTL).astimezone(UTC),
    )
    with caplog.at_level("WARNING"):
        jobs = list(parsed_jobs)

    assert len(jobs) == 1
    assert jobs[0]["cluster_name"] == "raisin"

    assert (
        'Job 1 from cluster "raisin" has a different cluster name: "patate". Using "raisin"'
        in caplog.text
    )


@pytest.mark.usefixtures("tzlocal_is_mtl", "enabled_cache")
@pytest.mark.parametrize("json_jobs", [{}], indirect=True)
def test_parse_jobs_from_cache(sacct_json, file_regression, test_config):
    # Store json data in cache
    cache = Cache(subdirectory="jobs")
    with cache.create_entry(datetime(2023, 2, 16, tzinfo=UTC)) as cache_entry:
        cache_entry.add_value(
            "test_2023-02-14T00:00_2023-02-15T00:00", sacct_json.encode("utf-8")
        )

    # parse jobs
    nb_entries = 0
    jobs = []
    for cache_entry in cache.read_from(from_time=datetime(2023, 2, 15, tzinfo=UTC)):
        nb_entries += 1

        # Retrieve all jobs associated to the time intervals
        for key, value in cache_entry.items():
            cluster_name = key.split("_")[0]
            scraped_start = datetime.fromisoformat(key.split("_")[1]).replace(
                tzinfo=UTC
            )
            scraped_end = datetime.fromisoformat(key.split("_")[2]).replace(tzinfo=UTC)

            # Store the jobs in the database, beginning by the
            # oldest intervals
            for entry in parse_raw(value, cluster_name, scraped_start, scraped_end):
                jobs.append(entry)

    assert nb_entries == 1  # one cache entry for 2023-02-16
    assert len(jobs) == 1  # one job in the cache entry

    file_regression.check("\n".join([_dump(job) for job in jobs]))


@pytest.mark.usefixtures("no_pkey")
@pytest.mark.parametrize(
    "test_config", [{"clusters": {"patate": {"host": "patate"}}}], indirect=True
)
def test_sacct_bin_and_accounts(test_config, remote):
    remote.expect(
        host="patate",
        cmd=f"export TZ=UTC && /opt/software/slurm/bin/sacct -A rrg-bonhomme-ad_gpu,rrg-bonhomme-ad_cpu,def-bonhomme_gpu,def-bonhomme_cpu -X -S {_dtfmt(2023, 2, 14)} -E {_dtfmt(2023, 2, 15)} --allusers --json",
        out=b'{"jobs": []}',
    )
    fetch_raw(
        cluster=config().clusters["patate"],
        start=datetime(2023, 2, 14, tzinfo=MTL).astimezone(UTC),
        end=datetime(2023, 2, 15, tzinfo=MTL).astimezone(UTC),
    )


@patch("os.system")
@pytest.mark.usefixtures("jobless_read_write_db")
def test_localhost(os_system, monkeypatch):
    # This test requires write_setup.cache to be empty else it will never call
    # mock_subprocess_run
    def mock_subprocess_run(*args, **kwargs):
        mock_subprocess_run.called += 1
        return subprocess.CompletedProcess(
            args=args, returncode=0, stdout='{"jobs": []}', stderr=""
        )

    mock_subprocess_run.called = 0

    monkeypatch.setattr(subprocess, "run", mock_subprocess_run)

    fetch_raw(
        cluster=config().clusters["local"],
        start=datetime(2023, 2, 14, tzinfo=MTL).astimezone(UTC),
        end=datetime(2023, 2, 15, tzinfo=MTL).astimezone(UTC),
    )

    assert mock_subprocess_run.called >= 1


@pytest.mark.parametrize(
    "test_config", [{"clusters": {"raisin": {"host": "raisin"}}}], indirect=True
)
@pytest.mark.parametrize("json_jobs", [{}], indirect=True)
@pytest.mark.usefixtures("jobless_read_write_db", "enabled_cache", "no_pkey")
def test_stdout_message_before_json(
    get_jobs, test_config, sacct_json, remote, file_regression, cli_main, monkeypatch
):

    remote.expect(
        host="raisin",
        cmd=f"export TZ=UTC && /opt/slurm/bin/sacct -X -S {_dtfmt(2023, 2, 15)} -E {_dtfmt(2023, 2, 16)} --allusers --json",
        out=f"Welcome on raisin,\nThe sweetest supercomputer in the world!\n{sacct_json}".encode(
            "utf-8"
        ),
    )

    # Import here so that config() is setup correctly when CLI is created.
    import sarc.cli  # noqa: F401

    assert (
        cli_main(
            [
                "fetch",
                "jobs",
                "--cluster_names",
                "raisin",
                "--intervals",
                f"{_dtfmt(2023, 2, 15)}-{_dtfmt(2023, 2, 16)}",
            ]
        )
        == 0
    )

    assert cli_main(["-v", "parse", "jobs", "--since", "2023-02-14T00:00"]) == 0

    jobs = list(get_jobs())
    assert jobs

    file_regression.check(
        f"Found {len(jobs)} job(s):\n"
        + "\n".join(
            [job.model_dump_json(exclude={"id": True}, indent=4) for job in jobs]
        )
    )


@pytest.mark.parametrize(
    "test_config", [{"clusters": {"raisin": {"host": "raisin"}}}], indirect=True
)
@pytest.mark.parametrize("json_jobs", [{}], indirect=True)
@pytest.mark.usefixtures("jobless_read_write_db", "enabled_cache", "no_pkey")
def test_update_job(
    get_jobs, test_config, sacct_json, remote, file_regression, cli_main
):
    remote.expect(
        host="raisin",
        commands=[
            Command(
                cmd=f"export TZ=UTC && /opt/slurm/bin/sacct -X -S {_dtfmt(2023, 2, 15)} -E {_dtfmt(2023, 2, 16)} --allusers --json",
                out=sacct_json.encode("utf-8"),
            )
            for _ in range(2)
        ],
    )

    # Import here so that config() is setup correctly when CLI is created.
    import sarc.cli  # noqa: F401

    assert (
        cli_main(
            [
                "fetch",
                "jobs",
                "--cluster_names",
                "raisin",
                "--intervals",
                f"{_dtfmt(2023, 2, 15)}-{_dtfmt(2023, 2, 16)}",
            ]
        )
        == 0
    )

    assert cli_main(["-v", "parse", "jobs", "--since", "2023-02-14T00:00"]) == 0

    assert len(list(get_jobs())) == 1

    time.sleep(1)
    assert (
        cli_main(
            [
                "fetch",
                "jobs",
                "--cluster_names",
                "raisin",
                "--intervals",
                f"{_dtfmt(2023, 2, 15)}-{_dtfmt(2023, 2, 16)}",
            ]
        )
        == 0
    )

    assert cli_main(["-v", "parse", "jobs", "--since", "2023-02-15T00:00"]) == 0

    jobs = list(get_jobs())
    assert len(jobs) == 1

    file_regression.check(
        f"Found {len(jobs)} job(s):\n"
        + "\n".join(
            [job.model_dump_json(exclude={"id": True}, indent=4) for job in jobs]
        )
    )


@pytest.mark.parametrize(
    "test_config", [{"clusters": {"raisin": {"host": "raisin"}}}], indirect=True
)
@pytest.mark.parametrize("json_jobs", [{}], indirect=True)
@pytest.mark.usefixtures("jobless_read_write_db", "enabled_cache", "no_pkey")
def test_save_job(get_jobs, test_config, sacct_json, remote, file_regression, cli_main):
    remote.expect(
        host="raisin",
        cmd=f"export TZ=UTC && /opt/slurm/bin/sacct -X -S {_dtfmt(2023, 2, 15)} -E {_dtfmt(2023, 2, 16)} --allusers --json",
        out=sacct_json.encode("utf-8"),
    )

    # Import here so that config() is setup correctly when CLI is created.
    import sarc.cli  # noqa: F401

    assert (
        cli_main(
            [
                "fetch",
                "jobs",
                "--cluster_names",
                "raisin",
                "--intervals",
                f"{_dtfmt(2023, 2, 15)}-{_dtfmt(2023, 2, 16)}",
            ]
        )
        == 0
    )

    assert cli_main(["-v", "parse", "jobs", "--since", "2023-02-14T00:00"]) == 0

    jobs = list(get_jobs())
    assert len(jobs) == 1

    file_regression.check(
        f"Found {len(jobs)} job(s):\n"
        + "\n".join(
            [job.model_dump_json(exclude={"id": True}, indent=4) for job in jobs]
        )
    )


@pytest.mark.parametrize(
    "json_jobs",
    [
        [
            {
                "job_id": 1_000_000,
                "node": "cn-c017",
                "time": {
                    "submission": int(
                        datetime(2023, 2, 14, 0, 0, 0, tzinfo=MTL)
                        .astimezone(UTC)
                        .timestamp()
                    )
                },
                "state": {"current": ["PREEMPTED"], "reason": "None"},
            },
            {
                "job_id": 1_000_000,
                "node": "cn-b099",
                "time": {
                    "submission": int(
                        datetime(2023, 2, 15, 0, 0, 0, tzinfo=MTL)
                        .astimezone(UTC)
                        .timestamp()
                    )
                },
                "state": {"current": ["COMPLETED"], "reason": "None"},
            },
        ]
    ],
    indirect=True,
)
@pytest.mark.usefixtures("jobless_read_write_db", "enabled_cache", "no_pkey")
def test_save_preempted_job(
    get_jobs, test_config, sacct_json, remote, file_regression, cli_main
):
    remote.expect(
        cmd=f"export TZ=UTC && /opt/slurm/bin/sacct -X -S {_dtfmt(2023, 2, 15)} -E {_dtfmt(2023, 2, 16)} --allusers --json",
        host="raisin",
        out=sacct_json.encode("utf-8"),
    )

    # Import here so that config() is setup correctly when CLI is created.
    import sarc.cli  # noqa: F401

    assert (
        cli_main(
            [
                "fetch",
                "jobs",
                "--cluster_names",
                "raisin",
                "--intervals",
                f"{_dtfmt(2023, 2, 15)}-{_dtfmt(2023, 2, 16)}",
            ]
        )
        == 0
    )

    assert cli_main(["-v", "parse", "jobs", "--since", "2023-02-14T00:00"]) == 0

    jobs = list(get_jobs())
    assert len(jobs) == 2
    assert {j.job_state for j in jobs} == {SlurmState.PREEMPTED, SlurmState.COMPLETED}

    file_regression.check(
        f"Found {len(jobs)} job(s):\n"
        + "\n".join(
            [job.model_dump_json(exclude={"id": True}, indent=4) for job in jobs]
        )
    )


@pytest.mark.usefixtures("jobless_read_write_db", "enabled_cache", "no_pkey")
def test_multiple_dates(
    create_sacct_json, get_jobs, test_config, remote, file_regression, cli_main
):
    datetimes = [
        datetime(2023, 2, 15, tzinfo=MTL).astimezone(UTC) + timedelta(days=i)
        for i in range(5)
    ]

    remote.expect(
        host="raisin",
        commands=[
            Command(
                cmd=(
                    "export TZ=UTC && /opt/slurm/bin/sacct -X "
                    f"-S {job_submit_datetime.strftime('%Y-%m-%dT%H:%M')} "
                    f"-E {(job_submit_datetime + timedelta(days=1)).strftime('%Y-%m-%dT%H:%M')} "
                    "--allusers --json"
                ),
                out=create_sacct_json(
                    [
                        {
                            "job_id": job_id,
                            "time": {
                                "submission": int(job_submit_datetime.timestamp())
                            },
                        }
                    ]
                ).encode("utf-8"),
            )
            for job_id, job_submit_datetime in enumerate(datetimes)
        ],
    )

    # Import here so that config() is setup correctly when CLI is created.
    import sarc.cli  # noqa: F401

    assert (
        cli_main(
            [
                "fetch",
                "jobs",
                "--cluster_names",
                "raisin",
                "--intervals",
                f"{_dtfmt(2023, 2, 15)}-{_dtfmt(2023, 2, 16)}",
                f"{_dtfmt(2023, 2, 16)}-{_dtfmt(2023, 2, 17)}",
                f"{_dtfmt(2023, 2, 17)}-{_dtfmt(2023, 2, 18)}",
                f"{_dtfmt(2023, 2, 18)}-{_dtfmt(2023, 2, 19)}",
                f"{_dtfmt(2023, 2, 19)}-{_dtfmt(2023, 2, 20)}",
            ]
        )
        == 0
    )

    assert cli_main(["-v", "parse", "jobs", "--since", "2023-02-14T00:00"]) == 0

    jobs = list(get_jobs())
    assert len(jobs) == len(datetimes)

    file_regression.check(
        f"Found {len(jobs)} job(s):\n"
        + "\n".join(
            [job.model_dump_json(exclude={"id": True}, indent=4) for job in jobs]
        )
    )


@pytest.mark.usefixtures("jobless_read_write_db", "enabled_cache", "no_pkey")
def test_multiple_clusters_and_dates(
    get_jobs, create_sacct_json, test_config, remote, file_regression, cli_main
):
    cluster_names = ["raisin", "patate"]
    datetimes = [
        datetime(2023, 2, 15, tzinfo=MTL).astimezone(UTC) + timedelta(days=i)
        for i in range(2)
    ]

    def _create_session(cluster_name, cmd_template, datetimes):
        return Session(
            host=cluster_name,
            commands=[
                Command(
                    cmd=(
                        cmd_template.format(
                            start=job_submit_datetime.strftime("%Y-%m-%dT%H:%M"),
                            end=(job_submit_datetime + timedelta(days=1)).strftime(
                                "%Y-%m-%dT%H:%M"
                            ),
                        )
                    ),
                    out=create_sacct_json(
                        [
                            {
                                "job_id": job_id,
                                "cluster": cluster_name,
                                "time": {
                                    "submission": int(job_submit_datetime.timestamp())
                                },
                            }
                        ]
                    ).encode("utf-8"),
                )
                for job_id, job_submit_datetime in enumerate(datetimes)
            ],
        )

    remote.expect_sessions(
        _create_session(
            "raisin",
            "export TZ=UTC && /opt/slurm/bin/sacct -X -S {start} -E {end} --allusers --json",
            datetimes=datetimes,
        ),
        _create_session(
            "patate",
            (
                "export TZ=UTC && /opt/software/slurm/bin/sacct "
                "-A rrg-bonhomme-ad_gpu,rrg-bonhomme-ad_cpu,def-bonhomme_gpu,def-bonhomme_cpu "
                "-X -S {start} -E {end} --allusers --json"
            ),
            datetimes=datetimes,
        ),
    )

    # Import here so that config() is setup correctly when CLI is created.
    import sarc.cli  # noqa: F401

    assert (
        cli_main(
            [
                "fetch",
                "jobs",
                "--cluster_names",
                "raisin",
                "patate",
                "--intervals",
                f"{_dtfmt(2023, 2, 15)}-{_dtfmt(2023, 2, 16)}",
                f"{_dtfmt(2023, 2, 16)}-{_dtfmt(2023, 2, 17)}",
            ]
        )
        == 0
    )

    assert cli_main(["-v", "parse", "jobs", "--since", "2023-02-14T00:00"]) == 0

    jobs = list(get_jobs())
    assert len(jobs) == len(datetimes) * len(cluster_names)

    file_regression.check(
        f"Found {len(jobs)} job(s):\n"
        + "\n".join(
            [job.model_dump_json(exclude={"id": True}, indent=4) for job in jobs]
        )
    )


@pytest.mark.usefixtures("tzlocal_is_mtl")
@pytest.mark.parametrize(
    "json_jobs",
    [
        {
            "time": {
                "submission": int(
                    datetime(2023, 2, 15, 12, 0, 0, tzinfo=PST)
                    .astimezone(UTC)
                    .timestamp()
                )
            }
        }
    ],
    indirect=True,
)
@pytest.mark.parametrize(
    "test_config", [{"clusters": {"patate": {"host": "patate"}}}], indirect=True
)
@pytest.mark.usefixtures("jobless_read_write_db", "enabled_cache", "no_pkey")
def test_job_tz(get_jobs, test_config, sacct_json, remote, cli_main):
    remote.expect(
        host="patate",
        cmd="export TZ=UTC && /opt/software/slurm/bin/sacct -A rrg-bonhomme-ad_gpu,rrg-bonhomme-ad_cpu,def-bonhomme_gpu,def-bonhomme_cpu -X -S 2023-02-15T00:00 -E 2023-02-16T00:00 --allusers --json",
        out=sacct_json.encode("utf-8"),
    )

    assert (
        cli_main(
            [
                "fetch",
                "jobs",
                "--cluster_names",
                "patate",
                "--intervals",
                "2023-02-15T00:00-2023-02-16T00:00",
            ]
        )
        == 0
    )

    assert cli_main(["-v", "parse", "jobs", "--since", "2023-02-14T00:00"]) == 0

    jobs = list(get_jobs())
    assert len(jobs) == 1
    assert jobs[0].submit_time == datetime(2023, 2, 15, 12 + 3, 0, 0, tzinfo=MTL)


@pytest.mark.parametrize(
    "sacct_outputs",
    [
        "slurm_21_8_8.json",
        "slurm_22_5_9.json",
        "slurm_23_2_6.json",
        "slurm_23_11_5.json",
    ],
)
def test_parse_sacct_slurm_versions(sacct_outputs):
    file = Path(__file__).parent / "sacct_outputs" / sacct_outputs
    # read file content as bytes
    raw_data = file.read_bytes()
    jobs = list(
        parse_raw(
            raw_data,
            "cedar",
            datetime(2023, 2, 14, tzinfo=MTL).astimezone(UTC),
            datetime(2023, 2, 15, tzinfo=MTL).astimezone(UTC),
        )
    )
    assert len(jobs) == 1


@pytest.mark.usefixtures("jobless_read_write_db", "enabled_cache")
def test_acquire_jobs_mutually_exclusive_args(get_jobs, cli_main, caplog):
    # Both --intervals and --auto_interval: must fail
    assert (
        cli_main(
            [
                "fetch",
                "jobs",
                "--cluster_names",
                "raisin",
                "--intervals",
                "2023-02-15T00:00-2023-02-16T00:00",
                "--auto_interval",
                "10",
            ]
        )
        == -1
    )

    assert cli_main(["-v", "parse", "jobs", "--since", "2023-02-14T00:00"]) == 0

    assert not list(get_jobs())
    assert (
        "Parameters mutually exclusive: either --intervals or --auto_interval, not both"
        in caplog.text
    )


@pytest.mark.usefixtures("jobless_read_write_db", "enabled_cache")
def test_acquire_jobs_invalid_interval(get_jobs, cli_main, caplog):
    # Malformed interval
    assert (
        cli_main(
            [
                "fetch",
                "jobs",
                "--cluster_names",
                "raisin",
                "--intervals",
                "2023-02-15x00:00-2023-02-16T00:00",
            ]
        )
        == 0
    )

    assert cli_main(["-v", "parse", "jobs", "--since", "2023-02-14T00:00"]) == 0

    assert not list(get_jobs())
    assert (
        "Invalid interval 2023-02-15x00:00-2023-02-16T00:00 ; skipping cluster"
        in caplog.text
    )


@pytest.mark.usefixtures("jobless_read_write_db", "enabled_cache")
def test_acquire_jobs_interval_start_gt_end(get_jobs, cli_main, caplog):
    # Malformed interval: start > end
    assert (
        cli_main(
            [
                "fetch",
                "jobs",
                "--cluster_names",
                "raisin",
                "--intervals",
                "2023-02-17T00:00-2023-02-16T00:00",
            ]
        )
        == 0
    )

    assert cli_main(["-v", "parse", "jobs", "--since", "2023-02-14T00:00"]) == 0
    assert not list(get_jobs())
    assert (
        "Interval: 2023-02-17 00:00:00+00:00 > 2023-02-16 00:00:00+00:00 ; skipping cluster"
        in caplog.text
    )


@pytest.mark.usefixtures("jobless_read_write_db", "enabled_cache")
def test_acquire_jobs_args_no_interval(get_jobs, cli_main, caplog):
    # No interval, nothing to do
    assert cli_main(["fetch", "jobs", "--cluster_names", "raisin"]) == 0

    assert cli_main(["-v", "parse", "jobs", "--since", "2023-02-14T00:00"]) == 0

    assert not list(get_jobs())
    assert "No --intervals or --auto_interval parsed, nothing to do." in caplog.text


def get_raisin():
    with config().db.session() as sess:
        return sess.exec(
            select(SlurmClusterDB).where(SlurmClusterDB.name == "raisin")
        ).one_or_none()


@pytest.mark.usefixtures("read_write_db", "enabled_cache")
def test_auto_interval(cli_main, monkeypatch, time_machine, caplog):
    """Test auto_interval and check updated end time."""

    def mock_fetch_raw(*args, **kwargs):
        mock_fetch_raw.called += 1
        return '{"jobs": []}'

    mock_fetch_raw.called = 0

    import sarc.core.scraping.jobs_utils

    monkeypatch.setattr(sarc.core.scraping.jobs, "fetch_raw", mock_fetch_raw)

    orig_end_time = get_raisin().end_time_sacct

    expected_final_end_time = orig_end_time + timedelta(minutes=300)
    time_machine.move_to(orig_end_time + timedelta(minutes=301), tick=False)

    assert (
        cli_main(
            [
                "-v",
                "fetch",
                "jobs",
                "--cluster_names",
                "raisin",
                "--auto_interval",
                "60",
            ]
        )
        == 0
    )

    # end_time_sacct should have been updated
    assert get_raisin().end_time_sacct == expected_final_end_time

    # 300 minutes every 60 minutes => 5 intervals => 5 cached files
    assert mock_fetch_raw.called == 5


@pytest.mark.usefixtures("read_write_db", "enabled_cache")
def test_auto_interval_0(cli_main, monkeypatch, time_machine, caplog):
    """Test auto_interval with unique interval and check generated cache files."""

    def mock_fetch_raw(*args, **kwargs):
        mock_fetch_raw.called += 1
        return '{"jobs": []}'

    mock_fetch_raw.called = 0

    import sarc.core.scraping.jobs_utils

    monkeypatch.setattr(sarc.core.scraping.jobs, "fetch_raw", mock_fetch_raw)

    orig_end_time = get_raisin().end_time_sacct
    expected_final_end_time = orig_end_time + timedelta(minutes=300)
    time_machine.move_to(expected_final_end_time, tick=False)

    assert (
        cli_main(
            [
                "-v",
                "fetch",
                "jobs",
                "--cluster_names",
                "raisin",
                "--auto_interval",
                "0",  # no minutes => take whole time
            ]
        )
        == 0
    )
    assert cli_main(["-v", "parse", "jobs", "--since", "2024-01-01T00:00"]) == 0
    # end_time_sacct should have been updated
    assert get_raisin().end_time_sacct == expected_final_end_time

    # We expected only 1 interval and cache
    assert mock_fetch_raw.called == 1


# # ── require_user_link tests ─────────────────────────────────────────


def _fetch_one_job_on_raisin(cli_main, remote, sacct_json):
    """Helper: fetch a single default job on cluster raisin into the cache."""
    remote.expect(
        host="raisin",
        cmd=f"export TZ=UTC && /opt/slurm/bin/sacct -X -S {_dtfmt(2023, 2, 15)} -E {_dtfmt(2023, 2, 16)} --allusers --json",
        out=sacct_json.encode("utf-8"),
    )

    import sarc.cli  # noqa: F401

    assert (
        cli_main(
            [
                "fetch",
                "jobs",
                "--cluster_names",
                "raisin",
                "--intervals",
                f"{_dtfmt(2023, 2, 15)}-{_dtfmt(2023, 2, 16)}",
            ]
        )
        == 0
    )


@pytest.mark.parametrize(
    "test_config", [{"clusters": {"raisin": {"host": "raisin"}}}], indirect=True
)
@pytest.mark.parametrize("json_jobs", [{"user": "super-secret"}], indirect=True)
@pytest.mark.usefixtures("enabled_cache", "no_pkey")
def test_parse_jobs_user_link_no_match(
    get_jobs, test_config, sacct_json, remote, cli_main, jobless_read_write_db
):
    """With no matching users, no jobs are saved."""
    _fetch_one_job_on_raisin(cli_main, remote, sacct_json)

    assert cli_main(["-v", "parse", "jobs", "--since", "2023-02-14T00:00"]) == 0

    assert len(list(get_jobs())) == 0


@pytest.mark.parametrize(
    "test_config", [{"clusters": {"raisin": {"host": "raisin"}}}], indirect=True
)
@pytest.mark.parametrize("json_jobs", [{}], indirect=True)
@pytest.mark.usefixtures("enabled_cache", "no_pkey")
def test_parse_jobs_user_link_with_match(
    get_jobs, test_config, sacct_json, remote, cli_main, jobless_read_write_db
):
    """With a matching user, job is saved with user_uuid."""
    _fetch_one_job_on_raisin(cli_main, remote, sacct_json)

    assert cli_main(["-v", "parse", "jobs", "--since", "2023-02-14T00:00"]) == 0

    jobs = list(get_jobs())
    assert len(jobs) == 1
    assert jobs[0].sarc_user_id == 11
