from __future__ import annotations

import copy
import json
import logging
import re
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest
from fabric.testing.base import Command, Session

from sarc.config import MTL, PST, UTC, config
from sarc.jobs import sacct
from sarc.jobs.job import JobStatistics, get_jobs
from sarc.jobs.sacct import SAcctScraper

from .factory import JsonJobFactory, json_raw


def create_json_jobs(json_jobs: list[dict]) -> list[dict]:
    json_job_factory = JsonJobFactory()
    for job in json_jobs:
        json_job_factory.add_job(**job)

    return json_job_factory.jobs


@pytest.mark.usefixtures("standard_config")
@pytest.fixture
def json_jobs(request):
    if isinstance(request.param, dict):
        request.param = [request.param]

    return create_json_jobs(request.param)


@pytest.fixture
def sacct_json(json_jobs):
    tmp_json_raw = copy.deepcopy(json_raw)
    tmp_json_raw["jobs"] = json_jobs
    return json.dumps(tmp_json_raw)


def create_sacct_json(configs: list[dict]) -> str:
    tmp_json_raw = copy.deepcopy(json_raw)
    tmp_json_raw["jobs"] = create_json_jobs(configs)
    return json.dumps(tmp_json_raw)


parameters = {
    "user": {"user": "longbonhomme"},
    "job_state": {"state": {"current": "OUT_OF_MEMORY", "reason": "None"}},
    "signal": {
        "exit_code": {
            "status": "SIGNALED",
            "return_code": None,
            "signal": {"signal_id": 9, "name": "Killed"},
        }
    },
    "exit_code": {"exit_code": {"return_code": 1, "status": "FAILED"}},
    "time_limit": {"time": {"limit": 12345}},  # 12345 * 60 = 740700 secs
    "submit_time": {
        "time": {
            "submission": int(
                datetime(2023, 2, 24, tzinfo=MTL).astimezone(UTC).timestamp()
            )
        }
    },
    "dont_trust_start_time": {
        "time": {
            "submission": int(
                datetime(2023, 2, 24, 0, 0, 0, tzinfo=MTL).astimezone(UTC).timestamp()
            ),
            "start": int(
                datetime(2023, 2, 24, 0, 0, 0, tzinfo=MTL).astimezone(UTC).timestamp()
            ),
            "end": int(
                datetime(2023, 2, 25, 0, 0, 0, tzinfo=MTL).astimezone(UTC).timestamp()
            ),
            "elapsed": 60,
        }
    },
    "end_time": {
        "time": {
            "end": int(datetime(2023, 2, 15, tzinfo=MTL).astimezone(UTC).timestamp())
        }
    },
    "no_end_time": {"time": {"end": None}},
    "nodes": {"nodes": "node1,node[3-5]"},
    "flags": {
        "flags": ["CLEAR_SCHEDULING", "STARTED_ON_SUBMIT"],
    },
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
            "task_id": 10,
        },
        "job_id": 29036725,
    },
}


@pytest.fixture
def scraper():
    return SAcctScraper(cluster=config().clusters["raisin"], day=datetime(2023, 2, 14))


@pytest.mark.usefixtures("standard_config")
@pytest.mark.usefixtures("tzlocal_is_mtl")
@pytest.mark.parametrize(
    "json_jobs", parameters.values(), ids=parameters.keys(), indirect=True
)
def test_parse_json_job(json_jobs, scraper, file_regression):
    file_regression.check(scraper.convert(json_jobs[0]).json(indent=4))


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
@pytest.mark.usefixtures("standard_config")
def test_parse_malformed_jobs(sacct_json, scraper, capsys):
    scraper.results = json.loads(sacct_json)
    assert list(scraper) == []
    assert (
        """\
There was a problem with this entry:
====================================
{'account': 'mila',
"""
        in capsys.readouterr().err
    )


@pytest.mark.usefixtures("standard_config")
@pytest.mark.usefixtures("tzlocal_is_mtl")
@pytest.mark.parametrize(
    "json_jobs",
    [{"group": None}],
    indirect=True,
)
def test_parse_no_group_jobs(sacct_json, scraper, caplog):
    scraper.results = json.loads(sacct_json)
    with caplog.at_level("DEBUG"):
        assert list(scraper) == []
    assert 'Skipping job with group "None": 1' in caplog.text


@pytest.mark.usefixtures("standard_config")
@pytest.mark.usefixtures("tzlocal_is_mtl")
@pytest.mark.parametrize(
    "json_jobs",
    [{"cluster": "patate"}],
    indirect=True,
)
def test_scrape_lost_job_on_wrong_cluster(sacct_json, scraper, caplog):
    scraper.results = json.loads(sacct_json)
    with caplog.at_level("WARNING"):
        jobs = list(scraper)

    assert len(jobs) == 1
    assert scraper.cluster.name == "raisin"
    assert jobs[0].cluster_name == "raisin"

    assert (
        'Job 1 from cluster "raisin" has a different cluster name: "patate". Using "raisin"'
        in caplog.text
    )


@pytest.mark.usefixtures("standard_config")
@pytest.mark.usefixtures("tzlocal_is_mtl")
@pytest.mark.parametrize("json_jobs", [{}], indirect=True)
def test_scraper_with_cache(scraper, sacct_json, file_regression):
    # We'd like to test that this starts with "/tmp/pytest",
    # but this isn't the case when we run the tests on Mac OS,
    # ending up in '/private/var/folders/*/pytest-of-gyomalin/pytest-63'.
    assert "pytest" in str(scraper.cachefile)

    with open(scraper.cachefile, "w") as f:
        f.write(sacct_json)

    jobs = list(scraper)

    file_regression.check("\n".join([job.json(indent=1) for job in jobs]))


@pytest.mark.usefixtures("tzlocal_is_mtl")
@pytest.mark.parametrize(
    "test_config", [{"clusters": {"raisin": {"host": "patate"}}}], indirect=True
)
def test_scraper_with_malformed_cache(test_config, remote, scraper, caplog):
    # see remark in `test_scraper_with_cache` for that "pytest" substring check
    assert "pytest" in str(scraper.cachefile)

    with open(scraper.cachefile, "w") as f:
        f.write("I am malformed!! :'(")

    channel = remote.expect(
        host="patate",
        cmd="/opt/slurm/bin/sacct  -X -S '2023-02-14T00:00' -E '2023-02-15T00:00' --allusers --json",
        out=b"{}",
    )

    with caplog.at_level("WARNING"):
        assert len(scraper.get_raw()) == 0

    assert "Need to re-fetch because cache has malformed JSON." in caplog.text


@pytest.mark.parametrize(
    "test_config", [{"clusters": {"patate": {"host": "patate"}}}], indirect=True
)
def test_sacct_bin_and_accounts(test_config, remote):
    scraper = SAcctScraper(
        cluster=config().clusters["patate"], day=datetime(2023, 2, 14)
    )
    channel = remote.expect(
        host="patate",
        cmd="/opt/software/slurm/bin/sacct -A rrg-bonhomme-ad_gpu,rrg-bonhomme-ad_cpu,def-bonhomme_gpu,def-bonhomme_cpu -X -S '2023-02-14T00:00' -E '2023-02-15T00:00' --allusers --json",
        out=b'{"jobs": []}',
    )

    assert len(list(scraper)) == 0


@patch("os.system")
@pytest.mark.usefixtures("write_setup")
def test_localhost(os_system, monkeypatch):
    def mock_subprocess_run(*args, **kwargs):
        mock_subprocess_run.called += 1
        return subprocess.CompletedProcess(
            args=args, returncode=0, stdout='{"jobs": []}', stderr=""
        )

    mock_subprocess_run.called = 0

    monkeypatch.setattr(subprocess, "run", mock_subprocess_run)

    scraper = SAcctScraper(
        cluster=config().clusters["local"], day=datetime(2023, 2, 14)
    )

    assert len(list(scraper)) == 0
    assert mock_subprocess_run.called >= 1


@pytest.mark.parametrize(
    "test_config", [{"clusters": {"raisin": {"host": "raisin"}}}], indirect=True
)
@pytest.mark.parametrize("json_jobs", [{}], indirect=True)
@pytest.mark.usefixtures("empty_read_write_db")
def test_stdout_message_before_json(
    test_config, sacct_json, remote, file_regression, cli_main, prom_custom_query_mock
):
    channel = remote.expect(
        host="raisin",
        cmd="/opt/slurm/bin/sacct  -X -S '2023-02-15T00:00' -E '2023-02-16T00:00' --allusers --json",
        out=f"Welcome on raisin,\nThe sweetest supercomputer in the world!\n{sacct_json}".encode(
            "utf-8"
        ),
    )

    # Import here so that config() is setup correctly when CLI is created.
    import sarc.cli

    assert (
        cli_main(
            [
                "acquire",
                "jobs",
                "--cluster_name",
                "raisin",
                "--dates",
                "2023-02-15",
                "--no_prometheus",
            ]
        )
        == 0
    )

    jobs = list(get_jobs())
    file_regression.check(
        f"Found {len(jobs)} job(s):\n"
        + "\n".join([job.json(exclude={"id": True}, indent=4) for job in jobs])
    )


@pytest.mark.parametrize(
    "test_config", [{"clusters": {"raisin": {"host": "raisin"}}}], indirect=True
)
@pytest.mark.parametrize("json_jobs", [{}], indirect=True)
@pytest.mark.usefixtures("empty_read_write_db")
def test_get_gpu_type_from_prometheus(
    test_config, sacct_json, remote, file_regression, cli_main, monkeypatch
):
    channel = remote.expect(
        host="raisin",
        cmd="/opt/slurm/bin/sacct  -X -S '2023-02-15T00:00' -E '2023-02-16T00:00' --allusers --json",
        out=f"Welcome on raisin,\nThe sweetest supercomputer in the world!\n{sacct_json}".encode(
            "utf-8"
        ),
    )

    # Import here so that config() is setup correctly when CLI is created.
    from prometheus_api_client import PrometheusConnect

    import sarc.cli

    def mock_compute_job_statistics(job):
        mock_compute_job_statistics.called += 1
        return JobStatistics()

    mock_compute_job_statistics.called = 0

    monkeypatch.setattr(
        "sarc.jobs.series.compute_job_statistics", mock_compute_job_statistics
    )

    def mock_get_job_time_series(job, metric, **kwargs):
        assert metric == "slurm_job_utilization_gpu_memory"
        assert job.job_id == 1
        return [{"metric": {"gpu_type": "phantom_gpu"}}]

    monkeypatch.setattr(sacct, "get_job_time_series", mock_get_job_time_series)

    assert (
        cli_main(
            [
                "acquire",
                "jobs",
                "--cluster_name",
                "raisin",
                "--dates",
                "2023-02-15",
            ]
        )
        == 0
    )

    jobs = list(get_jobs())

    assert len(jobs) == 1
    job = jobs[0]
    assert job.allocated.gpu_type == "phantom_gpu"

    file_regression.check(
        f"Found {len(jobs)} job(s):\n"
        + "\n".join([job.json(exclude={"id": True}, indent=4) for job in jobs])
    )


@pytest.mark.parametrize(
    "test_config",
    [{"clusters": {"raisin_no_prometheus": {"host": "raisin_no_prometheus"}}}],
    indirect=True,
)
@pytest.mark.parametrize("json_jobs", [{}], indirect=True)
@pytest.mark.usefixtures("empty_read_write_db")
def test_get_gpu_type_without_prometheus(
    test_config, sacct_json, remote, file_regression, cli_main, monkeypatch
):
    channel = remote.expect(
        host="raisin_no_prometheus",
        cmd="/opt/slurm/bin/sacct  -X -S '2023-02-15T00:00' -E '2023-02-16T00:00' --allusers --json",
        out=f"Welcome on raisin_no_prometheus,\nThe sweetest supercomputer in the world!\n{sacct_json}".encode(
            "utf-8"
        ),
    )

    # Import here so that config() is setup correctly when CLI is created.
    import sarc.cli

    assert (
        cli_main(
            [
                "acquire",
                "jobs",
                "--cluster_name",
                "raisin_no_prometheus",
                "--dates",
                "2023-02-15",
            ]
        )
        == 0
    )

    jobs = list(get_jobs())

    assert len(jobs) == 1
    job = jobs[0]
    print(job)
    print(job.nodes)
    assert job.allocated.gpu_type == "gpu:asupergpu:4"

    file_regression.check(
        f"Found {len(jobs)} job(s):\n"
        + "\n".join([job.json(exclude={"id": True}, indent=4) for job in jobs])
    )


@pytest.mark.parametrize(
    "test_config", [{"clusters": {"raisin": {"host": "raisin"}}}], indirect=True
)
@pytest.mark.parametrize("json_jobs", [{}], indirect=True)
@pytest.mark.usefixtures("empty_read_write_db")
def test_save_job(
    test_config, sacct_json, remote, file_regression, cli_main, prom_custom_query_mock
):
    channel = remote.expect(
        host="raisin",
        cmd="/opt/slurm/bin/sacct  -X -S '2023-02-15T00:00' -E '2023-02-16T00:00' --allusers --json",
        out=sacct_json.encode("utf-8"),
    )

    # Import here so that config() is setup correctly when CLI is created.
    import sarc.cli

    assert (
        cli_main(
            [
                "acquire",
                "jobs",
                "--cluster_name",
                "raisin",
                "--dates",
                "2023-02-15",
                "--no_prometheus",
            ]
        )
        == 0
    )

    jobs = list(get_jobs())
    file_regression.check(
        f"Found {len(jobs)} job(s):\n"
        + "\n".join([job.json(exclude={"id": True}, indent=4) for job in jobs])
    )


@pytest.mark.parametrize(
    "test_config", [{"clusters": {"raisin": {"host": "raisin"}}}], indirect=True
)
@pytest.mark.parametrize("json_jobs", [{}], indirect=True)
@pytest.mark.usefixtures("empty_read_write_db", "disabled_cache")
def test_update_job(
    test_config, sacct_json, remote, file_regression, cli_main, prom_custom_query_mock
):
    channel = remote.expect(
        host="raisin",
        commands=[
            Command(
                cmd="/opt/slurm/bin/sacct  -X -S '2023-02-15T00:00' -E '2023-02-16T00:00' --allusers --json",
                out=sacct_json.encode("utf-8"),
            )
            for _ in range(2)
        ],
    )

    # Import here so that config() is setup correctly when CLI is created.
    import sarc.cli

    assert (
        cli_main(
            [
                "acquire",
                "jobs",
                "--cluster_name",
                "raisin",
                "--dates",
                "2023-02-15",
                "--no_prometheus",
            ]
        )
        == 0
    )

    assert len(list(get_jobs())) == 1

    assert (
        cli_main(
            [
                "acquire",
                "jobs",
                "--cluster_name",
                "raisin",
                "--dates",
                "2023-02-15",
                "--no_prometheus",
            ]
        )
        == 0
    )

    jobs = list(get_jobs())

    assert len(list(get_jobs())) == 1

    file_regression.check(
        f"Found {len(jobs)} job(s):\n"
        + "\n".join([job.json(exclude={"id": True}, indent=4) for job in jobs])
    )


@pytest.mark.parametrize(
    "json_jobs",
    [
        [
            {
                "job_id": 1_000_000,
                "node": "cn-c017",
                "state": {"current": "PREEMPTED", "reason": "None"},
            },
            {
                "job_id": 1_000_000,
                "node": "cn-b099",
                "state": {"current": "COMPLETED", "reason": "None"},
            },
        ]
    ],
    indirect=True,
)
@pytest.mark.usefixtures("empty_read_write_db", "disabled_cache")
def test_save_preempted_job(
    test_config, sacct_json, remote, file_regression, cli_main, prom_custom_query_mock
):
    channel = remote.expect(
        cmd="/opt/slurm/bin/sacct  -X -S '2023-02-15T00:00' -E '2023-02-16T00:00' --allusers --json",
        host="raisin",
        out=sacct_json.encode("utf-8"),
    )

    # Import here so that config() is setup correctly when CLI is created.
    import sarc.cli

    assert (
        cli_main(
            [
                "acquire",
                "jobs",
                "--cluster_name",
                "raisin",
                "--dates",
                "2023-02-15",
                "--no_prometheus",
            ]
        )
        == 0
    )

    jobs = list(get_jobs())

    assert len(list(get_jobs())) == 2

    file_regression.check(
        f"Found {len(jobs)} job(s):\n"
        + "\n".join([job.json(exclude={"id": True}, indent=4) for job in jobs])
    )


@pytest.mark.usefixtures("empty_read_write_db", "disabled_cache")
def test_multiple_dates(
    test_config, remote, file_regression, cli_main, prom_custom_query_mock
):
    datetimes = [
        datetime(2023, 2, 15, tzinfo=MTL) + timedelta(days=i) for i in range(5)
    ]
    channel = remote.expect(
        host="raisin",
        commands=[
            Command(
                cmd=(
                    "/opt/slurm/bin/sacct  -X "
                    f"-S '{job_submit_datetime.strftime('%Y-%m-%dT%H:%M')}' "
                    f"-E '{(job_submit_datetime + timedelta(days=1)).strftime('%Y-%m-%dT%H:%M')}' "
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
    import sarc.cli

    assert (
        cli_main(
            [
                "acquire",
                "jobs",
                "--cluster_name",
                "raisin",
                "--dates",
                "2023-02-15",
                "2023-02-16-2023-02-20",
                "--no_prometheus",
            ]
        )
        == 0
    )

    jobs = list(get_jobs())

    assert len(list(get_jobs())) == len(datetimes)

    file_regression.check(
        f"Found {len(jobs)} job(s):\n"
        + "\n".join([job.json(exclude={"id": True}, indent=4) for job in jobs])
    )


@pytest.mark.usefixtures("empty_read_write_db", "disabled_cache")
def test_multiple_clusters_and_dates(
    test_config,
    remote,
    file_regression,
    cli_main,
    prom_custom_query_mock,
    caplog,
    captrace,
):
    caplog.set_level(logging.INFO)
    cluster_names = ["raisin", "patate"]
    datetimes = [
        datetime(2023, 2, 15, tzinfo=MTL) + timedelta(days=i) for i in range(2)
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

    channel = remote.expect_sessions(
        _create_session(
            "raisin",
            "/opt/slurm/bin/sacct  -X -S '{start}' -E '{end}' --allusers --json",
            datetimes=datetimes,
        ),
        _create_session(
            "patate",
            (
                "/opt/software/slurm/bin/sacct "
                "-A rrg-bonhomme-ad_gpu,rrg-bonhomme-ad_cpu,def-bonhomme_gpu,def-bonhomme_cpu "
                "-X -S '{start}' -E '{end}' --allusers --json"
            ),
            datetimes=datetimes,
        ),
    )

    # Import here so that config() is setup correctly when CLI is created.
    import sarc.cli

    assert (
        cli_main(
            [
                "acquire",
                "jobs",
                "--cluster_name",
                "raisin",
                "patate",
                "--dates",
                "2023-02-15",
                "2023-02-16",
                "--no_prometheus",
            ]
        )
        == 0
    )

    jobs = list(get_jobs())

    assert len(list(get_jobs())) == len(datetimes) * len(cluster_names)

    file_regression.check(
        f"Found {len(jobs)} job(s):\n"
        + "\n".join([job.json(exclude={"id": True}, indent=4) for job in jobs])
    )

    # Check logging
    assert bool(
        re.search(
            r"root:jobs\.py:[0-9]+ Acquire data on raisin for date: 2023-02-15 00:00:00 \(is_auto=False\)",
            caplog.text,
        )
    )
    assert bool(
        re.search(
            r"root:jobs\.py:[0-9]+ Acquire data on patate for date: 2023-02-15 00:00:00 \(is_auto=False\)",
            caplog.text,
        )
    )

    # Check trace
    spans = captrace.get_finished_spans()

    # Expected spans. Each list has format
    # [span name, [event names], {key-value attributes}]
    expected_spans = [
        # patate, 2023-02-16
        [
            "acquire_cluster_data",
            ["Acquire data on patate for date: 2023-02-16 00:00:00 (is_auto=False)"],
            {
                "cluster_name": "patate",
                "date": "2023-02-16 00:00:00",
                "is_auto": False,
            },
        ],
        [
            "sacct_mongodb_import",
            ["Getting the sacct data...", "Saving into mongodb collection 'jobs'..."],
            {},
        ],
        ["get_raw", ["Getting results ..."], {}],
        ["get_raw", ["Getting results ..."], {}],
        ["get_raw", ["Getting results ..."], {}],
        ["get_raw", ["Getting results ..."], {}],
        ["get_raw", ["Fetching raw ..."], {}],
        # patate, 2023-02-15
        [
            "acquire_cluster_data",
            ["Acquire data on patate for date: 2023-02-15 00:00:00 (is_auto=False)"],
            {
                "cluster_name": "patate",
                "date": "2023-02-15 00:00:00",
                "is_auto": False,
            },
        ],
        [
            "sacct_mongodb_import",
            ["Getting the sacct data...", "Saving into mongodb collection 'jobs'..."],
            {},
        ],
        ["get_raw", ["Getting results ..."], {}],
        ["get_raw", ["Getting results ..."], {}],
        ["get_raw", ["Getting results ..."], {}],
        ["get_raw", ["Getting results ..."], {}],
        ["get_raw", ["Fetching raw ..."], {}],
        # raisin, 2023-02-16
        [
            "acquire_cluster_data",
            ["Acquire data on raisin for date: 2023-02-16 00:00:00 (is_auto=False)"],
            {
                "cluster_name": "raisin",
                "date": "2023-02-16 00:00:00",
                "is_auto": False,
            },
        ],
        [
            "sacct_mongodb_import",
            ["Getting the sacct data...", "Saving into mongodb collection 'jobs'..."],
            {},
        ],
        ["get_raw", ["Getting results ..."], {}],
        ["get_raw", ["Getting results ..."], {}],
        ["get_raw", ["Getting results ..."], {}],
        ["get_raw", ["Getting results ..."], {}],
        ["get_raw", ["Fetching raw ..."], {}],
        # raisin, 2023-02-15
        [
            "acquire_cluster_data",
            ["Acquire data on raisin for date: 2023-02-15 00:00:00 (is_auto=False)"],
            {
                "cluster_name": "raisin",
                "date": "2023-02-15 00:00:00",
                "is_auto": False,
            },
        ],
        [
            "sacct_mongodb_import",
            ["Getting the sacct data...", "Saving into mongodb collection 'jobs'..."],
            {},
        ],
        ["get_raw", ["Getting results ..."], {}],
        ["get_raw", ["Getting results ..."], {}],
        ["get_raw", ["Getting results ..."], {}],
        ["get_raw", ["Getting results ..."], {}],
        ["get_raw", ["Fetching raw ..."], {}],
    ]

    assert len(spans) == len(expected_spans)
    for i, (span_name, event_names, attributes) in enumerate(reversed(expected_spans)):
        # Check span name
        assert spans[i].name == span_name
        # Check span event count
        assert len(spans[i].events) == len(event_names)
        # Check span event names
        for j, event_name in enumerate(event_names):
            assert spans[i].events[j].name == event_name
        # Check span attributes
        for key, value in attributes.items():
            assert spans[i].attributes[key] == value


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
                ),
            }
        }
    ],
    indirect=True,
)
@pytest.mark.parametrize(
    "test_config", [{"clusters": {"patate": {"host": "patate"}}}], indirect=True
)
@pytest.mark.usefixtures("empty_read_write_db")
def test_job_tz(test_config, sacct_json, remote, cli_main, prom_custom_query_mock):
    channel = remote.expect(
        host="patate",
        cmd="/opt/software/slurm/bin/sacct -A rrg-bonhomme-ad_gpu,rrg-bonhomme-ad_cpu,def-bonhomme_gpu,def-bonhomme_cpu -X -S '2023-02-15T00:00' -E '2023-02-16T00:00' --allusers --json",
        out=sacct_json.encode("utf-8"),
    )

    assert (
        cli_main(
            [
                "acquire",
                "jobs",
                "--cluster_name",
                "patate",
                "--dates",
                "2023-02-15",
                "--no_prometheus",
            ]
        )
        == 0
    )

    jobs = list(get_jobs())
    assert len(jobs) == 1
    assert jobs[0].submit_time == datetime(2023, 2, 15, 12 + 3, 0, 0, tzinfo=MTL)


@pytest.mark.usefixtures("tzlocal_is_mtl")
@pytest.mark.parametrize("json_jobs", [{}], indirect=True)
@pytest.mark.usefixtures("empty_read_write_db")
@pytest.mark.parametrize("no_prometheus", [True, False])
def test_cli_ignore_stats(
    sacct_json,
    cli_main,
    scraper,
    no_prometheus,
    monkeypatch,
    prom_custom_query_mock,
):
    # We'd like to test that this starts with "/tmp/pytest",
    # but this isn't the case when we run the tests on Mac OS,
    # ending up in '/private/var/folders/*/pytest-of-gyomalin/pytest-63'.
    assert "pytest" in str(scraper.cachefile)

    print(scraper.cachefile)

    with open(scraper.cachefile, "w") as f:
        f.write(sacct_json)

    def mock_compute_job_statistics(job):
        mock_compute_job_statistics.called += 1
        return JobStatistics()

    mock_compute_job_statistics.called = 0

    monkeypatch.setattr(
        "sarc.jobs.series.compute_job_statistics", mock_compute_job_statistics
    )

    args = [
        "acquire",
        "jobs",
        "--cluster_name",
        "raisin",
        "--dates",
        "2023-02-14",
    ]

    if no_prometheus:
        args += ["--no_prometheus"]

    assert len(list(get_jobs())) == 0

    assert cli_main(args) == 0

    assert len(list(get_jobs())) > 0

    if no_prometheus:
        assert mock_compute_job_statistics.called == 0
    else:
        assert mock_compute_job_statistics.called >= 1


@pytest.mark.usefixtures("standard_config")
@pytest.mark.parametrize(
    "sacct_outputs",
    ["slurm_21_8_8.json", "slurm_22_5_9.json", "slurm_23_2_6.json"],
)
def test_parse_sacct_slurm_versions(sacct_outputs, scraper):
    file = Path(__file__).parent / "sacct_outputs" / sacct_outputs
    scraper.results = json.load(open(file, "r", encoding="utf8"))
    jobs = list(scraper)
    assert len(jobs) == 1
