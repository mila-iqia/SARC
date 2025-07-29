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
from opentelemetry.trace import StatusCode

from sarc.client.job import JobStatistics, get_jobs
from sarc.config import MTL, PST, UTC, config
from sarc.jobs import prometheus_scraping
from sarc.jobs.sacct import SAcctScraper

from .factory import JsonJobFactory, json_raw


def create_json_jobs(json_jobs: list[dict]) -> list[dict]:
    json_job_factory = JsonJobFactory()
    for job in json_jobs:
        json_job_factory.add_job(**job)

    return json_job_factory.jobs


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


@pytest.mark.usefixtures("tzlocal_is_mtl")
@pytest.mark.parametrize(
    "json_jobs", parameters.values(), ids=parameters.keys(), indirect=True
)
def test_parse_json_job(json_jobs, scraper, file_regression):
    file_regression.check(
        scraper.convert(json_jobs[0]).model_dump_json(exclude={"id": True}, indent=4)
    )


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
def test_parse_malformed_jobs(sacct_json, scraper, captrace):
    scraper.get_raw._save_for_key(
        key=scraper.get_raw.key(), value=json.loads(sacct_json)
    )
    with pytest.raises(KeyError):
        list(scraper)
    spans = captrace.get_finished_spans()
    assert len(spans) > 0
    # Just check the span that should have got an error.
    error_spans = [
        span for span in spans if span.status.status_code == StatusCode.ERROR
    ]
    assert len(error_spans) == 1
    (error_span,) = error_spans
    assert error_span.name == "SAcctScraper.__iter__"
    entry = json.loads(error_span.attributes["entry"])
    assert isinstance(entry, dict)
    assert entry["account"] == "mila"


@pytest.mark.usefixtures("tzlocal_is_mtl")
@pytest.mark.parametrize(
    "json_jobs",
    [{"group": None}],
    indirect=True,
)
def test_parse_no_group_jobs(sacct_json, scraper, caplog):
    scraper.get_raw._save_for_key(
        key=scraper.get_raw.key(), value=json.loads(sacct_json)
    )
    with caplog.at_level("DEBUG"):
        assert list(scraper) == [None]
    assert 'Skipping job with group "None": 1' in caplog.text


@pytest.mark.usefixtures("tzlocal_is_mtl")
@pytest.mark.parametrize(
    "json_jobs",
    [{"cluster": "patate"}],
    indirect=True,
)
def test_scrape_lost_job_on_wrong_cluster(sacct_json, scraper, caplog):
    scraper.get_raw._save_for_key(
        key=scraper.get_raw.key(), value=json.loads(sacct_json)
    )
    with caplog.at_level("WARNING"):
        jobs = list(scraper)

    assert len(jobs) == 1
    assert scraper.cluster.name == "raisin"
    assert jobs[0].cluster_name == "raisin"

    assert (
        'Job 1 from cluster "raisin" has a different cluster name: "patate". Using "raisin"'
        in caplog.text
    )


@pytest.mark.usefixtures("tzlocal_is_mtl", "enabled_cache")
@pytest.mark.parametrize("json_jobs", [{}], indirect=True)
def test_scraper_with_cache(scraper, sacct_json, file_regression):
    # We'd like to test that this starts with "/tmp/pytest",
    # but this isn't the case when we run the tests on Mac OS,
    # ending up in '/private/var/folders/*/pytest-of-gyomalin/pytest-63'.
    assert "pytest" in str(scraper.get_raw.cache_dir)

    scraper.get_raw.cache_dir.mkdir(parents=True, exist_ok=True)

    cache_path = scraper.get_raw.cache_dir / scraper.get_raw.key().format(
        time=datetime.now()
    )

    with open(cache_path, "w") as f:
        f.write(sacct_json)

    jobs = list(scraper)

    file_regression.check(
        "\n".join([job.model_dump_json(exclude={"id": True}, indent=1) for job in jobs])
    )


@pytest.mark.usefixtures("tzlocal_is_mtl", "enabled_cache")
@pytest.mark.parametrize(
    "test_config", [{"clusters": {"raisin": {"host": "patate"}}}], indirect=True
)
def test_scraper_with_malformed_cache(test_config, remote, scraper, caplog):
    # see remark in `test_scraper_with_cache` for that "pytest" substring check
    assert "pytest" in str(scraper.get_raw.cache_dir)

    scraper.get_raw.cache_dir.mkdir(parents=True, exist_ok=True)

    cache_path = scraper.get_raw.cache_dir / scraper.get_raw.key().format(
        time=datetime.now()
    )

    with open(cache_path, "w") as f:
        f.write("I am malformed!! :'(")

    remote.expect(
        host="patate",
        cmd="export TZ=UTC && /opt/slurm/bin/sacct  -X -S 2023-02-14T00:00 -E 2023-02-15T00:00 --allusers --json",
        out=b"{}",
    )

    with caplog.at_level("WARNING"):
        assert len(scraper.get_raw()) == 0

    assert "Could not load malformed cache file" in caplog.text


@pytest.mark.parametrize(
    "test_config", [{"clusters": {"patate": {"host": "patate"}}}], indirect=True
)
def test_sacct_bin_and_accounts(test_config, remote):
    scraper = SAcctScraper(
        cluster=config().clusters["patate"], day=datetime(2023, 2, 14)
    )
    remote.expect(
        host="patate",
        cmd="export TZ=UTC && /opt/software/slurm/bin/sacct -A rrg-bonhomme-ad_gpu,rrg-bonhomme-ad_cpu,def-bonhomme_gpu,def-bonhomme_cpu -X -S 2023-02-14T00:00 -E 2023-02-15T00:00 --allusers --json",
        out=b'{"jobs": []}',
    )

    assert len(list(scraper)) == 0


@patch("os.system")
@pytest.mark.usefixtures("write_setup")
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
    remote.expect(
        host="raisin",
        cmd="export TZ=UTC && /opt/slurm/bin/sacct  -X -S 2023-02-15T00:00 -E 2023-02-16T00:00 --allusers --json",
        out=f"Welcome on raisin,\nThe sweetest supercomputer in the world!\n{sacct_json}".encode(
            "utf-8"
        ),
    )

    # Import here so that config() is setup correctly when CLI is created.
    import sarc.cli  # noqa: F401

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
    file_regression.check(
        f"Found {len(jobs)} job(s):\n"
        + "\n".join(
            [job.model_dump_json(exclude={"id": True}, indent=4) for job in jobs]
        )
    )


@pytest.mark.parametrize(
    "test_config",
    [{"clusters": {"raisin": {"host": "raisin"}}}],
    indirect=True,
)
@pytest.mark.parametrize(
    "json_jobs",
    [
        {
            "tres": {
                "allocated": [
                    {
                        "count": 1,
                        "id": 1002,
                        "name": "gpu:gpu_name_from_sacct",
                        "type": "gres",
                    },
                ],
            }
        }
    ],
    indirect=True,
)
@pytest.mark.usefixtures("empty_read_write_db", "enabled_cache")
def test_get_gpu_type(test_config, sacct_json, remote, cli_main, monkeypatch):
    """Test all 3 sources of GPU type (sacct, node->GPU and prometheus)"""

    remote.expect(
        host="raisin",
        cmd="export TZ=UTC && /opt/slurm/bin/sacct  -X -S 2023-02-15T00:00 -E 2023-02-16T00:00 --allusers --json",
        out=f"Welcome on raisin,\nThe sweetest supercomputer in the world!\n{sacct_json}".encode(
            "utf-8"
        ),
    )

    cmd_sacct = [
        "acquire",
        "jobs",
        "--cluster_name",
        "raisin",
        "--dates",
        "2023-02-15",
    ]

    # Test `acquire jobs` without node->gpu available
    # -----------------------------------------------
    # Should return GPU name from sacct
    assert cli_main(cmd_sacct) == 0
    jobs = list(get_jobs())
    assert len(jobs) == 1
    job = jobs[0]
    print(job)
    print(job.nodes)
    assert job.allocated.gpu_type == "gpu_name_from_sacct"
    assert not job.stored_statistics

    # Test `acquire jobs` with node->gpu available
    # --------------------------------------------
    # node->gpu is prior to sacct data
    # Save slurm config in cache.
    _save_slurm_conf(
        "raisin",
        "2023-02-15",
        "NodeName=cn-c0[18-30] Param1=Anything1 Param2=Anything2 Gres=gpu:gpu2:4 Param3=Anything3",
    )
    # Acquire slurm config.
    assert (
        cli_main(
            [
                "acquire",
                "slurmconfig",
                "--cluster_name",
                "raisin",
                "--day",
                "2023-02-15",
            ]
        )
        == 0
    )
    # acquire jobs
    assert cli_main(cmd_sacct) == 0
    jobs = list(get_jobs())
    assert len(jobs) == 1
    job = jobs[0]
    assert job.allocated.gpu_type == "THE GPU II"
    assert not job.stored_statistics

    # Test `acquire prometheus`
    # -------------------------
    # Prometheus data is prior to node->gpu and sacct data
    def mock_compute_job_statistics(job):
        mock_compute_job_statistics.called += 1
        return JobStatistics()

    mock_compute_job_statistics.called = 0
    monkeypatch.setattr(
        "sarc.jobs.series.compute_job_statistics", mock_compute_job_statistics
    )

    def mock_get_job_time_series(job, metric, **kwargs):
        mock_get_job_time_series.called += 1
        assert metric == "slurm_job_utilization_gpu_memory"
        assert job.job_id == 1
        return [{"metric": {"gpu_type": "phantom_gpu"}}]

    mock_get_job_time_series.called = 0
    monkeypatch.setattr(
        prometheus_scraping, "get_job_time_series", mock_get_job_time_series
    )

    assert (
        cli_main(
            [
                "acquire",
                "prometheus",
                "--cluster_name",
                "raisin",
                "--dates",
                "2023-02-14",
            ]
        )
        == 0
    )
    assert mock_compute_job_statistics.called == 1
    assert mock_get_job_time_series.called == 1
    jobs = list(get_jobs())
    assert len(jobs) == 1
    job = jobs[0]
    assert job.allocated.gpu_type == "PHANTOM GPU MENACE"
    assert job.stored_statistics


def _save_slurm_conf(cluster_name: str, day: str, content: str):
    from sarc.cli.acquire.slurmconfig import SlurmConfigParser

    scp = SlurmConfigParser(config().clusters[cluster_name], day)
    folder = "slurm_conf"
    filename = scp._cache_key()
    cache_dir = config().cache
    file_dir = cache_dir / folder
    file_dir.mkdir(parents=True, exist_ok=True)
    file_path = file_dir / filename
    print(file_path)
    with file_path.open("w") as file:
        file.write(content)


@pytest.mark.parametrize(
    "test_config", [{"clusters": {"raisin": {"host": "raisin"}}}], indirect=True
)
@pytest.mark.parametrize("json_jobs", [{}], indirect=True)
@pytest.mark.usefixtures("empty_read_write_db")
def test_save_job(
    test_config, sacct_json, remote, file_regression, cli_main, prom_custom_query_mock
):
    remote.expect(
        host="raisin",
        cmd="export TZ=UTC && /opt/slurm/bin/sacct  -X -S 2023-02-15T00:00 -E 2023-02-16T00:00 --allusers --json",
        out=sacct_json.encode("utf-8"),
    )

    # Import here so that config() is setup correctly when CLI is created.
    import sarc.cli  # noqa: F401

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
@pytest.mark.usefixtures("empty_read_write_db", "disabled_cache")
def test_update_job(
    test_config, sacct_json, remote, file_regression, cli_main, prom_custom_query_mock
):
    remote.expect(
        host="raisin",
        commands=[
            Command(
                cmd="export TZ=UTC && /opt/slurm/bin/sacct  -X -S 2023-02-15T00:00 -E 2023-02-16T00:00 --allusers --json",
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
            ]
        )
        == 0
    )

    jobs = list(get_jobs())

    assert len(list(get_jobs())) == 1

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
    remote.expect(
        cmd="export TZ=UTC && /opt/slurm/bin/sacct  -X -S 2023-02-15T00:00 -E 2023-02-16T00:00 --allusers --json",
        host="raisin",
        out=sacct_json.encode("utf-8"),
    )

    # Import here so that config() is setup correctly when CLI is created.
    import sarc.cli  # noqa: F401

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

    assert len(list(get_jobs())) == 2

    file_regression.check(
        f"Found {len(jobs)} job(s):\n"
        + "\n".join(
            [job.model_dump_json(exclude={"id": True}, indent=4) for job in jobs]
        )
    )


@pytest.mark.usefixtures("empty_read_write_db", "disabled_cache")
def test_multiple_dates(
    test_config, remote, file_regression, cli_main, prom_custom_query_mock
):
    datetimes = [
        datetime(2023, 2, 15, tzinfo=MTL) + timedelta(days=i) for i in range(5)
    ]
    remote.expect(
        host="raisin",
        commands=[
            Command(
                cmd=(
                    "export TZ=UTC && /opt/slurm/bin/sacct  -X "
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
                "acquire",
                "jobs",
                "--cluster_name",
                "raisin",
                "--dates",
                "2023-02-15",
                "2023-02-16-2023-02-20",
            ]
        )
        == 0
    )

    jobs = list(get_jobs())

    assert len(list(get_jobs())) == len(datetimes)

    file_regression.check(
        f"Found {len(jobs)} job(s):\n"
        + "\n".join(
            [job.model_dump_json(exclude={"id": True}, indent=4) for job in jobs]
        )
    )


@pytest.mark.usefixtures("empty_read_write_db", "disabled_cache")
def test_multiple_clusters_and_dates(
    test_config, remote, file_regression, cli_main, prom_custom_query_mock
):
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

    remote.expect_sessions(
        _create_session(
            "raisin",
            "export TZ=UTC && /opt/slurm/bin/sacct  -X -S {start} -E {end} --allusers --json",
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
                "acquire",
                "jobs",
                "--cluster_name",
                "raisin",
                "patate",
                "--dates",
                "2023-02-15",
                "2023-02-16",
            ]
        )
        == 0
    )

    jobs = list(get_jobs())

    assert len(list(get_jobs())) == len(datetimes) * len(cluster_names)

    file_regression.check(
        f"Found {len(jobs)} job(s):\n"
        + "\n".join(
            [job.model_dump_json(exclude={"id": True}, indent=4) for job in jobs]
        )
    )


@pytest.mark.usefixtures("empty_read_write_db", "disabled_cache")
def test_tracer_with_multiple_clusters_and_dates_and_prometheus(
    test_config,
    remote,
    file_regression,
    cli_main,
    prom_custom_query_mock,
    caplog,
    captrace,
    monkeypatch,
):
    """
    Copied from test_multiple_clusters_and_dates above, with changes:
    - Added captrace to test tracing
    - Added a call to `acquire prometheus` to test prometheus-related tracing
    """

    def _setup_logging_do_nothing(*args, **kwargs):
        pass

    monkeypatch.setattr("sarc.cli.setupLogging", _setup_logging_do_nothing)
    caplog.set_level(logging.INFO)
    cluster_names = ["raisin", "patate"]
    datetimes = [
        datetime(2023, 2, 15, tzinfo=MTL) + timedelta(days=i) for i in range(2)
    ]

    def _gen_error_command(cmd_template, job_submit_datetime):
        return Command(
            cmd=(
                cmd_template.format(
                    start=job_submit_datetime.strftime("%Y-%m-%dT%H:%M"),
                    end=(job_submit_datetime + timedelta(days=1)).strftime(
                        "%Y-%m-%dT%H:%M"
                    ),
                )
            ),
            exit=1,
        )

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
            ]
            + [_gen_error_command(cmd_template, datetime(2023, 3, 16, tzinfo=MTL))],
        )

    remote.expect_sessions(
        _create_session(
            "raisin",
            "export TZ=UTC && /opt/slurm/bin/sacct  -X -S {start} -E {end} --allusers --json",
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

    def mock_compute_job_statistics(job):
        return JobStatistics()

    monkeypatch.setattr(
        "sarc.jobs.series.compute_job_statistics", mock_compute_job_statistics
    )

    def mock_get_job_time_series(job, metric, **kwargs):
        assert metric == "slurm_job_utilization_gpu_memory"
        return [
            {"metric": {"gpu_type": f"phantom_gpu_{job.cluster_name}_{job.job_id}"}}
        ]

    monkeypatch.setattr(
        prometheus_scraping, "get_job_time_series", mock_get_job_time_series
    )

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
                "2023-03-16",
            ]
        )
        == 0
    )

    assert (
        cli_main(
            [
                "acquire",
                "prometheus",
                "--cluster_name",
                "raisin",
                "patate",
                "--dates",
                "2023-02-15",
                "2023-02-16",
                "2023-03-16",
            ]
        )
        == 0
    )

    jobs = list(get_jobs())

    assert len(jobs) == len(datetimes) * len(cluster_names)

    # Check both jobs and trace in file regression
    spans = captrace.get_finished_spans()
    spans_data = [
        {
            "span_name": span.name,
            "span_events": [event.name for event in span.events],
            "span_attributes": dict(span.attributes),
            "span_has_error": span.status.status_code == StatusCode.ERROR,
        }
        for span in reversed(spans)
    ]

    file_regression.check(
        f"Found {len(jobs)} job(s):\n"
        + "\n".join(
            [job.model_dump_json(exclude={"id": True}, indent=4) for job in jobs]
        )
        + f"\n\nFound {len(spans)} span(s):\n"
        + json.dumps(spans_data, indent=1)
    )

    # Check logging
    print(caplog.text)
    assert bool(
        re.search(
            r"sarc.cli.acquire.jobs:jobs\.py:[0-9]+ Acquire data on raisin for date: 2023-02-15 00:00:00 \(is_auto=False\)",
            caplog.text,
        )
    )
    assert bool(
        re.search(
            r"sarc.cli.acquire.jobs:jobs\.py:[0-9]+ Acquire data on patate for date: 2023-02-15 00:00:00 \(is_auto=False\)",
            caplog.text,
        )
    )
    assert (
        "Getting the sacct data for cluster raisin, date 2023-02-15 00:00:00..."
        in caplog.text
    )
    assert "Saving into mongodb collection '" in caplog.text
    assert bool(
        re.search(
            r"sarc\.jobs\.sacct:sacct\.py:[0-9]+ Saved [0-9]+/[0-9]+ entries\.",
            caplog.text,
        )
    )

    # There should be 2 acquisition errors for unexpected data 2023-03-16, one per cluster.
    assert bool(
        re.search(
            r"sarc.cli.acquire.jobs:jobs\.py:[0-9]+ Failed to acquire data for raisin on 2023-03-16 00:00:00:",
            caplog.text,
        )
    )
    assert bool(
        re.search(
            r"sarc.cli.acquire.jobs:jobs\.py:[0-9]+ Failed to acquire data for patate on 2023-03-16 00:00:00:",
            caplog.text,
        )
    )

    # For Prometheus metrics, there should be 1 entry saved per cluster on 2023-02-15 and 2023-02-16,
    # and 0 entries saved per cluster on 2023-03-16 (as there's no job scraped for this date).
    for cluster_name in cluster_names:
        assert bool(
            re.search(
                rf"sarc\.jobs\.prometheus_scraping:prometheus_scraping\.py:[0-9]+ Saved Prometheus metrics for 1 jobs on {cluster_name} from 2023-02-15 00:00:00-05:00 to 2023-02-16 00:00:00-05:00\.",
                caplog.text,
            )
        )
        assert bool(
            re.search(
                rf"sarc\.jobs\.prometheus_scraping:prometheus_scraping\.py:[0-9]+ Saved Prometheus metrics for 1 jobs on {cluster_name} from 2023-02-16 00:00:00-05:00 to 2023-02-17 00:00:00-05:00\.",
                caplog.text,
            )
        )
        assert bool(
            re.search(
                rf"sarc\.jobs\.prometheus_scraping:prometheus_scraping\.py:[0-9]+ Saved Prometheus metrics for 0 jobs on {cluster_name} from 2023-03-16 00:00:00-04:00 to 2023-03-17 00:00:00-04:00\.",
                caplog.text,
            )
        )


@pytest.mark.usefixtures("empty_read_write_db", "disabled_cache")
def test_tracer_with_multiple_clusters_and_time_interval_and_prometheus(
    test_config,
    remote,
    file_regression,
    cli_main,
    prom_custom_query_mock,
    caplog,
    captrace,
    monkeypatch,
):
    """
    Copied from test_tracer_with_multiple_clusters_and_dates_and_prometheus above,
    with changes:
    - test --time_from and --time_to
    """
    caplog.set_level(logging.INFO)
    cluster_names = ["raisin", "patate"]
    datetimes = [datetime(2023, 2, 15, hour=1, tzinfo=MTL)]
    delta = timedelta(minutes=5)

    def _gen_error_command(cmd_template, job_submit_datetime):
        return Command(
            cmd=(
                cmd_template.format(
                    start=job_submit_datetime.strftime("%Y-%m-%dT%H:%M"),
                    end=(job_submit_datetime + delta).strftime("%Y-%m-%dT%H:%M"),
                )
            ),
            exit=1,
        )

    def _create_session(cluster_name, cmd_template, datetimes):
        return Session(
            host=cluster_name,
            commands=[
                Command(
                    cmd=(
                        cmd_template.format(
                            start=job_submit_datetime.strftime("%Y-%m-%dT%H:%M"),
                            end=(job_submit_datetime + delta).strftime(
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
            ]
            + [
                _gen_error_command(
                    cmd_template, datetime(2023, 3, 16, hour=1, tzinfo=MTL)
                )
            ],
        )

    remote.expect_sessions(
        _create_session(
            "raisin",
            "export TZ=UTC && /opt/slurm/bin/sacct  -X -S {start} -E {end} --allusers --json",
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

    def mock_compute_job_statistics(job):
        return JobStatistics()

    monkeypatch.setattr(
        "sarc.jobs.series.compute_job_statistics", mock_compute_job_statistics
    )

    def mock_get_job_time_series(job, metric, **kwargs):
        assert metric == "slurm_job_utilization_gpu_memory"
        return [
            {"metric": {"gpu_type": f"phantom_gpu_{job.cluster_name}_{job.job_id}"}}
        ]

    monkeypatch.setattr(
        prometheus_scraping, "get_job_time_series", mock_get_job_time_series
    )

    assert len(list(get_jobs())) == 0

    assert (
        cli_main(
            [
                "acquire",
                "jobs",
                "--cluster_name",
                "raisin",
                "patate",
                "--time_from",
                "2023-02-15T01:00",
                "--time_to",
                "2023-02-15T01:05",
            ]
        )
        == 0
    )

    assert len(list(get_jobs())) == len(datetimes) * len(cluster_names)

    assert (
        cli_main(
            [
                "acquire",
                "jobs",
                "--cluster_name",
                "raisin",
                "patate",
                "--time_from",
                "2023-03-16T01:00",
                "--time_to",
                "2023-03-16T01:05",
            ]
        )
        == 0
    )

    assert len(list(get_jobs())) == len(datetimes) * len(cluster_names)

    assert (
        cli_main(
            [
                "acquire",
                "prometheus",
                "--cluster_name",
                "raisin",
                "patate",
                "--time_from",
                "2023-02-15T01:00",
                "--time_to",
                "2023-02-15T01:05",
            ]
        )
        == 0
    )

    assert (
        cli_main(
            [
                "acquire",
                "prometheus",
                "--cluster_name",
                "raisin",
                "patate",
                "--time_from",
                "2023-03-16T01:00",
                "--time_to",
                "2023-03-16T01:05",
            ]
        )
        == 0
    )

    jobs = list(get_jobs())

    assert len(jobs) == len(datetimes) * len(cluster_names)

    # Check both jobs and trace in file regression
    spans = captrace.get_finished_spans()
    spans_data = [
        {
            "span_name": span.name,
            "span_events": [event.name for event in span.events],
            "span_attributes": dict(span.attributes),
            "span_has_error": span.status.status_code == StatusCode.ERROR,
        }
        for span in reversed(spans)
    ]

    file_regression.check(
        f"Found {len(jobs)} job(s):\n"
        + "\n".join(
            [job.model_dump_json(exclude={"id": True}, indent=4) for job in jobs]
        )
        + f"\n\nFound {len(spans)} span(s):\n"
        + json.dumps(spans_data, indent=1)
    )

    # Check logging
    print(caplog.text)
    for cluster_name in cluster_names:
        assert bool(
            re.search(
                rf"sarc\.cli\.acquire\.jobs:jobs\.py:[0-9]+ Acquire data on {cluster_name} for interval: 2023-02-15 01:00:00 to 2023-02-15 01:05:00 \(5.0 min\)",
                caplog.text,
            )
        )
        assert (
            f"Getting the sacct data for cluster {cluster_name}, time 2023-02-15 01:00:00 to 2023-02-15 01:05:00..."
            in caplog.text
        )

    assert "Saving into mongodb collection '" in caplog.text
    assert bool(
        re.search(
            r"sarc\.jobs\.sacct:sacct\.py:[0-9]+ Saved [0-9]+/[0-9]+ entries\.",
            caplog.text,
        )
    )

    # There should be 2 acquisition errors for unexpected data 2023-03-16, one per cluster.
    for cluster_name in cluster_names:
        assert bool(
            re.search(
                rf"sarc\.cli\.acquire\.jobs:jobs\.py:[0-9]+ Failed to acquire data on {cluster_name} for interval: 2023-03-16 01:00:00 to 2023-03-16 01:05:00:",
                caplog.text,
            )
        )

    # For Prometheus metrics, there should be 1 entry saved per cluster on 2023-02-15 and 2023-02-16,
    # and 0 entries saved per cluster on 2023-03-16 (as there's no job scraped for this date).
    for cluster_name in cluster_names:
        assert bool(
            re.search(
                rf"sarc\.jobs\.prometheus_scraping:prometheus_scraping\.py:[0-9]+ Saved Prometheus metrics for 1 jobs on {cluster_name} from 2023-02-15 01:00:00-05:00 to 2023-02-15 01:05:00-05:00\.",
                caplog.text,
            )
        )
        assert bool(
            re.search(
                rf"sarc\.jobs\.prometheus_scraping:prometheus_scraping\.py:[0-9]+ Saved Prometheus metrics for 0 jobs on {cluster_name} from 2023-03-16 01:00:00-04:00 to 2023-03-16 01:05:00-04:00\.",
                caplog.text,
            )
        )


@pytest.mark.usefixtures("empty_read_write_db", "disabled_cache")
def test_acquire_prometheus_for_cluster_without_prometheus(
    test_config,
    remote,
    file_regression,
    cli_main,
    prom_custom_query_mock,
    caplog,
    captrace,
    monkeypatch,
):
    """
    Test that we can't scrape Prometheus metrics for a cluster
    which does not have prometheus_url
    """
    caplog.set_level(logging.INFO)

    assert (
        cli_main(
            [
                "acquire",
                "prometheus",
                "--cluster_name",
                "raisin_no_prometheus",
                "--time_from",
                "2023-02-15T01:00",
                "--time_to",
                "2023-02-15T01:05",
            ]
        )
        == 0
    )

    assert len(list(get_jobs())) == 0

    # Check logging
    print(caplog.text)
    assert bool(
        re.search(
            r"sarc\.cli\.acquire\.prometheus:prometheus\.py:[0-9]+ No prometheus URL for cluster: raisin_no_prometheus, cannot get Prometheus metrics\.",
            caplog.text,
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
    remote.expect(
        host="patate",
        cmd="export TZ=UTC && /opt/software/slurm/bin/sacct -A rrg-bonhomme-ad_gpu,rrg-bonhomme-ad_cpu,def-bonhomme_gpu,def-bonhomme_cpu -X -S 2023-02-15T00:00 -E 2023-02-16T00:00 --allusers --json",
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
            ]
        )
        == 0
    )

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
def test_parse_sacct_slurm_versions(sacct_outputs, scraper):
    file = Path(__file__).parent / "sacct_outputs" / sacct_outputs
    scraper.get_raw._save_for_key(
        key=scraper.get_raw.key(), value=json.load(open(file, "r", encoding="utf8"))
    )
    jobs = list(scraper)
    assert len(jobs) == 1
