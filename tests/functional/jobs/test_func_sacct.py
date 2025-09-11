from __future__ import annotations

import json
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest
from fabric.testing.base import Command, Session
from opentelemetry.trace import StatusCode

from sarc.client.job import get_jobs
from sarc.config import MTL, PST, UTC, config
from sarc.jobs.sacct import SAcctScraper

from .factory import create_sacct_json


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
        key=scraper.get_raw.key(),
        value=json.loads(sacct_json),
        at_time=datetime.now(UTC),
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
        key=scraper.get_raw.key(),
        value=json.loads(sacct_json),
        at_time=datetime.now(UTC),
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
        key=scraper.get_raw.key(),
        value=json.loads(sacct_json),
        at_time=datetime.now(UTC),
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
    test_config, sacct_json, remote, file_regression, cli_main
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
    "test_config", [{"clusters": {"raisin": {"host": "raisin"}}}], indirect=True
)
@pytest.mark.parametrize("json_jobs", [{}], indirect=True)
@pytest.mark.usefixtures("empty_read_write_db")
def test_save_job(test_config, sacct_json, remote, file_regression, cli_main):
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
def test_update_job(test_config, sacct_json, remote, file_regression, cli_main):
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
def test_save_preempted_job(test_config, sacct_json, remote, file_regression, cli_main):
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
def test_multiple_dates(test_config, remote, file_regression, cli_main):
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
def test_multiple_clusters_and_dates(test_config, remote, file_regression, cli_main):
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
def test_job_tz(test_config, sacct_json, remote, cli_main):
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
        key=scraper.get_raw.key(),
        value=json.load(open(file, "r", encoding="utf8")),
        at_time=datetime.now(UTC),
    )
    jobs = list(scraper)
    assert len(jobs) == 1
