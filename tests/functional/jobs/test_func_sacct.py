from __future__ import annotations

import copy
import json
from datetime import datetime, timedelta

import pytest
from fabric.testing.base import Command, Session

from sarc.config import MTL, PST, UTC, config
from sarc.jobs.job import get_jobs
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
        cmd="/opt/slurm/bin/sacct  -X -S '2023-02-14T00:00' -E '2023-02-15T00:00' --json",
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
        cmd="/opt/software/slurm/bin/sacct -A rrg-bonhomme-ad_gpu,rrg-bonhomme-ad_cpu,def-bonhomme_gpu,def-bonhomme_cpu -X -S '2023-02-14T00:00' -E '2023-02-15T00:00' --json",
        out=b'{"jobs": []}',
    )

    assert len(list(scraper)) == 0


@pytest.mark.parametrize(
    "test_config", [{"clusters": {"raisin": {"host": "raisin"}}}], indirect=True
)
@pytest.mark.parametrize("json_jobs", [{}], indirect=True)
@pytest.mark.usefixtures("empty_read_write_db")
def test_save_job(test_config, sacct_json, remote, file_regression, cli_main):
    channel = remote.expect(
        host="raisin",
        cmd="/opt/slurm/bin/sacct  -X -S '2023-02-15T00:00' -E '2023-02-16T00:00' --json",
        out=sacct_json.encode("utf-8"),
    )

    # Import here so that config() is setup correctly when CLI is created.
    import sarc.cli

    assert (
        cli_main(
            ["acquire", "jobs", "--cluster_name", "raisin", "--dates", "2023-02-15"]
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
def test_update_job(test_config, sacct_json, remote, file_regression, cli_main):
    channel = remote.expect(
        host="raisin",
        commands=[
            Command(
                cmd="/opt/slurm/bin/sacct  -X -S '2023-02-15T00:00' -E '2023-02-16T00:00' --json",
                out=sacct_json.encode("utf-8"),
            )
            for _ in range(2)
        ],
    )

    # Import here so that config() is setup correctly when CLI is created.
    import sarc.cli

    assert (
        cli_main(
            ["acquire", "jobs", "--cluster_name", "raisin", "--dates", "2023-02-15"]
        )
        == 0
    )

    assert len(list(get_jobs())) == 1

    assert (
        cli_main(
            ["acquire", "jobs", "--cluster_name", "raisin", "--dates", "2023-02-15"]
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
def test_save_preempted_job(test_config, sacct_json, remote, file_regression, cli_main):
    channel = remote.expect(
        cmd="/opt/slurm/bin/sacct  -X -S '2023-02-15T00:00' -E '2023-02-16T00:00' --json",
        host="raisin",
        out=sacct_json.encode("utf-8"),
    )

    # Import here so that config() is setup correctly when CLI is created.
    import sarc.cli

    assert (
        cli_main(
            ["acquire", "jobs", "--cluster_name", "raisin", "--dates", "2023-02-15"]
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
def test_multiple_dates(test_config, remote, file_regression, cli_main):
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
                    "--json"
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

    channel = remote.expect_sessions(
        _create_session(
            "raisin",
            "/opt/slurm/bin/sacct  -X -S '{start}' -E '{end}' --json",
            datetimes=datetimes,
        ),
        _create_session(
            "patate",
            (
                "/opt/software/slurm/bin/sacct "
                "-A rrg-bonhomme-ad_gpu,rrg-bonhomme-ad_cpu,def-bonhomme_gpu,def-bonhomme_cpu "
                "-X -S '{start}' -E '{end}' --json"
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
    channel = remote.expect(
        host="patate",
        cmd="/opt/software/slurm/bin/sacct -A rrg-bonhomme-ad_gpu,rrg-bonhomme-ad_cpu,def-bonhomme_gpu,def-bonhomme_cpu -X -S '2023-02-15T00:00' -E '2023-02-16T00:00' --json",
        out=sacct_json.encode("utf-8"),
    )

    assert (
        cli_main(
            ["acquire", "jobs", "--cluster_name", "patate", "--dates", "2023-02-15"]
        )
        == 0
    )

    jobs = list(get_jobs())
    assert len(jobs) == 1
    assert jobs[0].submit_time == datetime(2023, 2, 15, 12 + 3, 0, 0, tzinfo=MTL)
