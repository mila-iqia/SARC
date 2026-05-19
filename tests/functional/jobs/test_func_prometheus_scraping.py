import json
import logging
import re
from datetime import datetime, timedelta

import pytest
from fabric.testing.base import Command, Session
from opentelemetry.trace import StatusCode

from sarc.config import UTC
from sarc.db.job import JobStatisticDB

from ...common.dateutils import MTL, _dtfmt, _dtreg
from ..cli.test_slurmconfig_fetch_parse import _save_slurm_conf


@pytest.fixture
def mock_compute_job_statistics(monkeypatch):
    _stats = JobStatisticDB(
        name="gpu_utilization",
        mean=1.0,
        std=0.0,
        q05=0.0,
        q25=0.0,
        median=1.0,
        q75=1.0,
        max=1.0,
        unused=0.0,
    )

    def mock_func(job, prom_stats):
        mock_func.called += 1
        return {"gpu_utilization": _stats}

    mock_func.called = 0
    monkeypatch.setattr("sarc.scraping.series.compute_job_statistics", mock_func)

    yield mock_func

    assert mock_func.called > 0


@pytest.mark.parametrize(
    "test_config", [{"clusters": {"raisin": {"host": "raisin"}}}], indirect=True
)
@pytest.mark.parametrize(
    "json_jobs",
    [
        {
            "nodes": "X007",
            "tres": {
                "allocated": [
                    {
                        "count": 1,
                        "id": 1002,
                        "name": "gpu:gpu_name_from_sacct",
                        "type": "gres",
                    }
                ]
            },
        }
    ],
    indirect=True,
)
@pytest.mark.usefixtures("enabled_cache", "no_pkey")
def test_get_gpu_type(
    test_config,
    get_jobs,
    jobless_read_write_db,
    sacct_json,
    remote,
    cli_main,
    monkeypatch,
    mock_compute_job_statistics,
):
    """Test all 3 sources of GPU type (sacct, node->GPU and prometheus)"""

    remote.expect(
        host="raisin",
        commands=[
            Command(
                cmd="export TZ=UTC && /opt/slurm/bin/sacct -X -S 2023-02-15T00:00 -E 2023-02-16T00:00 --allusers --json --duplicates",
                out=f"Welcome on raisin,\nThe sweetest supercomputer in the world!\n{sacct_json}".encode(
                    "utf-8"
                ),
            )
            for _ in range(2)
        ],
    )

    cmd_sacct_fetch = [
        "fetch",
        "jobs",
        "--cluster_names",
        "raisin",
        "--intervals",
        "2023-02-15T00:00-2023-02-16T00:00",
    ]

    cmd_sacct_parse = ["parse", "jobs", "--since", "2023-02-14T00:00"]

    # Test `acquire jobs` without node->gpu available
    # -----------------------------------------------
    # Should return GPU name from sacct
    assert cli_main(cmd_sacct_fetch) == 0
    assert cli_main(cmd_sacct_parse) == 0

    jobs = list(get_jobs())
    assert len(jobs) == 1
    job = jobs[0]
    assert job.allocated_gpu_type == "gpu_name_from_sacct"
    assert not job.statistics

    # Test `fetch jobs` and `parse_jobs` with node->gpu available
    # --------------------------------------------
    # node->gpu is prior to sacct data

    # Save slurm config in cache.
    _save_slurm_conf(
        "raisin",
        "2023-02-15",
        "NodeName=X00[1-9] Param1=Anything1 Param2=Anything2 Gres=gpu:gpu2:4 Param3=Anything3",
    )
    # Acquire slurm config.
    assert cli_main(["parse", "slurmconfig", "--cluster_name", "raisin"]) == 0
    # acquire jobs
    assert cli_main(cmd_sacct_fetch) == 0
    assert cli_main(cmd_sacct_parse) == 0

    jobless_read_write_db.commit()
    jobs = list(get_jobs())
    assert len(jobs) == 1
    job = jobs[0]
    assert job.allocated_gpu_type == "THE GPU II"
    assert not job.statistics

    # Test `acquire prometheus`
    # -------------------------
    # Prometheus data is prior to node->gpu and sacct data

    def mock_get_job_time_series(job, metric, **kwargs):
        mock_get_job_time_series.called += 1
        assert job.job_id == 1
        return [
            {"metric": {"__name__": "slurm_job_gpu_name", "gpu_type": "phantom_gpu"}}
        ]

    mock_get_job_time_series.called = 0
    monkeypatch.setattr(
        "sarc.scraping.series.get_job_time_series_data", mock_get_job_time_series
    )

    assert cli_main(["fetch", "prometheus", "--cluster_name", "raisin"]) == 0
    assert cli_main(["parse", "prometheus", "--since", "2023-02-14T00:00"]) == 0
    assert mock_compute_job_statistics.called == 1
    assert mock_get_job_time_series.called == 1

    jobless_read_write_db.commit()
    jobs = list(get_jobs())
    assert len(jobs) == 1
    job = jobs[0]
    assert job.allocated_gpu_type == "PHANTOM GPU MENACE"
    assert job.statistics


@pytest.mark.usefixtures("enabled_cache", "no_pkey")
def test_tracer_with_multiple_clusters_and_dates_and_prometheus(
    jobless_read_write_db,
    test_config,
    remote,
    file_regression,
    cli_main,
    caplog,
    captrace,
    monkeypatch,
    mock_compute_job_statistics,
    get_jobs,
    create_sacct_json,
):
    """
    Copied from test_multiple_clusters_and_dates above, with changes:
    - Added captrace to test tracing
    - Added a call to `acquire prometheus` to test prometheus-related tracing
    """
    caplog.set_level(logging.INFO)
    cluster_names = ["raisin", "patate"]
    datetimes = [
        datetime(2023, 2, 15, tzinfo=MTL).astimezone(UTC) + timedelta(days=i)
        for i in range(2)
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
                                    "submission": (
                                        ts := int(job_submit_datetime.timestamp())
                                    ),
                                    "eligible": ts,
                                    "start": ts + 3600,
                                    "end": ts + 7200,
                                    "elapsed": 3600,
                                },
                            }
                        ]
                    ).encode("utf-8"),
                )
                for job_id, job_submit_datetime in enumerate(datetimes)
            ]
            + [
                _gen_error_command(
                    cmd_template, datetime(2023, 3, 16, tzinfo=MTL).astimezone(UTC)
                )
            ],
        )

    remote.expect_sessions(
        _create_session(
            "raisin",
            "export TZ=UTC && /opt/slurm/bin/sacct -X -S {start} -E {end} --allusers --json --duplicates",
            datetimes=datetimes,
        ),
        _create_session(
            "patate",
            (
                "export TZ=UTC && /opt/software/slurm/bin/sacct "
                "-A rrg-bonhomme-ad_gpu,rrg-bonhomme-ad_cpu,def-bonhomme_gpu,def-bonhomme_cpu "
                "-X -S {start} -E {end} --allusers --json --duplicates"
            ),
            datetimes=datetimes,
        ),
    )

    # Import here so that config() is setup correctly when CLI is created.
    import sarc.cli  # noqa: F401

    def mock_get_job_time_series(job, metric, **kwargs):
        return [{"metric": {"gpu_type": f"phantom_gpu_{job.cluster_id}_{job.job_id}"}}]

    monkeypatch.setattr(
        "sarc.scraping.series.get_job_time_series_data", mock_get_job_time_series
    )

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
                f"{_dtfmt(2023, 3, 16)}-{_dtfmt(2023, 3, 17)}",
            ]
        )
        == 0
    )

    assert cli_main(["parse", "jobs", "--since", "2023-02-14T00:00"]) == 0

    assert cli_main(["fetch", "prometheus", "--cluster_name", "raisin", "patate"]) == 0
    assert cli_main(["parse", "prometheus", "--since", "2023-02-14T00:00"]) == 0

    jobless_read_write_db.commit()
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
            [
                json.dumps(
                    job.model_dump(mode="json", exclude={"id": True}),
                    indent=4,
                    sort_keys=True,
                )
                for job in jobs
            ]
        )
        + f"\n\nFound {len(spans)} span(s):\n"
        + json.dumps(spans_data, indent=1)
    )

    # Check logging
    print(caplog.text)
    assert bool(
        re.search(
            rf"sarc.scraping.jobs:jobs\.py:[0-9]+ Fetching the sacct data for cluster raisin, time {_dtreg(2023, 2, 15)} to {_dtreg(2023, 2, 16)}",
            caplog.text,
        )
    )
    assert bool(
        re.search(
            rf"sarc.scraping.jobs:jobs\.py:[0-9]+ Fetching the sacct data for cluster patate, time {_dtreg(2023, 2, 15)} to {_dtreg(2023, 2, 16)}",
            caplog.text,
        )
    )
    assert (
        "Parsing slurm jobs identified by: raisin_2023-02-15T05:00_2023-02-16T05:00..."
        in caplog.text
    )
    assert bool(
        re.search(
            r"sarc\.scraping\.prometheus:prometheus\.py:[0-9]+ Saved Prometheus metrics for [0-9]+ jobs\.",
            caplog.text,
        )
    )

    # There should be 2 acquisition errors for unexpected data 2023-03-16, one per cluster.
    assert bool(
        re.search(
            rf"sarc.scraping.jobs:jobs\.py:[0-9]+ Failed to fetch data on raisin for interval: {_dtreg(2023, 3, 16)} to {_dtreg(2023, 3, 17)}:",
            caplog.text,
        )
    )
    assert bool(
        re.search(
            rf"sarc.scraping.jobs:jobs\.py:[0-9]+ Failed to fetch data on patate for interval: {_dtreg(2023, 3, 16)} to {_dtreg(2023, 3, 17)}:",
            caplog.text,
        )
    )

    # For Prometheus metrics, check that fetching happened
    assert bool(
        re.search(
            r"sarc\.scraping\.prometheus:prometheus\.py:[0-9]+ Fetched Prometheus metrics for 2 jobs\.",
            caplog.text,
        )
    )


@pytest.mark.usefixtures("jobless_read_write_db", "disabled_cache")
def test_acquire_prometheus_for_cluster_without_prometheus(
    test_config, cli_main, caplog, get_jobs
):
    """
    Test that we can't scrape Prometheus metrics for a cluster
    which does not have prometheus_url
    """
    caplog.set_level(logging.INFO)

    assert (
        cli_main(["fetch", "prometheus", "--cluster_name", "raisin_no_prometheus"]) == 0
    )

    assert len(list(get_jobs())) == 0

    # Check logging
    print(caplog.text)
    assert bool(
        re.search(
            r"sarc\.cli\.fetch\.prometheus:prometheus\.py:[0-9]+ No prometheus URL for cluster: raisin_no_prometheus, cannot get Prometheus metrics\.",
            caplog.text,
        )
    )
