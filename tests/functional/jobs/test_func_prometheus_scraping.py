from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timedelta

import pytest
from fabric.testing.base import Command, Session
from opentelemetry.trace import StatusCode

from sarc.client.job import JobStatistics, get_jobs, get_available_clusters
from sarc.config import MTL, UTC, config
from sarc.jobs import prometheus_scraping

from .factory import create_sacct_json


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


@pytest.fixture
def mock_compute_job_statistics(monkeypatch):
    def mock_func(job):
        mock_func.called += 1
        return JobStatistics()

    mock_func.called = 0
    monkeypatch.setattr("sarc.jobs.series.compute_job_statistics", mock_func)

    yield mock_func

    assert mock_func.called > 0


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
def test_get_gpu_type(
    test_config, sacct_json, remote, cli_main, monkeypatch, mock_compute_job_statistics
):
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
        "--intervals",
        "2023-02-15T00:00-2023-02-16T00:00",
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
                "--intervals",
                "2023-02-15T00:00-2023-02-16T00:00",
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


@pytest.mark.usefixtures("empty_read_write_db", "disabled_cache")
def test_tracer_with_multiple_clusters_and_dates_and_prometheus(
    test_config,
    remote,
    file_regression,
    cli_main,
    caplog,
    captrace,
    monkeypatch,
    mock_compute_job_statistics,
):
    """
    Copied from test_multiple_clusters_and_dates above, with changes:
    - Added captrace to test tracing
    - Added a call to `acquire prometheus` to test prometheus-related tracing
    """
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
                "--intervals",
                "2023-02-15T00:00-2023-02-16T00:00",
                "2023-02-16T00:00-2023-02-17T00:00",
                "2023-03-16T00:00-2023-03-17T00:00",
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
                "--intervals",
                "2023-02-15T00:00-2023-02-16T00:00",
                "2023-02-16T00:00-2023-02-17T00:00",
                "2023-03-16T00:00-2023-03-17T00:00",
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
            r"sarc.cli.acquire.jobs:jobs\.py:[0-9]+ Acquire data on raisin for interval: 2023-02-15 00:00:00\+00:00 to 2023-02-16 00:00:00\+00:00 \(1440.0 min\)",
            caplog.text,
        )
    )
    assert bool(
        re.search(
            r"sarc.cli.acquire.jobs:jobs\.py:[0-9]+ Acquire data on patate for interval: 2023-02-15 00:00:00\+00:00 to 2023-02-16 00:00:00\+00:00 \(1440.0 min\)",
            caplog.text,
        )
    )
    assert (
        "Getting the sacct data for cluster raisin, time 2023-02-15 00:00:00+00:00 to 2023-02-16 00:00:00+00:00..."
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
            r"sarc.cli.acquire.jobs:jobs\.py:[0-9]+ Failed to acquire data on raisin for interval: 2023-03-16 00:00:00\+00:00 to 2023-03-17 00:00:00\+00:00:",
            caplog.text,
        )
    )
    assert bool(
        re.search(
            r"sarc.cli.acquire.jobs:jobs\.py:[0-9]+ Failed to acquire data on patate for interval: 2023-03-16 00:00:00\+00:00 to 2023-03-17 00:00:00\+00:00:",
            caplog.text,
        )
    )

    # For Prometheus metrics, there should be 1 entry saved per cluster on 2023-02-15 and 2023-02-16,
    # and 0 entries saved per cluster on 2023-03-16 (as there's no job scraped for this date).
    for cluster_name in cluster_names:
        assert bool(
            re.search(
                rf"sarc\.jobs\.prometheus_scraping:prometheus_scraping\.py:[0-9]+ Saved Prometheus metrics for 1 jobs on {cluster_name} from 2023-02-15 00:00:00\+00:00 to 2023-02-16 00:00:00\+00:00\.",
                caplog.text,
            )
        )
        assert bool(
            re.search(
                rf"sarc\.jobs\.prometheus_scraping:prometheus_scraping\.py:[0-9]+ Saved Prometheus metrics for 1 jobs on {cluster_name} from 2023-02-16 00:00:00\+00:00 to 2023-02-17 00:00:00\+00:00\.",
                caplog.text,
            )
        )
        assert bool(
            re.search(
                rf"sarc\.jobs\.prometheus_scraping:prometheus_scraping\.py:[0-9]+ Saved Prometheus metrics for 0 jobs on {cluster_name} from 2023-03-16 00:00:00\+00:00 to 2023-03-17 00:00:00\+00:00\.",
                caplog.text,
            )
        )


@pytest.mark.usefixtures("empty_read_write_db", "disabled_cache")
def test_tracer_with_multiple_clusters_and_time_interval_and_prometheus(
    test_config,
    remote,
    file_regression,
    cli_main,
    caplog,
    captrace,
    monkeypatch,
    mock_compute_job_statistics,
):
    """
    Copied from test_tracer_with_multiple_clusters_and_dates_and_prometheus above,
    with changes:
    - test --time_from and --time_to
    """
    caplog.set_level(logging.INFO)
    cluster_names = ["raisin", "patate"]
    datetimes = [datetime(2023, 2, 15, hour=1, tzinfo=UTC)]
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
                    cmd_template, datetime(2023, 3, 16, hour=1, tzinfo=UTC)
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
                "--intervals",
                "2023-02-15T01:00-2023-02-15T01:05",
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
                "--intervals",
                "2023-03-16T01:00-2023-03-16T01:05",
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
                "--intervals",
                "2023-02-15T01:00-2023-02-15T01:05",
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
                "--intervals",
                "2023-03-16T01:00-2023-03-16T01:05",
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
                rf"sarc\.cli\.acquire\.jobs:jobs\.py:[0-9]+ Acquire data on {cluster_name} for interval: 2023-02-15 01:00:00\+00:00 to 2023-02-15 01:05:00\+00:00 \(5.0 min\)",
                caplog.text,
            )
        )
        assert (
            f"Getting the sacct data for cluster {cluster_name}, time 2023-02-15 01:00:00+00:00 to 2023-02-15 01:05:00+00:00..."
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
                rf"sarc\.cli\.acquire\.jobs:jobs\.py:[0-9]+ Failed to acquire data on {cluster_name} for interval: 2023-03-16 01:00:00\+00:00 to 2023-03-16 01:05:00\+00:00:",
                caplog.text,
            )
        )

    # For Prometheus metrics, there should be 1 entry saved per cluster on 2023-02-15 and 2023-02-16,
    # and 0 entries saved per cluster on 2023-03-16 (as there's no job scraped for this date).
    for cluster_name in cluster_names:
        assert bool(
            re.search(
                rf"sarc\.jobs\.prometheus_scraping:prometheus_scraping\.py:[0-9]+ Saved Prometheus metrics for 1 jobs on {cluster_name} from 2023-02-15 01:00:00\+00:00 to 2023-02-15 01:05:00\+00:00\.",
                caplog.text,
            )
        )
        assert bool(
            re.search(
                rf"sarc\.jobs\.prometheus_scraping:prometheus_scraping\.py:[0-9]+ Saved Prometheus metrics for 0 jobs on {cluster_name} from 2023-03-16 01:00:00\+00:00 to 2023-03-16 01:05:00\+00:00\.",
                caplog.text,
            )
        )


@pytest.mark.usefixtures("empty_read_write_db", "disabled_cache")
def test_acquire_prometheus_for_cluster_without_prometheus(
    test_config,
    cli_main,
    caplog,
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
                "--intervals",
                "2023-02-15T01:00-2023-02-15T01:05",
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


def test_acquire_prometheus_mutually_exclusive_args(cli_main, caplog):
    # Both --intervals and --auto_interval: must fail
    assert (
        cli_main(
            [
                "acquire",
                "prometheus",
                "--cluster_name",
                "raisin",
                "--intervals",
                "2023-02-15T00:00-2023-02-16T00:00",
                "--auto_interval",
                "10",
            ]
        )
        == -1
    )

    assert not list(get_jobs())
    assert (
        "Parameters mutually exclusive: either --intervals or --auto_interval, not both"
        in caplog.text
    )


def test_acquire_prometheus_invalid_interval(cli_main, caplog):
    # Malformed interval
    assert (
        cli_main(
            [
                "acquire",
                "prometheus",
                "--cluster_name",
                "raisin",
                "--intervals",
                "2023-02-15x00:00-2023-02-16T00:00",
            ]
        )
        == 0
    )
    assert (
        "Invalid interval 2023-02-15x00:00-2023-02-16T00:00 ; skipping cluster"
        in caplog.text
    )


def test_acquire_prometheus_interval_start_gt_end(cli_main, caplog):
    # Malformed interval: start > end
    assert (
        cli_main(
            [
                "acquire",
                "prometheus",
                "--cluster_name",
                "raisin",
                "--intervals",
                "2023-02-17T00:00-2023-02-16T00:00",
            ]
        )
        == 0
    )
    assert (
        "Interval: 2023-02-17 00:00:00+00:00 > 2023-02-16 00:00:00+00:00 ; skipping cluster"
        in caplog.text
    )


def test_acquire_prometheus_args_no_interval(cli_main, caplog):
    # No interval, nothing to do
    assert (
        cli_main(
            [
                "acquire",
                "prometheus",
                "--cluster_name",
                "raisin",
            ]
        )
        == 0
    )
    assert "No --intervals or --auto_interval parsed, nothing to do." in caplog.text


def _get_cluster_raisin():
    return [
        cluster
        for cluster in get_available_clusters()
        if cluster.cluster_name == "raisin"
    ][0]


@pytest.mark.usefixtures("read_write_db", "enabled_cache")
def test_auto_interval(cli_main, monkeypatch, freezer, caplog):
    """Test auto_interval."""

    def mock_scrap_prometheus(*args, **kwargs):
        mock_scrap_prometheus.called += 1

    mock_scrap_prometheus.called = 0

    monkeypatch.setattr(
        "sarc.cli.acquire.prometheus.scrap_prometheus", mock_scrap_prometheus
    )

    orig_end_time = datetime.strptime(
        _get_cluster_raisin().end_time_prometheus, "%Y-%m-%dT%H:%M"
    )
    expected_final_end_time = orig_end_time + timedelta(minutes=300)
    freezer.move_to(expected_final_end_time)

    assert (
        cli_main(
            [
                "-v",
                "acquire",
                "prometheus",
                "--cluster_name",
                "raisin",
                "--auto_interval",
                "60",
            ]
        )
        == 0
    )
    print(caplog.text)
    # end_time_prometheus should have been updated
    assert (
        datetime.strptime(_get_cluster_raisin().end_time_prometheus, "%Y-%m-%dT%H:%M")
        == expected_final_end_time
    )
    # We should have scraped 5 times
    assert mock_scrap_prometheus.called == 5


@pytest.mark.usefixtures("read_write_db", "enabled_cache")
def test_auto_interval_0(cli_main, monkeypatch, freezer, caplog):
    """Test auto_interval with unique interval."""

    def mock_scrap_prometheus(*args, **kwargs):
        mock_scrap_prometheus.called += 1

    mock_scrap_prometheus.called = 0

    monkeypatch.setattr(
        "sarc.cli.acquire.prometheus.scrap_prometheus", mock_scrap_prometheus
    )

    orig_end_time = datetime.strptime(
        _get_cluster_raisin().end_time_prometheus, "%Y-%m-%dT%H:%M"
    )
    expected_final_end_time = orig_end_time + timedelta(minutes=300)
    freezer.move_to(expected_final_end_time)

    assert (
        cli_main(
            [
                "-v",
                "acquire",
                "prometheus",
                "--cluster_name",
                "raisin",
                "--auto_interval",
                "0",
            ]
        )
        == 0
    )
    print(caplog.text)
    # end_time_prometheus should have been updated
    assert (
        datetime.strptime(_get_cluster_raisin().end_time_prometheus, "%Y-%m-%dT%H:%M")
        == expected_final_end_time
    )
    # We should have scraped 1 time
    assert mock_scrap_prometheus.called == 1
