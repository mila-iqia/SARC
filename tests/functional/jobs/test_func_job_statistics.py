import pytest
from prometheus_api_client import MetricRangeDataFrame

from sarc.client.job import SlurmJob, get_job
from tests.functional.jobs.factory import elapsed_time as BASE_ELAPSED_TIME


def generate_point_range(n, min, max):
    return [min + (i / n) * (max - min) for i in range(n)]


def format_fake_timeseries(metric, values, t0, delta):
    return {
        "metric": metric,
        "values": [[t0 + i * delta, str(value)] for i, value in enumerate(values)],
    }


def generate_fake_timeseries(job: SlurmJob, metric, max_points=100, dataframe=True):
    assert job.nodes

    n = job.elapsed_time // 30

    if n == 0:
        return None

    metric_ranges = {
        "slurm_job_utilization_gpu": (0, 100, {"gpu": 3}),
        "slurm_job_fp16_gpu": (0, 100, {"gpu": 3}),
        "slurm_job_fp32_gpu": (0, 100, {"gpu": 3}),
        "slurm_job_fp64_gpu": (0, 100, {"gpu": 3}),
        "slurm_job_sm_occupancy_gpu": (0, 100, {"gpu": 3}),
        "slurm_job_utilization_gpu_memory": (0, 100, {"gpu": 3}),
        "slurm_job_core_usage": (1e9, 1e9 * job.elapsed_time, {"core": 7}),
        "slurm_job_memory_usage": (0, job.allocated.mem * 1e6, {}),
        "slurm_job_power_gpu": (50_000, 150_000, {}),
    }

    metrics = [metric] if isinstance(metric, str) else metric
    results = []
    for this_metric in metrics:
        (mn, mx, extra) = metric_ranges[this_metric]

        metric_dict = {
            "__name__": this_metric,
            "account": job.account,
            "instance": job.nodes[0],
            "job": "slurm_jobs",
            "slurmjobid": str(job.job_id),
            "user": job.user,
            **extra,
        }

        results.append(
            format_fake_timeseries(
                metric=metric_dict,
                values=generate_point_range(n, mn, mx),
                t0=int(job.start_time.timestamp()),
                # Change delta depending on job's elapsed time wrt/ base test elapsed time
                delta=30 * job.elapsed_time / BASE_ELAPSED_TIME,
            )
        )

    if dataframe:
        return MetricRangeDataFrame(results) if results else None
    else:
        return results


@pytest.mark.usefixtures("read_only_db")
def test_job_statistics(monkeypatch, data_regression):
    job = get_job(job_id=1)

    monkeypatch.setattr(
        "sarc.jobs.series.get_job_time_series", generate_fake_timeseries
    )

    assert not job.stored_statistics
    statistics = job.statistics(save=False)
    assert not job.stored_statistics

    data_regression.check(statistics.model_dump())


@pytest.mark.usefixtures("read_only_db")
def test_job_statistics_no_save_without_end_time(monkeypatch, data_regression):
    job = get_job(job_state="RUNNING")
    assert not job.end_time

    monkeypatch.setattr(
        "sarc.jobs.series.get_job_time_series", generate_fake_timeseries
    )

    assert not job.stored_statistics
    job.statistics(save=True)
    assert not job.stored_statistics

    rejob = get_job(job_state="RUNNING")
    assert job == rejob
    assert not rejob.stored_statistics


@pytest.mark.usefixtures("read_only_db")
def test_job_statistics_nothing(monkeypatch):
    job = get_job(job_id=1)

    def _fake_job_time_series(*args, **kwargs):
        return None if kwargs.get("dataframe", True) else []

    monkeypatch.setattr("sarc.jobs.series.get_job_time_series", _fake_job_time_series)

    assert not job.stored_statistics
    statistics = job.statistics(save=False)
    assert not statistics.cpu_utilization
    assert not statistics.gpu_utilization
    assert not statistics.gpu_utilization_fp16
    assert not statistics.gpu_utilization_fp32
    assert not statistics.gpu_utilization_fp64
    assert not statistics.gpu_sm_occupancy
    assert not statistics.gpu_memory
    assert not statistics.gpu_power
    assert not statistics.system_memory


@pytest.mark.usefixtures("read_write_db")
def test_job_statistics_save(monkeypatch, data_regression):
    job = get_job(job_id=1)

    monkeypatch.setattr(
        "sarc.jobs.series.get_job_time_series", generate_fake_timeseries
    )

    assert not job.stored_statistics
    job.statistics()
    assert job.stored_statistics

    rejob = get_job(job_id=1)
    assert rejob.stored_statistics
    assert rejob.statistics() is rejob.stored_statistics
    recomputed = rejob.statistics(recompute=True, save=False)
    assert recomputed == rejob.stored_statistics
    assert recomputed is not rejob.stored_statistics
