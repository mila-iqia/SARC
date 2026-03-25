import pytest
from prometheus_api_client import MetricRangeDataFrame

from sarc.client.job import SlurmJob, get_job
from sarc.jobs.series import JOB_STATISTICS_METRIC_NAMES, compute_job_statistics
from tests.functional.jobs.factory import elapsed_time as BASE_ELAPSED_TIME


def generate_point_range(n, min, max):
    return [min + (i / n) * (max - min) for i in range(n)]


def format_fake_timeseries(metric, values, t0, delta):
    return {
        "metric": metric,
        "values": [[t0 + i * delta, str(value)] for i, value in enumerate(values)],
    }


def generate_fake_timeseries(
    job: SlurmJob, metric=JOB_STATISTICS_METRIC_NAMES, max_points=100
):
    assert job.nodes

    n = int(job.elapsed_time // 30)

    if n == 0:
        return []

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

    return results


@pytest.mark.usefixtures("read_only_db")
def test_compute_job_statistics(data_regression):
    job = get_job(job_id=1)
    assert job is not None

    statistics = compute_job_statistics(job, generate_fake_timeseries(job))

    data_regression.check(statistics.model_dump())
