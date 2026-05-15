from datetime import UTC, datetime

from sarc.core.scraping.series import JOB_STATISTICS_METRIC_NAMES
from sarc.db.job import SlurmJobDB
from tests.db.factory import elapsed_time as BASE_ELAPSED_TIME


def _get_warnings(text: str, modules: list[str]) -> list:
    """Parse warning messages from given text (typically caplog.text)"""
    warnings = []
    for line in text.split("\n"):
        line = line.strip()
        if line.startswith("ERROR "):
            line_content = line[len("ERROR") :].lstrip()
            line_ref, warning_msg = line_content.split(" ", maxsplit=1)
            if any(line_ref.startswith(f"{module_str}:") for module_str in modules):
                warnings.append(warning_msg.strip())
    return warnings


def generate_point_range(n, min, max):
    return [min + (i / n) * (max - min) for i in range(n)]


def format_fake_timeseries(metric, values, t0, delta):
    return {
        "metric": metric,
        "values": [[t0 + i * delta, str(value)] for i, value in enumerate(values)],
    }


def generate_fake_timeseries(
    job: SlurmJobDB, metric=JOB_STATISTICS_METRIC_NAMES, max_points=100
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
        "slurm_job_memory_usage": (0, job.allocated_mem * 1e6, {}),
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
            "user": job.cluster_user,
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


MOCK_TIME = datetime(2023, 11, 22, tzinfo=UTC)
