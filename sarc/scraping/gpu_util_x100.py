"""Undo the x100 that slurm-job-exporter 0.5.0 puts on the true GPU utilization.

In DCGM mode, 0.5.0 publishes `slurm_job_utilization_gpu` as DCGM's integer
percentage times 100 (0, 100, ..., 10000); 0.5.1 (#73) and the NVML mode
publish the percentage itself. The exporter exposes no version, so the job
is judged by its values: only DCGM 0.5.0 sends more than 100, and it sends
multiples of 100 only.

Like gpu_activity_series, the job is judged as a whole: in a job spanning
nodes on different exporter versions, or an exporter upgrade, the samples
at 100 may be misread.
"""

from datetime import UTC, datetime

from sarc.scraping.dcgm import DCGM_FP64_BLANK

# Publication times of slurm-job-exporter 0.5.0 (the x100) and 0.5.1 (its
# fix): clusters are assumed to run official releases only, so samples
# before X100_SINCE are not 0.5.0's. FIXED_SINCE decides nothing: zeros and
# 100 are undecided wherever they fall.
X100_SINCE = datetime(2026, 9, 24, 17, 16, 4, tzinfo=UTC).timestamp()
FIXED_SINCE = datetime(2026, 10, 7, 15, 4, 1, tzinfo=UTC).timestamp()

_MEMORY = "slurm_job_utilization_gpu_memory"
# Published in DCGM mode only, when profiling is readable. _MEMORY is left
# out: it is a profiling metric in DCGM mode, but NVML mode publishes it too.
_DCGM_ONLY = (
    "slurm_job_sm_active_gpu",
    "slurm_job_sm_occupancy_gpu",
    "slurm_job_fp16_gpu",
    "slurm_job_fp32_gpu",
    "slurm_job_fp64_gpu",
)


def rescale_gpu_utilization(
    utilization: list[dict], metric_to_data: dict[str, list[dict]]
) -> list[dict] | None:
    """Divide by 100 the samples of `utilization` that exporter 0.5.0 sent.

    `utilization` holds the job's true utilization series, `metric_to_data`
    all its series by metric name. Returns None when the job's samples at
    100 may be 1 % or 100 %.
    """
    samples = [(float(t), float(v)) for s in utilization for t, v in s["values"]]
    # Real samples (no DCGM BLANK sentinel, no NaN) that 0.5.0 may have sent.
    recent = [(t, v) for t, v in samples if t >= X100_SINCE and v < DCGM_FP64_BLANK]
    if not recent or _nvml(metric_to_data):
        return utilization

    values = [v for _, v in recent]
    if any(v % 100 != 0 for v in values):
        # Not 0.5.0, or a job mixing versions: only the values above 100
        # come from 0.5.0.
        return _divide_series(utilization, only_above_100=True)
    if any(v > 100 for v in values):
        # Multiples of 100, one above 100: 0.5.0.
        return _divide_series(utilization)
    hundreds = [t for t, v in recent if v == 100]
    if not hundreds:
        # Zeros only.
        return utilization
    # Only zeros and 100. Unable to determine if we are before 0.5.0, in 0.5.0, or in 0.5.1.
    # Let's just return nothing (no data).
    return None


def _nvml(metric_to_data: dict[str, list[dict]]) -> bool:
    """NVML mode always publishes the memory utilization, never a DCGM-only one."""
    return bool(metric_to_data.get(_MEMORY)) and not any(
        metric_to_data.get(name) for name in _DCGM_ONLY
    )


def _divide_series(utilization: list[dict], only_above_100: bool = False) -> list[dict]:
    """Divide by 100 the real values 0.5.0 may have sent, or only those above 100."""
    return [
        {
            **s,
            "values": [
                [t, _divide(float(t), float(v), only_above_100)] for t, v in s["values"]
            ],
        }
        for s in utilization
    ]


def _divide(timestamp: float, value: float, only_above_100: bool) -> float:
    if (
        timestamp < X100_SINCE
        or value >= DCGM_FP64_BLANK
        or (only_above_100 and value <= 100)
    ):
        return value
    return value / 100
