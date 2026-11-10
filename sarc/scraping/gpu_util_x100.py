"""Undo the x100 that slurm-job-exporter 0.5.0 puts on the true GPU utilization.

In DCGM mode, 0.5.0 publishes `slurm_job_utilization_gpu` as DCGM's integer
percentage times 100 (0, 100, ..., 10000); 0.5.1 (#73) and the NVML mode
publish the percentage itself. The exporter exposes no version, so the job
is judged by its values: only DCGM 0.5.0 sends more than 100, and it sends
multiples of 100 only. A job sampled only at 0 and 100 stays undecided where
and while a cluster ran 0.5.0 (X100_UNTIL).

Like gpu_activity_series, the job is judged as a whole: in a job spanning
nodes on different exporter versions, or an exporter upgrade, the samples
at 100 may be misread.

X100_UNTIL comes from the prod database, queried on 2026-11-10 before any
rescale, where a gpu_utilization max above 1.0 is a raw sample above 100.
Only rorqual ever sent the x100:

    SELECT c.name, count(*) AS x100_jobs, max(j.end_time) AS last_x100_end
    FROM slurm_jobs j
    JOIN clusters c ON c.id = j.cluster_id
    JOIN jobstatisticdb s ON s.job_id = j.id AND s.name = 'gpu_utilization'
    WHERE s.max > 1.0 AND s.max <> 'NaN'::float8
      AND j.end_time >= '2026-09-24 17:16:04+00'
    GROUP BY c.name
    ORDER BY c.name;

      name   | x100_jobs |     last_x100_end
    ---------+-----------+------------------------
     rorqual |      1071 | 2026-10-07 21:17:52+00

though the GPU utilization kept being collected after that date:

    SELECT c.name, count(*) AS util_jobs, max(j.end_time) AS last_end
    FROM slurm_jobs j
    JOIN clusters c ON c.id = j.cluster_id
    JOIN jobstatisticdb s ON s.job_id = j.id AND s.name = 'gpu_utilization'
    WHERE j.end_time >= '2026-09-24 17:16:04+00'
    GROUP BY c.name
    ORDER BY c.name;

      name   | util_jobs |        last_end
    ---------+-----------+------------------------
     mila    |    309105 | 2026-11-10 17:02:26+00
     narval  |       268 | 2026-11-09 13:22:55+00
     rorqual |     18424 | 2026-11-10 17:02:15+00

On rorqual, a node's first job whose max lies strictly between 0 and 1.0 (a
raw value 0.5.0 cannot send) came up to weeks after 2026-10-07. But every
such late node had run dozens of jobs since without one x100: on 0.5.0, those
before that first job would all have peaked at 1 % or less. Single-node jobs:

    WITH per_node AS (
      SELECT j.nodes->>0 AS node,
             count(*) FILTER (WHERE j.end_time > '2026-10-07 21:17:52+00') AS jobs_after_last_x100,
             min(j.end_time) FILTER (WHERE s.max > 0 AND s.max < 1.0
                                       AND j.end_time >= '2026-10-07 15:04:01+00') AS first_proof_end
      FROM slurm_jobs j
      JOIN clusters c ON c.id = j.cluster_id
      JOIN jobstatisticdb s ON s.job_id = j.id AND s.name = 'gpu_utilization'
      WHERE c.name = 'rorqual'
        AND j.end_time >= '2026-09-24 17:16:04+00'
        AND jsonb_array_length(j.nodes) = 1
        AND s.max <> 'NaN'::float8
      GROUP BY 1
    )
    SELECT node, jobs_after_last_x100, first_proof_end
    FROM per_node
    WHERE first_proof_end IS NULL OR first_proof_end > '2026-10-10'
    ORDER BY first_proof_end NULLS FIRST;

    20 rows: rg12901 and rg12803 with 0 jobs after the last x100 and no
    first_proof_end; 18 nodes with 47 to 219 jobs after it and a
    first_proof_end from 2026-10-10 15:03:50+00 to 2026-11-02 06:12:19+00.

X100_UNTIL takes the midnight after rorqual's last x100.
"""

import logging
from datetime import UTC, datetime

from sarc.scraping.dcgm import DCGM_FP64_BLANK

logger = logging.getLogger(__name__)

# Publication time of slurm-job-exporter 0.5.0, the x100: clusters are assumed
# to run official releases only, so samples before it are not 0.5.0's. 0.5.1,
# the fix, was published on 2026-10-07 15:04:01 UTC.
X100_SINCE = datetime(2026, 9, 24, 17, 16, 4, tzinfo=UTC).timestamp()
# The clusters that ran 0.5.0, each with a time from which none of its nodes
# did. Elsewhere, or from that time on, a sample at 100 is 100 %, and a sample
# above 100 is logged.
X100_UNTIL: dict[str, float] = {
    "rorqual": datetime(2026, 10, 8, tzinfo=UTC).timestamp()
}

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
    utilization: list[dict], metric_to_data: dict[str, list[dict]], cluster_name: str
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
    unexpected = _first_unexpected_x100(recent, cluster_name)
    if unexpected is not None:
        logger.warning(
            f"cluster {cluster_name}: gpu_utilization above 100 at "
            f"{datetime.fromtimestamp(unexpected, UTC).isoformat(timespec='seconds')}, "
            f"outside X100_UNTIL: slurm-job-exporter 0.5.0? Add the cluster to "
            f"X100_UNTIL, or its samples at 100 count as 100 %."
        )

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
    until = X100_UNTIL.get(cluster_name)
    if until is None or all(t >= until for t in hundreds):
        # Zeros and 100, none while the cluster ran 0.5.0: 100 %.
        return utilization
    # Zeros and 100, some while the cluster ran 0.5.0: 1 % or 100 %.
    return None


def _first_unexpected_x100(
    recent: list[tuple[float, float]], cluster_name: str
) -> float | None:
    """The time of the first sample above 100 where X100_UNTIL expects no 0.5.0."""
    until = X100_UNTIL.get(cluster_name)
    times = [t for t, v in recent if v > 100 and (until is None or t >= until)]
    return min(times, default=None)


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
