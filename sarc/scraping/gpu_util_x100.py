"""Undo the x100 that slurm-job-exporter 0.5.0 puts on the true GPU utilization.

In DCGM mode, 0.5.0 publishes `slurm_job_utilization_gpu` as DCGM's integer
percentage times 100 (0, 100, ..., 10000); 0.5.1 (#73) and the NVML mode
publish the percentage itself. The exporter exposes no version, so the job
is judged by its values: only 0.5.0 sends more than 100, and it sends
multiples of 100 only. A job sampled only at 0 and 100 stays undecided where
and while a cluster ran 0.5.0 (X100_UNTIL). The exporter's mode is not a
judgement input: NVML never published a value above 100, and only clusters
that ran 0.5.0 have an X100_UNTIL entry.

Like gpu_activity_series, the job is judged as a whole: in a job spanning
nodes on different exporter versions, or an exporter upgrade, the samples
at 100 may be misread.

X100_UNTIL comes from the prod database, queried on 2026-11-10 before any
rescale, where a gpu_utilization max above 1.0 is a raw sample above 100.
Only rorqual ever sent the x100, last in a job ended 2026-10-07 21:17:52 UTC,
though it kept storing gpu_utilization after that date:

    SELECT c.name, max(j.end_time) AS last_x100_end
    FROM slurm_jobs j
    JOIN clusters c ON c.id = j.cluster_id
    JOIN jobstatisticdb s ON s.job_id = j.id AND s.name = 'gpu_utilization'
    WHERE s.max > 1.0 AND j.end_time >= '2026-09-24 17:16:04+00'
    GROUP BY c.name;

X100_UNTIL takes the midnight after rorqual's last x100.
"""

import logging
import math
from datetime import UTC, datetime

from sarc.scraping.dcgm import DCGM_FP64_BLANK

logger = logging.getLogger(__name__)

# Publication time of slurm-job-exporter 0.5.0, the x100: clusters are assumed
# to run official releases only, so samples before it are not 0.5.0's. 0.5.1,
# the fix, was published on 2026-10-07 15:04:01 UTC.
X100_SINCE = datetime(2026, 9, 24, 17, 16, 4, tzinfo=UTC).timestamp()
# The clusters that ran 0.5.0, each with a time from which none of its nodes
# did. Elsewhere, or from that time on, a sample above 100 is still divided,
# and logged; a sample at 100 is 100 %, unless its job sent nothing but
# multiples of 100 with one above 100 there.
X100_UNTIL: dict[str, float] = {
    "rorqual": datetime(2026, 10, 8, tzinfo=UTC).timestamp()
}


def rescale_gpu_utilization(
    utilization: list[dict], cluster_name: str
) -> list[dict] | None:
    """Divide by 100 the samples of `utilization` that exporter 0.5.0 sent.

    `utilization` holds the job's true utilization series. Returns None when
    the job's samples at 100 may be 1 % or 100 %.
    """
    samples = [(float(t), float(v)) for s in utilization for t, v in s["values"]]
    recent = [(t, v) for t, v in samples if _may_be_x100(t, v)]
    if not recent:
        return utilization
    # A cluster absent from X100_UNTIL never ran 0.5.0.
    x100_until = X100_UNTIL.get(cluster_name, X100_SINCE)
    unexpected_at = _first_unexpected_x100(recent, x100_until)
    if unexpected_at is not None:
        logger.warning(
            f"cluster {cluster_name}: gpu_utilization above 100 at "
            f"{datetime.fromtimestamp(unexpected_at, UTC).isoformat(timespec='seconds')}, "
            f"outside X100_UNTIL: slurm-job-exporter 0.5.0? Add the cluster to "
            f"X100_UNTIL, or its samples at 100 count as 100 %."
        )

    values = [v for _, v in recent]
    if any(v % 100 != 0 for v in values):
        # Not 0.5.0, or a job mixing versions: only the values above 100
        # come from 0.5.0.
        return _divide_series(utilization, x100_until=X100_SINCE)
    if any(v > 100 for v in values):
        # Multiples of 100, one above 100: 0.5.0. From X100_UNTIL on, its
        # samples at 100 are 0.5.1's (100 %)...
        if unexpected_at is not None:
            # ...unless it sent a value more than 100 there: X100_UNTIL does not
            # hold for this job, whose samples at 100 are 0.5.0's too.
            x100_until = math.inf
        return _divide_series(utilization, x100_until)
    hundreds = [t for t, v in recent if v == 100]
    if not hundreds:
        # Zeros only.
        return utilization
    if all(t >= x100_until for t in hundreds):
        # Zeros and 100, none while the cluster ran 0.5.0: 100 %.
        return utilization
    # Zeros and 100, some while the cluster ran 0.5.0: 1 % or 100 %.
    return None


def _may_be_x100(timestamp: float, value: float) -> bool:
    """A real sample (no DCGM BLANK sentinel, no NaN) that 0.5.0 may have sent."""
    return timestamp >= X100_SINCE and value < DCGM_FP64_BLANK


def _first_unexpected_x100(
    recent: list[tuple[float, float]], x100_until: float
) -> float | None:
    """The time of the first sample above 100 from `x100_until` on, when the
    cluster ran no 0.5.0."""
    times = [t for t, v in recent if v > 100 and t >= x100_until]
    return min(times, default=None)


def _divide_series(utilization: list[dict], x100_until: float) -> list[dict]:
    """Divide by 100 the real values 0.5.0 may have sent: those above 100, and
    the others taken before `x100_until`."""
    return [
        {
            **s,
            "values": [
                [t, _divide(float(t), float(v), x100_until)] for t, v in s["values"]
            ],
        }
        for s in utilization
    ]


def _divide(timestamp: float, value: float, x100_until: float) -> float:
    if not _may_be_x100(timestamp, value):
        return value
    if value > 100:
        # Only 0.5.0 sends more than 100, whatever the date.
        return value / 100
    if timestamp < x100_until:
        # A 0 or a 100 while the job ran 0.5.0.
        return value / 100
    return value
