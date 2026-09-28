"""NVIDIA DCGM sentinel values and helpers.

DCGM publishes special "BLANK" values when a metric is unavailable
(e.g. profiling paused, GPU not accessible, MIG slice not enabled). These
sentinels reach SARC via Prometheus exporters such as dcgm-exporter and
pollute statistics if not filtered before aggregation.

Constants and semantics mirror NVIDIA/DCGM ``testing/python3/dcgmvalue.py``:
https://github.com/NVIDIA/DCGM/blob/master/testing/python3/dcgmvalue.py
"""

import math

from sqlalchemy import ColumnElement, literal_column

DCGM_INT64_BLANK = 0x7FFF_FFFF_FFFF_FFF0
DCGM_INT32_BLANK = 0x7FFF_FFF0
DCGM_FP64_BLANK = 140737488355328.0  # 2**47 == 0x8000_0000_0000

DCGM_FP64_NOT_FOUND = DCGM_FP64_BLANK + 1.0
DCGM_FP64_NOT_SUPPORTED = DCGM_FP64_BLANK + 2.0
DCGM_FP64_NOT_PERMISSIONED = DCGM_FP64_BLANK + 3.0

# `slurm_job_power_gpu` is exposed in mW by the slurm-job-exporter and SARC
# stores it untouched, so the stored power statistics are mW too.
DEFAULT_MIN_POWER_MW = 100_000.0

# Postgres float8 literals: NaN sorts *above* every value, so `>= floor`
# alone would match NaN rows. Compared as literals (not bind parameters),
# same trick as sarc.api.metrics._is_real which sidesteps pg8000 quirks.
_NAN_LITERAL = literal_column("'NaN'::float8")
_INFINITY_LITERAL = literal_column("'Infinity'::float8")


def dcgm_prof_blackout(
    sm_occupancy_max: float | None,
    gpu_memory_max: float | None,
    gpu_power_max: float | None,
    *,
    min_power_mw: float = DEFAULT_MIN_POWER_MW,
) -> bool:
    """True when a job's GPU statistics show a DCGM PROF-family blackout.

    The DCGM PROF metrics (SM occupancy, DRAM activity, fp ratios) share one
    hardware counter access path: when it is blocked (driver profiling
    restriction, CUPTI conflict), they all report *exact zeros* for a node
    window while the DEV power metric keeps measuring the real work. SARC
    filters BLANK sentinels and NaN samples at parse time, so a stored 0 is
    an assertion "measured zero for the whole job" -- the false assertion
    this detector catches.

    The rule: measured SM occupancy AND measured DRAM activity both exactly
    0, while power max is at least `min_power_mw` (mW, as stored). A GPU
    burning 100 W+ for a job necessarily touched DRAM, so the three
    coexisting measurements cannot all be true: the PROF family is lying.

    A missing (None) statistic is an honest "no usable sample" -- never
    flagged, and so are NaN/infinity. `gpu_utilization` and the fp* ratios
    are deliberately absent from the rule: they are the same PROF family (no
    extra signal, and their zeros/absences add only noise), and
    `gpu_utilization` switched source from PROF SM_UTIL_RATIO to DEV GPU_UTIL
    (slurm-job-exporter #72), so requiring it to be 0 would stop matching
    blackouts after that exporter change.

    >>> dcgm_prof_blackout(0.0, 0.0, 150_000.0)
    True
    >>> dcgm_prof_blackout(0.0, 0.001, 150_000.0)
    False
    >>> dcgm_prof_blackout(0.0, 0.0, 40_000.0)
    False
    >>> dcgm_prof_blackout(None, 0.0, 150_000.0)
    False
    >>> dcgm_prof_blackout(float("nan"), 0.0, 150_000.0)
    False
    >>> dcgm_prof_blackout(0.0, 0.0, float("inf"))
    False
    """
    if sm_occupancy_max != 0 or gpu_memory_max != 0:
        return False
    return (
        gpu_power_max is not None
        and math.isfinite(gpu_power_max)
        and gpu_power_max >= min_power_mw
    )


def dcgm_prof_blackout_conditions(
    sm_occupancy_max,
    gpu_memory_max,
    gpu_power_max,
    *,
    min_power_mw: float = DEFAULT_MIN_POWER_MW,
) -> list[ColumnElement[bool]]:
    """SQL mirror of dcgm_prof_blackout, as WHERE conditions on statistics columns.

    Takes the three statistic *expressions* (e.g. the ``max`` column of one
    aliased ``JobStatisticDB`` join per statistic, or pivoted ``job_series``
    columns) and returns conditions to AND into a query, so the rule lives
    in exactly one place next to its Python twin.

    NULL semantics match the Python version: comparisons with NULL are
    unknown, so rows missing a statistic are excluded. NaN needs explicit
    care: PostgreSQL sorts it *above* every float, so ``power >= min`` alone
    would match NaN rows -- it is excluded explicitly, along with +Infinity
    (-Infinity falls out of the ``>=``; infinities would also fail the
    Python ``math.isfinite`` check). DCGM BLANK sentinels need no guard:
    the stats pipeline already drops them before they reach the database.
    """
    return [
        sm_occupancy_max == 0,
        gpu_memory_max == 0,
        gpu_power_max.is_not(None),
        gpu_power_max != _NAN_LITERAL,
        gpu_power_max != _INFINITY_LITERAL,
        gpu_power_max >= min_power_mw,
    ]
