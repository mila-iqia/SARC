"""Opt-in Slurm/NVML GPU accounting, separate from DCGM time series.

Slurm 26.05's JSON calls TRESUsageInMax ``tres/requested/max``. GPU
values are sampled task high-water marks and can aggregate multiple GPUs.
They are neither time averages nor simultaneous whole-job peaks.

Raw JSON (including absent fields, unavailable values and step identities)
is retained in the normal jobs-cache entry. Statistic names retain source,
aggregation and units; only their ``max`` column is populated. Sampled zero
does not establish that a job was idle throughout its execution.

Contracts: https://slurm.schedmd.com/gres.html#Accounting and Slurm tag
slurm-26-05-4-1, src/plugins/data_parser/v0.0.45/parsers.c,
src/plugins/jobacct_gather/common/common_jag.c, src/plugins/gpu/nvml/gpu_nvml.c.
"""

import json
import math
from itertools import batched

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlmodel import Session, col, delete, select, tuple_

from sarc.config import ClusterConfig
from sarc.db.job import JobStatisticDB, SlurmJobDB
from sarc.models.job import SlurmState
from sarc.scraping.jobs_utils import parse_in_timezone

BATCH_SIZE = 100
ACCOUNTING_STATISTICS = {
    "gpumem": "sacct_gpu_memory_task_peak_bytes",
    "gpuutil": "sacct_gpu_sm_task_peak_percent",
}
FINISHED_STATES = {
    SlurmState.BOOT_FAIL,
    SlurmState.CANCELLED,
    SlurmState.COMPLETED,
    SlurmState.DEADLINE,
    SlurmState.FAILED,
    SlurmState.NODE_FAIL,
    SlurmState.OUT_OF_MEMORY,
    SlurmState.PREEMPTED,
    SlurmState.REVOKED,
    SlurmState.TIMEOUT,
}


def _tres_value(records: list[dict] | None, name: str) -> float | None:
    matches = [
        record
        for record in records or []
        if record.get("type") == "gres" and record.get("name") == name
    ]
    if len(matches) != 1:
        return None
    value = matches[0].get("count")
    if isinstance(value, dict):
        if value.get("set") is False or value.get("infinite"):
            return None
        value = value.get("number")
    # Reject formatted units, booleans and Slurm NO_VAL/INFINITE sentinels.
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if value < 0 or value >= 2**64 - 2 or not math.isfinite(value):
        return None
    return float(value)


def compute_gpu_accounting(raw_job: dict) -> dict[str, float]:
    """Maximum sampled task peak across steps, without summing allocations."""
    values = {}
    for step in raw_job.get("steps") or []:
        records = step.get("tres", {}).get("requested", {}).get("max")
        for tres, name in ACCOUNTING_STATISTICS.items():
            if (value := _tres_value(records, tres)) is not None:
                values[name] = max(values.get(name, 0), value)
    return values


def parse_gpu_accounting(
    sess: Session, raw: bytes, cluster_id: int, cluster: ClusterConfig
) -> None:
    """Replace matched jobs' accounting samples, leaving other statistics intact."""
    if not cluster.sacct_gpu_accounting:
        return
    data = json.loads(raw[raw.find(b"{") :])
    if data.get("errors") or not isinstance(data.get("jobs"), list):
        raise ValueError("sacct GPU accounting did not return a valid jobs list")
    refs = {}
    for job in data["jobs"]:
        submitted = parse_in_timezone(job["time"]["submission"])
        ref = (job["job_id"], submitted)
        if ref in refs and refs[ref] != job:
            raise ValueError(f"Conflicting GPU accounting records for job {ref}")
        refs[ref] = job
    # A later invalid job or failed write must not leave earlier batches refreshed.
    with sess.begin_nested():
        for batch in batched(refs, BATCH_SIZE):
            jobs = sess.exec(
                select(SlurmJobDB).where(
                    SlurmJobDB.cluster_id == cluster_id,
                    col(SlurmJobDB.job_state).in_(FINISHED_STATES),
                    col(SlurmJobDB.end_time).is_not(None),
                    col(SlurmJobDB.allocated_gres_gpu) > 0,
                    tuple_(SlurmJobDB.job_id, SlurmJobDB.submit_time).in_(batch),
                )
            ).all()
            statistics = []
            stale = []
            for job in jobs:
                current = compute_gpu_accounting(refs[(job.job_id, job.submit_time)])
                stale.extend(
                    (job.id, name)
                    for name in ACCOUNTING_STATISTICS.values()
                    if name not in current
                )
                statistics.extend(
                    dict(job_id=job.id, name=name, max=value)
                    for name, value in current.items()
                )
            if stale:
                sess.exec(
                    delete(JobStatisticDB).where(
                        tuple_(JobStatisticDB.job_id, JobStatisticDB.name).in_(stale)
                    )
                )
            if statistics:
                stmt = pg_insert(JobStatisticDB).values(statistics)
                sess.exec(
                    stmt.on_conflict_do_update(
                        index_elements=["name", "job_id"],
                        set_={
                            "max": stmt.excluded.max,
                            **dict.fromkeys(
                                ("mean", "std", "q05", "q25", "median", "q75")
                            ),
                        },
                    )
                )
