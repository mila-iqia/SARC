"""Shared test helper for seeding GPU jobs (+ optional gpu_sm_occupancy stat).

Not collected by pytest (leading underscore). The existing JobFactory builds a
SlurmJobDB but does not create the gpu_sm_occupancy JobStatisticDB stat, so the
notification tests need this dedicated helper.
"""

import copy
from datetime import datetime, timedelta
from pathlib import Path

from sarc.db.job import JobStatisticDB, SlurmJobDB
from tests.db.factory import base_job

DEFAULT_GPU_TYPE = "A100-SXM4-80GB"

_REPO_ROOT = Path(__file__).parent.parent.parent.parent


UNDERUSAGE_REPORT_TEMPLATE = """{name}
{window_weeks} {window_range} {rgu_allocated} {rgu_unit} {avg_utilization}
leaving {rgu_wasted} {rgu_unit} unused (or about {user_wasted} {user_unit})
bottom_jobs_count:{bottom_jobs_count}
```
{bottom_jobs_section}
```
Track your usage over time: {dashboard_url}
"""


USAGE_REPORT_TEMPLATE = """{name}
{window_weeks} {window_range} {rgu_allocated} {rgu_unit} (or about
{user_allocated} {user_unit}) {avg_utilization}
top_jobs_count:{top_jobs_count}
```
{top_jobs_section}
```
bottom_jobs_count:{bottom_jobs_count}
```
{bottom_jobs_section}
```
Track your usage over time: {dashboard_url}
"""


def add_gpu_job(
    session,
    *,
    user_id: int,
    cluster_id: int,
    elapsed_h: float,
    submit_time: datetime,
    job_id: int,
    gpu_type: str = DEFAULT_GPU_TYPE,
    utilization: float | None = None,
    requested_gres: int = 1,
    allocated_gres: int = 1,
    allocated_billing: int | None = None,
) -> SlurmJobDB:
    """Insert a COMPLETED GPU SlurmJobDB at the literal *submit_time* and return it.

    end_time = submit_time + elapsed_h. When *utilization* is not None, also adds a
    gpu_sm_occupancy JobStatisticDB stat with that mean.
    """
    job_data = copy.deepcopy(base_job)
    job_data.pop("cluster_name")
    job_data.update(
        {
            "sarc_user_id": user_id,
            "cluster_id": cluster_id,
            "elapsed_time": int(elapsed_h * 3600),
            "submit_time": submit_time,
            "start_time": submit_time + timedelta(seconds=60),
            "end_time": submit_time + timedelta(hours=elapsed_h),
            "job_id": job_id,
            "requested_gres_gpu": requested_gres,
            "allocated_gres_gpu": allocated_gres,
            "allocated_gpu_type": gpu_type,
            "harmonized_gpu_type": gpu_type,
            "job_state": "COMPLETED",
        }
    )
    if allocated_billing is not None:
        job_data["allocated_billing"] = allocated_billing
    job = SlurmJobDB(**job_data)
    session.add(job)
    session.flush()
    if utilization is not None:
        session.add(
            JobStatisticDB(
                job_id=job.id,
                name="gpu_sm_occupancy",
                mean=utilization,
                std=None,
                q05=None,
                q25=None,
                median=None,
                q75=None,
                max=None,
            )
        )
    return job
