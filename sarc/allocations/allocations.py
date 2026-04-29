from datetime import date

import pandas as pd

from sarc.config import config
from sarc.db.allocation import AllocationDB, get_allocations


def increment(a: int | None, b: int | None) -> int:
    if a is None:
        return b or 0

    if b is None:
        return a

    return a + b


def get_allocation_summaries(
    cluster_name: str | list[str], start: None | date = None, end: None | date = None
) -> pd.DataFrame:
    with config().db.session() as sess:
        allocations = get_allocations(sess, cluster_name, start=start, end=end)

    def allocation_key(allocation: AllocationDB) -> tuple[str, date, date]:
        return (allocation.cluster.cluster_name, allocation.start, allocation.end)

    summaries: dict[tuple[str, date, date], AllocationDB] = {}
    for allocation in allocations:
        key = allocation_key(allocation)
        if key in summaries:
            for field in ["cpu_year", "gpu_year", "rgu_year", "vcpu_year", "vgpu_year"]:
                setattr(
                    summaries[key],
                    field,
                    increment(
                        getattr(summaries[key], field), getattr(allocation, field)
                    ),
                )

            for field in [
                "project_size",
                "project_inodes",
                "nearline",
                "dCache",
                "object",
                "cloud_volume",
                "cloud_shared",
            ]:
                setattr(
                    summaries[key],
                    field,
                    increment(
                        getattr(summaries[key], field), getattr(allocation, field)
                    ),
                )
        else:
            summaries[key] = allocation

    summaries_l = list(summaries.values())

    return pd.DataFrame(
        [summary.model_dump(exclude={"id", "resource_name"}) for summary in summaries_l]
    )
