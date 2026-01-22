import csv
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import cast

from pydantic import ByteSize
from simple_parsing import field

from sarc.allocations.allocations import (
    Allocation,
    AllocationCompute,
    AllocationRessources,
    AllocationStorage,
    get_allocations_collection,
)
from sarc.cache import Cache
from sarc.traces import trace_decorator

logger = logging.getLogger(__name__)


@trace_decorator()
def convert_csv_row_to_allocation(
    cluster_name: str,
    resource_name: str,
    group_name: str,
    start: datetime,
    end: datetime,
    cpu_year: None | int = None,
    gpu_year: None | int = None,
    rgu_year: None | int = None,
    vcpu_year: None | int = None,
    vgpu_year: None | int = None,
    project_size: None | str = None,
    project_inodes: None | str = None,
    nearline_size: None | str = None,
) -> Allocation:
    return Allocation(
        cluster_name=cluster_name,
        resource_name=resource_name,
        group_name=group_name,
        timestamp=datetime.now(UTC),
        start=start,
        end=end,
        resources=AllocationRessources(
            compute=AllocationCompute(
                gpu_year=gpu_year,
                rgu_year=rgu_year,
                cpu_year=cpu_year,
                vcpu_year=vcpu_year,
                vgpu_year=vgpu_year,
            ),
            storage=AllocationStorage(
                project_size=cast(ByteSize, project_size),
                project_inodes=cast(ByteSize, project_inodes),
                nearline=cast(ByteSize, nearline_size),
                dCache=None,
                object=None,
                cloud_volume=None,
                cloud_shared=None,
            ),
        ),
    )


@dataclass
class ParseAllocations:
    since: str = field(help="Start parsing the cache from the specified date")

    def execute(self) -> int:
        collection = get_allocations_collection()
        cache = Cache(subdirectory="allocations")

        ts = datetime.fromisoformat(self.since)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=UTC)
        ts = ts.astimezone(UTC)

        for ce in cache.read_from(ts):
            for _, value in ce.items():  # noqa: PERF102
                reader = csv.DictReader(
                    value.decode("utf-8").split("\n"),
                    skipinitialspace=True,
                    restkey="garbage",
                    restval="",
                )
                for row in reader:
                    row.pop("garbage", None)

                    for key in list(row.keys()):
                        if row[key].strip(" ") == "":
                            row.pop(key)

                    try:
                        allocation = convert_csv_row_to_allocation(**row)  # type: ignore[arg-type]
                    except Exception as e:
                        logger.exception(f"Skipping row: {row}", exc_info=e)
                        continue

                    logger.info(f"Adding allocation: {allocation}")
                    collection.add(allocation)

        return 0
