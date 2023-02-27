from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from sarc.allocations.allocations import (
    Allocation,
    AllocationCompute,
    AllocationRessources,
    AllocationStorage,
    get_allocations_collection,
)


def convert_csv_row_to_allocation(
    cluster_name: str,
    resource_name: str,
    group_name: str,
    start: datetime,
    end: datetime,
    cpu_year: None | int = None,
    gpu_year: None | int = None,
    vcpu_year: None | int = None,
    vgpu_year: None | int = None,
    project_size: None | str = None,
    project_inodes: None | str = None,
    nearline_size: None | str = None,
):
    return Allocation(
        cluster_name=cluster_name,
        resource_name=resource_name,
        group_name=group_name,
        timestamp=datetime.utcnow(),
        start=start,
        end=end,
        resources=AllocationRessources(
            compute=AllocationCompute(
                gpu_year=gpu_year,
                cpu_year=cpu_year,
                vcpu_year=vcpu_year,
                vgpu_year=vgpu_year,
            ),
            storage=AllocationStorage(
                project_size=project_size,
                project_inodes=project_inodes,
                nearline=nearline_size,
                dCache=None,
                object=None,
                cloud_volume=None,
                cloud_shared=None,
            ),
        ),
    )


@dataclass
class AcquireAllocations:
    file: Path

    def execute(self) -> int:
        collection = get_allocations_collection()

        with open(self.file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(
                f, skipinitialspace=True, restkey="garbage", restval=""
            )
            for row in reader:
                row.pop("garbage", None)

                for key in list(row.keys()):
                    if row[key].strip(" ") == "":
                        row.pop(key)

                try:
                    allocation = convert_csv_row_to_allocation(**row)
                except Exception as e:  # pylint: disable=broad-exception-caught
                    print(f"Skipping row: {row}")
                    print(e)
                    continue

                collection.add(allocation)

        return 0
