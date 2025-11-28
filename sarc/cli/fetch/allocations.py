from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from sarc.cache import Cache


@dataclass
class FetchAllocations:
    file: Path

    def execute(self) -> int:
        with Cache(subdirectory="allocations").create_entry(datetime.now(UTC)) as ce:
            ce.add_value("key", self.file.read_bytes())
        return 0
