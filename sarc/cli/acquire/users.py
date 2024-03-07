from __future__ import annotations

from dataclasses import dataclass

from simple_parsing import field

from sarc.ldap.acquire import run as update_user_records
from sarc.ldap.backfill import user_record_backfill
from sarc.traces import using_trace


@dataclass
class AcquireUsers:
    prompt: bool = field(
        action="store_true",
        help="Provide a prompt for manual matching if automatic matching fails (default: False)",
    )

    backfill: bool = field(
        action="store_true",
        help="Backfill record history from mymila",
    )

    def execute(self) -> int:
        if self.backfill:
            with using_trace("AcuireUsers", "backfill") as span:
                span.add_event("Backfilling record history from mymila ...")
                user_record_backfill()

        update_user_records(prompt=self.prompt)
        return 0
