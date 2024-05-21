from __future__ import annotations

from dataclasses import dataclass

from simple_parsing import field

from sarc.cache import CachePolicy
from sarc.ldap.acquire import run as update_user_records
from sarc.ldap.backfill import user_record_backfill


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

    force: bool = field(
        action="store_true",
        help="Force recalculating the data rather than use the cache",
    )

    no_fetch: bool = field(
        action="store_true",
        help="Use cached data",
    )

    def execute(self) -> int:
        if self.force:
            assert not self.no_fetch
            cache_policy = CachePolicy.refresh
        elif self.no_fetch:
            cache_policy = CachePolicy.always
        else:
            cache_policy = CachePolicy.use

        if self.backfill:
            user_record_backfill(cache_policy=cache_policy)

        update_user_records(prompt=self.prompt, cache_policy=cache_policy)
        return 0
