import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import gifnoc
import simple_parsing

from sarc.config import config
from sarc.notifications.messages import build_admin_digest, build_user_dm
from sarc.notifications.underusage import get_historical_stats, get_underusers

logger = logging.getLogger(__name__)


def _now_utc() -> datetime:
    return datetime.now(UTC)


@dataclass
class UnderusageNotifyCommand:
    """Preview or send resource-underusage notifications (dry-run by default)."""

    config: Path | None = None
    window_days: int | None = simple_parsing.field(
        default=None,
        alias=["--window-days"],
        help="Analysis window in days (overrides config).",
    )
    min_ratio: float | None = simple_parsing.field(
        default=None,
        alias=["--min-ratio"],
        help="Minimum waste ratio threshold (overrides config).",
    )
    min_gpu_hours: float | None = simple_parsing.field(
        default=None,
        alias=["--min-gpu-hours"],
        help="Minimum GPU-hours floor (overrides config).",
    )
    resource: str = "gpu"
    send: bool = simple_parsing.field(
        action="store_true",
        help="Send notifications (default: dry-run, prints only).",
    )
    no_dms: bool = simple_parsing.field(
        action="store_true",
        alias=["--no-dms"],
        help="Skip per-user DMs even when --send is set.",
    )

    def execute(self) -> int:
        if self.config is None:
            return self._exec()
        with gifnoc.use(self.config):
            return self._exec()

    def _exec(self) -> int:
        ncfg = config().notifications
        if ncfg is None:
            logger.error("No notifications configuration found in config")
            return -1

        window_days = self.window_days if self.window_days is not None else ncfg.window_days
        min_ratio = self.min_ratio if self.min_ratio is not None else ncfg.min_ratio
        min_gpu_hours = self.min_gpu_hours if self.min_gpu_hours is not None else ncfg.min_gpu_hours

        end = _now_utc()
        start = end - timedelta(days=window_days)
        period = f"{start.date()} – {end.date()}"

        if not self.send:
            print("=== DRY RUN — nothing will be sent ===")
            print()

        rows = get_underusers(
            start,
            end,
            min_ratio=min_ratio,
            min_gpu_hours=min_gpu_hours,
            resource=self.resource,
        )
        historical = get_historical_stats(
            end,
            min_ratio=min_ratio,
            min_gpu_hours=min_gpu_hours,
            resource=self.resource,
        )

        print(f"Recipients ({len(rows)} user(s) flagged):")
        for row in rows:
            print(f"  - {row.display_name} ({row.email})")
        print()

        digest = build_admin_digest(
            rows,
            period=period,
            top_n=ncfg.digest_top_n,
            historical=historical,
        )
        print("=== Admin Digest ===")
        print(digest)

        if rows:
            print()
            print("=== DM Previews ===")
            for row in rows:
                print(f"\n--- {row.display_name} ({row.email}) ---")
                dm = build_user_dm(
                    row,
                    window_days=window_days,
                    dashboard_url=ncfg.dashboard_url,
                    help_section=ncfg.help_section,
                )
                print(dm)

        return 0
