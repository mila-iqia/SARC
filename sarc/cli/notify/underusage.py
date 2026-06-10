import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import gifnoc
import simple_parsing

from sarc.config import config
from sarc.notifications.messages import build_admin_digest, build_user_dm
from sarc.notifications.underusage import (
    get_historical_stats,
    get_recurring_underusers,
    get_underusers,
)

logger = logging.getLogger(__name__)


def _now_utc() -> datetime:
    return datetime.now(UTC)


def _iso_week(dt: datetime) -> int:
    return dt.isocalendar().week


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
    min_rgu_hours: float | None = simple_parsing.field(
        default=None,
        alias=["--min-rgu-hours"],
        help="Minimum RGU-hours floor (overrides config).",
    )
    resource: str = "gpu"
    send: bool = simple_parsing.field(
        action="store_true", help="Send notifications (default: dry-run, prints only)."
    )
    no_dms: bool = simple_parsing.field(
        action="store_true",
        alias=["--no-dms"],
        help="Skip per-user DMs even when --send is set.",
    )
    as_of: str | None = simple_parsing.field(
        default=None,
        alias=["--as-of"],
        help="Simulate a run as of this date (YYYY-MM-DD, UTC). "
             "Default: now. Anchors the window, all queries, and the "
             "ISO-week DM parity.",
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

        window_days = (
            self.window_days if self.window_days is not None else ncfg.window_days
        )
        min_ratio = self.min_ratio if self.min_ratio is not None else ncfg.min_ratio
        min_rgu_hours = (
            self.min_rgu_hours if self.min_rgu_hours is not None else ncfg.min_rgu_hours
        )

        if self.as_of is not None:
            try:
                parsed = datetime.fromisoformat(self.as_of)
                end = parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)
            except ValueError:
                logger.error("Invalid --as-of date %r: expected YYYY-MM-DD", self.as_of)
                return -1
        else:
            end = _now_utc()
        start = end - timedelta(days=window_days)
        period = f"{start.date()} – {end.date()}"

        week_num = _iso_week(end)
        dms_eligible = not self.no_dms and week_num % 2 == 0

        if self.as_of is not None and end > _now_utc():
            print("Note: --as-of is in the future; the window may contain no jobs.")
            print()

        if not self.send:
            print("=== DRY RUN — nothing will be sent ===")
            print()

        if dms_eligible:
            print(f"ISO week {week_num} (even) — DMs eligible this run.")
        else:
            print(f"ISO week {week_num} (odd) — digest-only this run, no DMs.")
        print()

        rows = get_underusers(
            start,
            end,
            min_ratio=min_ratio,
            min_rgu_hours=min_rgu_hours,
            resource=self.resource,
            exclude_zero_usage=True,
        )
        historical = get_historical_stats(
            end,
            min_ratio=min_ratio,
            min_rgu_hours=min_rgu_hours,
            resource=self.resource,
            exclude_zero_usage=True,
        )
        recurring = get_recurring_underusers(
            end,
            min_ratio=min_ratio,
            min_rgu_hours=min_rgu_hours,
            resource=self.resource,
            window_weeks=ncfg.recurrence_window_weeks,
            cluster_share_threshold=ncfg.recurrence_cluster_share,
            exclude_zero_usage=True,
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
            recurring=recurring,
        )
        print("=== Admin Digest ===")
        print(digest)

        if rows and dms_eligible:
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
