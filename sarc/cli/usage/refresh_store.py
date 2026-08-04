import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from functools import partial

import simple_parsing
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlmodel import col, delete, select

from sarc.cli.usage.notify import _today_utc
from sarc.config import config
from sarc.db.cluster import SlurmClusterDB
from sarc.db.user_periods import UserPeriods
from sarc.notifications.usage import (
    _restrictive_action_flags,
    _run_concurrently,
    _week_anchor,
    classify_cycle,
)

logger = logging.getLogger(__name__)


@dataclass
class UsageRefreshStoreCommand:
    """Refresh the UserPeriods store (PowerBI + get_recurring_underusers fast path)."""

    as_of: str | None = simple_parsing.field(
        default=None,
        alias=["--as-of"],
        help="Simulate a run as of this date (YYYY-MM-DD, UTC). Default: now. "
        "Anchors the most-recent stored cycle.",
    )

    def execute(self) -> int:
        return self._exec()

    def _exec(self) -> int:
        ncfg = config.notifications
        if ncfg is None:
            logger.error("No notifications configuration found in config")
            return -1

        if self.as_of is not None:
            try:
                parsed = datetime.fromisoformat(self.as_of)
                end = (
                    parsed.replace(tzinfo=UTC)
                    if parsed.tzinfo is None
                    else parsed.astimezone(UTC)
                )
            except ValueError:
                logger.error("Invalid --as-of date %r: expected YYYY-MM-DD", self.as_of)
                return -1
        else:
            end = _today_utc()

        cycle_length_weeks = ncfg.usage_cycle_length_weeks
        history_cycles = ncfg.history_cycles
        anchor = _week_anchor(end)
        clusters = ncfg.clusters or None

        # Position 0 = most recent cycle, matching the ordering used throughout
        # sarc.notifications.usage (RecurringUserRow.pa_flags, cycle_flagged, ...).
        cycle_bounds = [
            (
                anchor - timedelta(weeks=(i + 1) * cycle_length_weeks),
                anchor - timedelta(weeks=i * cycle_length_weeks),
            )
            for i in range(history_cycles)
        ]

        classify_results = _run_concurrently(
            [
                partial(
                    classify_cycle,
                    c_start,
                    c_end,
                    min_waste_ratio=ncfg.min_waste_ratio,
                    min_waste_rgu_hours=ncfg.min_waste_rgu_hours,
                    personalized_action_min_waste_rgu_hours=ncfg.personalized_action_min_waste_rgu_hours,
                    recurrence_active_cycles=ncfg.recurrence_active_cycles,
                    cycle_length_weeks=cycle_length_weeks,
                    clusters=clusters,
                    utilization_ceiling=ncfg.utilization_ceiling,
                )
                for c_start, c_end in cycle_bounds
            ]
        )

        # elevated is a per-user (cross-cluster) escalation flag, derived from
        # each user's flagged sequence across positions -- same derivation
        # RecurringUserRow.restrictive_action_flags applies to pa_flags.
        flagged_by_user: dict[int, list[bool]] = {}
        for i, stats in enumerate(classify_results):
            for s in stats:
                if s.user_id not in flagged_by_user:
                    flagged_by_user[s.user_id] = [False] * history_cycles
                flagged_by_user[s.user_id][i] = s.flagged
        elevated_by_user = {
            uid: _restrictive_action_flags(flags)
            for uid, flags in flagged_by_user.items()
        }

        oldest_kept_end = cycle_bounds[-1][1]
        rows_written = 0
        rows_pruned = 0
        with config.db.session() as session:
            cluster_ids = {
                c.name: c.id for c in session.exec(select(SlurmClusterDB)).all()
            }

            for i, (c_start, c_end) in enumerate(cycle_bounds):
                for s in classify_results[i]:
                    cluster_id = cluster_ids.get(s.cluster)
                    if cluster_id is None:
                        logger.warning(
                            "Skipping user_id=%s: no clusters row matches "
                            "cluster=%r (position %d)",
                            s.user_id,
                            s.cluster,
                            i,
                        )
                        continue
                    insert_stmt = pg_insert(UserPeriods).values(
                        user_id=s.user_id,
                        cluster_id=cluster_id,
                        start_date=c_start,
                        end_date=c_end,
                        sm_occ_mean=s.sm_occ_mean,
                        rgu_hours=s.rgu_hours,
                        unused_rguh=s.wasted,
                        isunderuser=s.isunderuser,
                        flagged=s.flagged,
                        elevated=elevated_by_user[s.user_id][i],
                    )
                    upsert_stmt = insert_stmt.on_conflict_do_update(
                        index_elements=["user_id", "cluster_id", "end_date"],
                        set_={
                            "start_date": insert_stmt.excluded.start_date,
                            "sm_occ_mean": insert_stmt.excluded.sm_occ_mean,
                            "rgu_hours": insert_stmt.excluded.rgu_hours,
                            "unused_rguh": insert_stmt.excluded.unused_rguh,
                            "isunderuser": insert_stmt.excluded.isunderuser,
                            "flagged": insert_stmt.excluded.flagged,
                            "elevated": insert_stmt.excluded.elevated,
                        },
                    )
                    session.exec(upsert_stmt)
                    rows_written += 1

            prune_result = session.exec(
                delete(UserPeriods).where(col(UserPeriods.end_date) < oldest_kept_end)
            )
            rows_pruned = prune_result.rowcount
            session.commit()

        logger.info(
            "Refreshed user_periods store: %d rows upserted, %d rows pruned "
            "(older than %s)",
            rows_written,
            rows_pruned,
            oldest_kept_end,
        )
        return 0
