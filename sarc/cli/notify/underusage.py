import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

import gifnoc
import simple_parsing

from sarc.config import config
from sarc.notifications.email import EmailClient
from sarc.notifications.messages import (
    build_admin_digest,
    build_usage_report,
    build_user_dm,
    split_usage_report_recipients,
)
from sarc.notifications.slack import SendStatus, SlackClient
from sarc.notifications.underusage import (
    get_all_users_usage,
    get_cycle_dates,
    get_historical_stats,
    get_recurring_underusers,
    get_underusers,
)

logger = logging.getLogger(__name__)


@dataclass
class _DeliveryResult:
    email: str
    display_name: str
    status: str  # "dm_sent" | "email_sent" | "skipped" | "failed"
    detail: str = field(default="")


def _delivery_counts(results: list[_DeliveryResult]) -> dict[str, int]:
    return {
        "dm_sent": sum(1 for r in results if r.status == "dm_sent"),
        "email_sent": sum(1 for r in results if r.status == "email_sent"),
        "skipped": sum(1 for r in results if r.status == "skipped"),
        "failed": sum(1 for r in results if r.status == "failed"),
    }


def _build_delivery_footer(results: list[_DeliveryResult], *, flagged: int) -> str:
    counts = _delivery_counts(results)
    lines = [
        "--- Delivery Summary ---",
        (
            f"flagged={flagged}  dm_sent={counts['dm_sent']}"
            f"  email_sent={counts['email_sent']}"
            f"  skipped={counts['skipped']}  failed={counts['failed']}"
        ),
    ]
    failures = [r for r in results if r.status == "failed"]
    if failures:
        lines.append("Failures:")
        for r in failures:
            lines.append(f"  - {r.display_name} ({r.email}): {r.detail}")
    return "\n".join(lines)


def _now_utc() -> datetime:
    return datetime.now(UTC)


def _iso_week(dt: datetime) -> int:
    return dt.isocalendar().week


@dataclass
class UnderusageNotifyCommand:
    """Preview or send resource-underusage notifications (dry-run by default)."""

    config: Path | None = None
    window_weeks: int | None = simple_parsing.field(
        default=None,
        alias=["--window-weeks"],
        help="Analysis window in weeks (overrides config).",
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

        window_weeks = (
            self.window_weeks if self.window_weeks is not None else ncfg.window_weeks
        )
        min_ratio = self.min_ratio if self.min_ratio is not None else ncfg.min_ratio
        min_rgu_hours = (
            self.min_rgu_hours if self.min_rgu_hours is not None else ncfg.min_rgu_hours
        )

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
            end = _now_utc()
            # Clip to midnight, today
            end.replace(hour=0, minute=0, second=0, microsecond=0)
        start = end - timedelta(weeks=window_weeks)
        period = f"{start.date()} – {end.date()}"

        week_num = _iso_week(end)
        # dms_eligible: week parity (controls preview section)
        dms_eligible = week_num % ncfg.cycle_length_weeks == 0
        # dms_will_send: additionally requires --no-dms gate and send_dms config (controls actual sends)
        dms_will_send = dms_eligible and not self.no_dms and ncfg.send_dms
        usage_report_eligible = week_num % ncfg.usage_report_every_weeks == 0
        usage_report_will_send = usage_report_eligible and ncfg.send_usage_report

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
        if usage_report_eligible:
            print(
                f"ISO week {week_num} (multiple of {ncfg.usage_report_every_weeks}) — Usage report eligible this run."
            )
        print()

        rows = get_underusers(
            start,
            end,
            min_ratio=min_ratio,
            min_rgu_hours=min_rgu_hours,
            top_jobs_per_user=ncfg.top_jobs_per_user,
            resource=self.resource,
            exclude_zero_usage=True,
        )
        historical = get_historical_stats(
            end,
            min_ratio=min_ratio,
            min_rgu_hours=min_rgu_hours,
            resource=self.resource,
            months=ncfg.historical_months,
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
            recurrence_display_cycles=ncfg.recurrence_display_cycles,
            recurrence_active_cycles=ncfg.recurrence_active_cycles,
            cycle_length_weeks=ncfg.cycle_length_weeks,
        )

        print(f"Recipients ({len(rows)} user(s) flagged):")
        for row in rows:
            print(f"  - {row.display_name} ({row.email})")
        print()

        cycle_dates = get_cycle_dates(
            end,
            ncfg.recurrence_display_cycles,
            cycle_length_weeks=ncfg.cycle_length_weeks,
        )
        digest = build_admin_digest(
            rows,
            period=period,
            cluster_share_threshold=ncfg.recurrence_cluster_share,
            cycle_length_weeks=ncfg.cycle_length_weeks,
            active_cycles=ncfg.recurrence_active_cycles,
            top_n=ncfg.digest_top_n,
            historical=historical,
            recurring=recurring,
            cycle_dates=cycle_dates,
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
                    window_weeks=window_weeks,
                    dashboard_url=ncfg.dashboard_url,
                    help_section=ncfg.help_section,
                )
                print(dm)

        usage_report_window_weeks = ncfg.usage_report_window_weeks
        report_recipients = []
        usage_report_skipped = []
        if usage_report_eligible:
            usage_start = end - timedelta(weeks=usage_report_window_weeks)
            usage_rows = get_all_users_usage(
                usage_start,
                end,
                top_jobs_per_user=ncfg.top_jobs_per_user,
                resource=self.resource,
            )
            underuser_emails = {r.email for r in rows}
            report_recipients, usage_report_skipped = split_usage_report_recipients(
                usage_rows, underuser_emails
            )
            if report_recipients:
                print()
                skip_note = (
                    f" ({len(usage_report_skipped)} already getting the underusage alert)"
                    if usage_report_skipped
                    else ""
                )
                print(
                    f"=== Usage Report Previews ({len(report_recipients)} recipient(s)){skip_note} ==="
                )
                for row in report_recipients:
                    print(f"\n--- {row.display_name} ({row.email}) ---")
                    report_text = build_usage_report(
                        row,
                        window_weeks=usage_report_window_weeks,
                        dashboard_url=ncfg.dashboard_url,
                        help_section=ncfg.help_section,
                    )
                    print(report_text)

        if not self.send:
            return 0

        # === SEND MODE ===
        slack_client = SlackClient(ncfg.slack.token)
        email_client = EmailClient(ncfg.email)

        delivery_results: list[_DeliveryResult] = []

        if dms_will_send:
            for row in rows:
                dm_text = build_user_dm(
                    row,
                    window_weeks=window_weeks,
                    dashboard_url=ncfg.dashboard_url,
                    help_section=ncfg.help_section,
                )
                slack_res = slack_client.dm_user(row.email, dm_text, preformatted=True)
                if slack_res.status == SendStatus.OK:
                    delivery_results.append(
                        _DeliveryResult(row.email, row.display_name, "dm_sent")
                    )
                elif slack_res.status == SendStatus.USER_NOT_FOUND:
                    email_res = email_client.send_plaintext(
                        row.email, f"GPU underusage alert ({period})", dm_text
                    )
                    if email_res.status == SendStatus.OK:
                        delivery_results.append(
                            _DeliveryResult(row.email, row.display_name, "email_sent")
                        )
                    else:
                        delivery_results.append(
                            _DeliveryResult(
                                row.email, row.display_name, "failed", email_res.detail
                            )
                        )
                else:
                    delivery_results.append(
                        _DeliveryResult(
                            row.email, row.display_name, "failed", slack_res.detail
                        )
                    )
        elif rows:
            if week_num % ncfg.cycle_length_weeks != 0:
                reason = "odd_week"
            elif self.no_dms:
                reason = "no_dms_flag"
            else:
                reason = "send_dms_disabled"
            for row in rows:
                delivery_results.append(
                    _DeliveryResult(row.email, row.display_name, "skipped", reason)
                )

        report_results: list[_DeliveryResult] = []
        if usage_report_will_send:
            for row in report_recipients:
                report_text = build_usage_report(
                    row,
                    window_weeks=usage_report_window_weeks,
                    dashboard_url=ncfg.dashboard_url,
                    help_section=ncfg.help_section,
                )
                slack_res = slack_client.dm_user(
                    row.email, report_text, preformatted=True
                )
                if slack_res.status == SendStatus.OK:
                    report_results.append(
                        _DeliveryResult(row.email, row.display_name, "dm_sent")
                    )
                elif slack_res.status == SendStatus.USER_NOT_FOUND:
                    email_res = email_client.send_plaintext(
                        row.email, f"GPU usage report ({period})", report_text
                    )
                    if email_res.status == SendStatus.OK:
                        report_results.append(
                            _DeliveryResult(row.email, row.display_name, "email_sent")
                        )
                    else:
                        report_results.append(
                            _DeliveryResult(
                                row.email, row.display_name, "failed", email_res.detail
                            )
                        )
                else:
                    report_results.append(
                        _DeliveryResult(
                            row.email, row.display_name, "failed", slack_res.detail
                        )
                    )
        elif usage_report_eligible and report_recipients:
            for row in report_recipients:
                report_results.append(
                    _DeliveryResult(
                        row.email,
                        row.display_name,
                        "skipped",
                        "send_usage_report_disabled",
                    )
                )

        footer = _build_delivery_footer(delivery_results, flagged=len(rows))
        digest_with_footer = digest + "\n\n" + footer
        if usage_report_eligible:
            report_counts = _delivery_counts(report_results)
            report_footer = (
                "--- Usage Report Summary ---\n"
                f"eligible={len(report_recipients)}"
                f"  dm_sent={report_counts['dm_sent']}"
                f"  email_sent={report_counts['email_sent']}"
                f"  skipped={report_counts['skipped']}"
                f"  failed={report_counts['failed']}"
            )
            report_failures = [r for r in report_results if r.status == "failed"]
            if report_failures:
                report_footer += "\nFailures:"
                for r in report_failures:
                    report_footer += f"\n  - {r.display_name} ({r.email}): {r.detail}"
            digest_with_footer += "\n\n" + report_footer

        channel_res = slack_client.post_channel_file(
            ncfg.slack.channel,
            digest_with_footer,
            title=f"GPU Underusage Digest — {period}",
        )
        if channel_res.status != SendStatus.OK:
            logger.error(
                "Failed to post admin digest to %s: %s",
                ncfg.slack.channel,
                channel_res.detail,
            )

        counts = _delivery_counts(delivery_results)
        report_counts = _delivery_counts(report_results)
        logger.info(
            "Underusage notification run: flagged=%d dm_sent=%d email_sent=%d skipped=%d failed=%d"
            " | report_eligible=%s report_sent=%d report_email=%d report_skipped=%d report_failed=%d",
            len(rows),
            counts["dm_sent"],
            counts["email_sent"],
            counts["skipped"],
            counts["failed"],
            usage_report_eligible,
            report_counts["dm_sent"],
            report_counts["email_sent"],
            report_counts["skipped"],
            report_counts["failed"],
        )

        print()
        print("=== Send Complete ===")
        print(footer)
        if usage_report_eligible:
            print()
            print(report_footer)

        return 0
