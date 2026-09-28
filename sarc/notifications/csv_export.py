import csv
import io
from datetime import date
from urllib.parse import urlencode

from sarc.notifications.usage import RecurringUserRow


def _dashboard_link(
    dashboard_url: str | None, email: str, start: date, end: date
) -> str:
    if dashboard_url is None:
        return ""
    query = urlencode(
        {
            "as_user": email,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "utm_source": "slack",
            "utm_medium": "notification",
            "utm_campaign": "admin_digest_csv",
            "utm_content": "dashboard",
        }
    )
    return f"{dashboard_url}?{query}"


def build_recurring_distilled_csv(
    recurring: dict[str, list[RecurringUserRow]],
    *,
    active_cycles: int,
    cycle_dates: list[date],
    dashboard_url: str | None,
    window_start: date,
    window_end: date,
) -> str:
    """Build a CSV close copy of the recurring-underusers Slack table: every
    user per cluster (no cluster_share_threshold truncation — that's already
    applied upstream by the caller's choice of *recurring*), one column per
    cycle in *cycle_dates* holding the same marker
    (``RecurringUserRow.cycle_symbol``) the Slack table renders, with a blank
    separator column after cycle index ``active_cycles - 1``.
    """
    if not recurring:
        return ""

    n_cycles = len(cycle_dates)
    buf = io.StringIO()
    writer = csv.writer(buf)

    header = ["cluster", "User", "user dashboard url", "Unused RGU-h", "Share"]
    for i, d in enumerate(cycle_dates):
        if i == active_cycles:
            header.append("")
        header.append(d.strftime("%m-%d"))
    writer.writerow(header)

    for cluster, rows in sorted(recurring.items()):
        for row in rows:
            data_row = [
                cluster,
                row.email,
                _dashboard_link(dashboard_url, row.email, window_start, window_end),
                row.wasted_current_active_window,
                row.cluster_share,
            ]
            for i in range(n_cycles):
                if i == active_cycles:
                    data_row.append("")
                data_row.append(row.cycle_symbol(i))
            writer.writerow(data_row)

    return buf.getvalue()
