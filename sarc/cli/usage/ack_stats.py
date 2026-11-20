"""`sarc usage ack-stats`: acknowledgement rate per notification cycle (SARC-802)."""

from dataclasses import dataclass

import simple_parsing

from sarc.cli.usage.notify import _userfacing_print
from sarc.notifications.delivery import ack_rate_by_cycle

_KINDS = ("underusage", "usage")


@dataclass
class UsageAckStatsCommand:
    """Show the acknowledgement rate of usage notification DMs per cycle."""

    kind: str | None = simple_parsing.field(
        default=None,
        help='Restrict to one notification kind ("underusage" or "usage"). '
        "Default: all kinds.",
    )
    cycles: int = simple_parsing.field(
        default=12, help="Show at most this many (cycle, kind) rows, most recent first."
    )

    def execute(self) -> int:
        if self.kind is not None and self.kind not in _KINDS:
            _userfacing_print(f"Unknown kind {self.kind!r}: expected one of {_KINDS}")
            return -1
        stats = ack_rate_by_cycle(kind=self.kind, limit=self.cycles)
        if not stats:
            _userfacing_print("No tracked notification deliveries yet.")
            return 0
        _userfacing_print(
            f"{'window_end':<10}  {'kind':<11}  {'sent':>4}  {'ack':>4}  {'rate':>5}"
        )
        for s in stats:
            _userfacing_print(
                f"{s.window_end:%Y-%m-%d}  {s.kind:<11}  "
                f"{s.sent:>4}  {s.acknowledged:>4}  {s.rate:>5.0%}"
            )
        return 0
