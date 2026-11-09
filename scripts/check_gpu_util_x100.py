"""List, cluster by cluster, the nodes still sending the slurm-job-exporter 0.5.0 x100.

In DCGM mode, 0.5.0 publishes the GPU utilization x100 (see
sarc/scraping/gpu_util_x100): once a job uses its GPU at 2 % or more, its node
sends values above 100. Two range queries per cluster, one point per UTC day,
tell for each node that sent any GPU utilization:
- the days it sent a value above 100: on 0.5.0 that day;
- the days it sent a value 0.5.0 cannot send: not on 0.5.0 that day.
A job series holding a DCGM BLANK sentinel is ignored for the day.

A node that sent nothing (down, or without GPU jobs) is not listed: it cannot
leave ambiguous jobs either.

    SARC_CONFIG=<file> uv run python scripts/check_gpu_util_x100.py --days 14
"""

import argparse
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta

from prometheus_api_client.prometheus_connect import PrometheusConnect

from sarc.config import config

DAY = timedelta(days=1)
SECOND = timedelta(seconds=1)

# A job series' highest value of the day, unless it is a DCGM BLANK sentinel:
# 0.5.0 sends at most 10000 (100 % x100). Only exporters before 0.4.13 send
# sentinels, and never x100 values, so dropping their series hides no 0.5.0.
REAL_SERIES_MAX = "max_over_time(slurm_job_utilization_gpu[1d]) <= 10000"

# Each node's highest value of the day.
MAX_QUERY = f"max by (instance) ({REAL_SERIES_MAX})"
# Each node's job series whose sum over the day is not a multiple of 100: 0.5.0
# sends multiples of 100 only, so such a series holds a value it cannot send.
# A sum can still land on a multiple of 100 (37 + 63): the series then proves
# nothing that day, but never proves wrong.
NOT_X100_QUERY = (
    "count by (instance) ("
    f"(sum_over_time(slurm_job_utilization_gpu[1d]) % 100 > 0) and ({REAL_SERIES_MAX})"
    ")"
)

STILL = "still on 0.5.0 (above 100 on its last day of data)"
NO_PROOF_OF_LEAVING = "no proof of leaving 0.5.0 since its last value above 100"
LEFT = "left 0.5.0 within the period"
UNDECIDED = "undecided (no proof either way)"
NEVER = "no 0.5.0 seen"


@dataclass
class NodeDays:
    """The UTC days a node sent any value, a value above 100, a value 0.5.0 cannot send."""

    data: set[date] = field(default_factory=set)
    x100: set[date] = field(default_factory=set)
    not_x100: set[date] = field(default_factory=set)


def query_days(
    prom: PrometheusConnect, query: str, first_day: date, last_day: date, timeout: int
) -> dict[str, dict[date, float]]:
    """Evaluate `query` once per UTC day, its [1d] window being that day."""
    results = prom.custom_query_range(
        query,
        start_time=_day_end(first_day),
        end_time=_day_end(last_day),
        step="1d",
        timeout=timeout,
    )
    by_instance: dict[str, dict[date, float]] = {}
    for result in results:
        instance = result["metric"].get("instance", "")
        by_instance[instance] = {
            _window_day(float(t)): float(v) for t, v in result["values"]
        }
    return by_instance


def _day_end(day: date) -> datetime:
    """The last second of `day`, UTC; for today, a time still to come.

    Not midnight: a [1d] window ending there also takes the sample scraped at
    midnight sharp, which belongs to the next day.
    """
    return datetime(day.year, day.month, day.day, tzinfo=UTC) + DAY - SECOND


def _window_day(timestamp: float) -> date:
    return datetime.fromtimestamp(timestamp, tz=UTC).date()


def check_cluster(
    prom: PrometheusConnect, first_day: date, last_day: date, timeout: int
) -> tuple[dict[str, NodeDays], float, float]:
    """Each node's days, and the seconds each query took."""
    start = time.monotonic()
    maxima = query_days(prom, MAX_QUERY, first_day, last_day, timeout)
    max_seconds = time.monotonic() - start
    start = time.monotonic()
    not_x100 = query_days(prom, NOT_X100_QUERY, first_day, last_day, timeout)
    not_x100_seconds = time.monotonic() - start

    nodes: dict[str, NodeDays] = {}
    for instance, days in maxima.items():
        nodes[instance] = NodeDays(
            data=set(days),
            x100={day for day, value in days.items() if value > 100},
            not_x100=set(not_x100.get(instance, {})),
        )
    return nodes, max_seconds, not_x100_seconds


def verdict(node: NodeDays) -> tuple[str, str]:
    """The node's category, and the dates to print next to it."""
    if not node.x100:
        if node.not_x100:
            return NEVER, ""
        return UNDECIDED, f"last data on {max(node.data)}"

    last_x100 = max(node.x100)
    last_data = max(node.data)
    later_proofs = [day for day in node.not_x100 if day >= last_x100]
    if later_proofs:
        switch_day = min(later_proofs)
        if switch_day == last_x100:
            return LEFT, f"switched on {switch_day}"
        return LEFT, f"switched between {last_x100} and {switch_day}"
    if last_x100 == last_data:
        return STILL, f"last data on {last_data}"
    return (
        NO_PROOF_OF_LEAVING,
        f"last above 100 on {last_x100}, last data on {last_data}",
    )


def print_report(
    name: str, nodes: dict[str, NodeDays], first_day: date, last_day: date
) -> None:
    by_category: dict[str, list[str]] = defaultdict(list)
    for instance in sorted(nodes):
        category, detail = verdict(nodes[instance])
        by_category[category].append(f"{instance}  {detail}")

    print(
        f"{len(nodes)} nodes sent GPU utilization from {first_day} to {last_day} "
        f"(UTC days, today partial)"
    )
    for category in (STILL, NO_PROOF_OF_LEAVING, LEFT, UNDECIDED, NEVER):
        lines = by_category[category]
        print(f"  {category}: {len(lines)}")
        if category != NEVER:
            for line in lines:
                print(f"    {line}")

    x100_days = [day for node in nodes.values() for day in node.x100]
    if x100_days:
        print(f"Last value above 100 on {name}: {max(x100_days)}")
    else:
        print(f"No value above 100 on {name} over the period")


def main():
    parser = argparse.ArgumentParser(
        description="List the nodes still sending the slurm-job-exporter 0.5.0 x100."
    )
    parser.add_argument(
        "--days", type=int, default=14, help="UTC days to check, today included."
    )
    parser.add_argument(
        "--cluster",
        action="append",
        help="Cluster to check (repeatable). Default: every cluster with a Prometheus.",
    )
    parser.add_argument(
        "--timeout", type=int, default=300, help="HTTP timeout per query, in seconds."
    )
    args = parser.parse_args()
    if args.days < 1:
        parser.error("--days must be at least 1")
    unknown = [name for name in args.cluster or [] if name not in config.clusters]
    if unknown:
        parser.error(f"unknown clusters: {', '.join(unknown)}")

    names = args.cluster or [
        name for name, cluster in config.clusters.items() if cluster.prometheus_url
    ]
    last_day = datetime.now(UTC).date()
    first_day = last_day - (args.days - 1) * DAY
    for name in names:
        print(f"== {name} ==", flush=True)
        try:
            nodes, max_seconds, not_x100_seconds = check_cluster(
                config.clusters[name].prometheus, first_day, last_day, args.timeout
            )
        except Exception as exc:
            print(f"failed: {exc!r}\n")
            continue
        print(f"queries: {max_seconds:.1f} s + {not_x100_seconds:.1f} s")
        print_report(name, nodes, first_day, last_day)
        print()


if __name__ == "__main__":
    main()
