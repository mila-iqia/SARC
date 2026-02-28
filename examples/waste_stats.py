"""Simple example that shows the compute usage and waste statistics per user over a given period."""

import dataclasses
import datetime
import logging
import os
import tempfile
from pathlib import Path

import gifnoc
import pandas as pd
import rich
import rich.logging
import rich.pretty
import simple_parsing

from examples.utils import (
    Filter,
    cache_results_to_file,
    midnight,
    setup_sarc_access,
)
from sarc.client.series import (
    compute_cost_and_waste,
    load_job_series,
    update_job_series_rgu,
)

logger = logging.getLogger(__name__)

SARC_CLIENT_CONFIG = Path(__file__).parent.parent / "config" / "sarc-client.yaml"
gifnoc.set_sources(SARC_CLIENT_CONFIG)

DEFAULT_CACHE_DIR = Path(
    os.environ.get("SARC_CACHE", os.environ.get("SCRATCH", tempfile.gettempdir()))
)


@dataclasses.dataclass(frozen=True)
class Options:
    """Command-line options for this example script."""

    filter: Filter = Filter(
        start=midnight(datetime.datetime.now() - datetime.timedelta(days=7)),
        end=midnight(datetime.datetime.now()),
        clusters=(),  # all clusters
        user=(),  # all users
    )

    cache_dir: Path = dataclasses.field(default=DEFAULT_CACHE_DIR)
    """ Directory where temporary files will be stored."""

    verbose: int = simple_parsing.field(
        alias=["-v", "--verbose"], action="count", default=0
    )

    csv: Path | None = None
    """Path to output CSV file."""

    topk: int | None = None
    """Number of top compute users to display."""


def main():
    args = simple_parsing.parse(Options, description=__doc__)
    filter: Filter = args.filter

    _setup_logging(args.verbose)
    setup_sarc_access()

    logger.debug(f"Using filtering options: {filter}")
    df = get_sarc_data(filter, cache_dir=args.cache_dir)

    compute_usage_df = get_compute_usage_df(df, filter)

    if args.csv:
        compute_usage_df.to_csv(args.csv)
        rich.print(f"Wrote compute usage stats to CSV file at [bold]{args.csv}[/bold]")
    else:
        rich.print("Hint: Use --csv to output to a CSV file.")

    if args.topk:
        rich.pretty.pprint(
            compute_usage_df.nlargest(args.topk, "rgu_years")
            # .sort_values(ascending=False, by="rgu_years")
            # .head(20)
        )
    else:
        rich.pretty.pprint(compute_usage_df)
    # # Print from worst offender to best usage.
    # print(ratios.reset_index().sort_values(ascending=False, by="total_ratio"))


def get_sarc_data(args: Filter, cache_dir: Path) -> pd.DataFrame:
    """Retrieve the SARC data for that period."""
    df = cache_results_to_file(cache_dir)(load_job_series)(
        start=args.start,
        end=args.end,
        cluster=args.clusters if args.clusters else None,
        user=_users if (_users := args.get_users()) else None,
    )
    # Add RGU information to jobs.
    df = update_job_series_rgu(df)
    # Compute the total amount of compute time used, wasted and overbilled by user.
    df = compute_cost_and_waste(df)

    # For now we need to add the cost/waste stats in RGUs manually:
    rgu_to_gpu = df["allocated.gres_rgu"] / df["allocated.gres_gpu"]
    df["rgu_cost"] = rgu_to_gpu * df["gpu_cost"]
    df["rgu_waste"] = rgu_to_gpu * df["gpu_waste"]
    df["rgu_equivalent_cost"] = rgu_to_gpu * df["gpu_equivalent_cost"]
    df["rgu_equivalent_waste"] = rgu_to_gpu * df["gpu_equivalent_waste"]
    df["rgu_overbilling_cost"] = rgu_to_gpu * df["gpu_overbilling_cost"]

    return df


def get_compute_usage_df(df: pd.DataFrame, _args: Filter) -> pd.DataFrame:
    """Compute the compute usage statistics per user."""
    # Group jobs by user
    grouped_df = df.groupby(["cluster_name", "user"])
    compute_stats = (
        # At first, select only the columns that have to do with costs.
        grouped_df[
            [
                f"{compute_type}_equivalent_{cost_or_waste}"
                for compute_type in ["cpu", "gpu", "rgu"]
                for cost_or_waste in ["cost", "waste"]
            ]
        ]
        # convert the cost/waste values from seconds to cpu/gpu/rgu years.
        .sum()
        .div(datetime.timedelta(days=365.25).total_seconds())
        .assign(
            **{
                f"{compute_type}_waste_pct": lambda v: (
                    v[f"{compute_type}_equivalent_waste"]
                    / v[f"{compute_type}_equivalent_cost"]
                )
                for compute_type in ["cpu", "gpu", "rgu"]
            }
        )
        .drop(
            # Drop the waste columns. Keep only the percentage of waste from above, easier to read.
            columns=[
                f"{compute_type}_equivalent_waste"
                for compute_type in ["cpu", "gpu", "rgu"]
            ]
        )
        .rename(
            columns={  # rename to just cpu_years, gpu_years, rgu_years.
                f"{compute_type}_equivalent_cost": f"{compute_type}_years"
                for compute_type in ["cpu", "gpu", "rgu"]
            }
        )
    )
    other_stats = grouped_df.aggregate(
        total_jobs=pd.NamedAgg(column="job_id", aggfunc="nunique"),
        clusters_used=pd.NamedAgg(column="cluster_name", aggfunc="nunique"),
        avg_job_length=pd.NamedAgg(
            column="elapsed_time",
            aggfunc=lambda v: pd.to_timedelta(v.mean().round(), unit="seconds"),
        ),
    )
    return pd.merge(
        compute_stats,
        other_stats,
        left_index=True,
        right_index=True,
    )


def _setup_logging(verbose: int):
    logging.basicConfig(
        handlers=[rich.logging.RichHandler(show_time=False)],
        format="%(message)s",
        level=logging.WARNING,
        force=True,
    )
    logging.getLogger("sarc").setLevel(logging.WARNING)
    this_logger = logger

    if verbose == 0:
        this_logger.setLevel("WARNING")
    elif verbose == 1:
        this_logger.setLevel("INFO")
    else:
        this_logger.setLevel("DEBUG")


if __name__ == "__main__":
    main()
