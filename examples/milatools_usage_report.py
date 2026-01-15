"""Analyze the milatools usage based on the number of jobs called 'mila-{command}'."""

# /// script
# requires-python = ">=3.9"
# dependencies = [
#     "matplotlib",
#     "simple-parsing",
#     "pandas",
#     "sarc>=0.1.0",
#     "pydantic",
#     "tzlocal",
# ]
#
# [tool.uv.sources]
# sarc = { git = "https://github.com/mila-iqia/SARC.git" }
# ///
from __future__ import annotations

import dataclasses
import logging
import os
import pickle
import pprint
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta
from logging import getLogger as get_logger
from pathlib import Path
from typing import Any, Iterable, TypedDict, TypeVar, Union

import matplotlib
import matplotlib.axes
import matplotlib.figure
import matplotlib.pyplot as plt
import pandas as pd
import pymongo
import pymongo.collection
import simple_parsing
from pandas.core.indexes.datetimes import DatetimeIndex
from typing_extensions import TypeGuard

from sarc.client.job import _jobs_collection
from sarc.config import UTC

logger = get_logger(__name__)

# Remember to set up the port forwarding if you want
# to access data from SARC.
#    ssh -L 27017:localhost:27017 sarc
# (or using the LocalForward option in your ~/.ssh/config file)

# Change this to the path to your config file.

if "SARC_CONFIG" not in os.environ:
    # TODO: Probably need to remove this, but idk how to make it work without it..
    sarc_config_file = Path(__file__).parent / "milatools-sarc-client.json"
    if sarc_config_file.exists():
        os.environ["SARC_CONFIG"] = str(sarc_config_file)


@dataclass(frozen=True, unsafe_hash=True)
class Args:
    start_date: datetime | str = datetime.today().replace(
        hour=0, minute=0, second=0, microsecond=0
    ).astimezone(UTC) - timedelta(days=30)

    end_date: datetime | str = (
        datetime.today()
        .replace(hour=0, minute=0, second=0, microsecond=0)
        .astimezone(UTC)
    )

    verbose: int = simple_parsing.field(
        alias="-v", action="count", default=0, hash=False
    )


@dataclass(frozen=True, unsafe_hash=True)
class Period:
    start_date: datetime
    end_date: datetime


def main():
    parser = simple_parsing.ArgumentParser(description="Analyze the milatools usage.")
    parser.add_arguments(Args, dest="args")
    args: Args = parser.parse_args().args
    start_date = args.start_date
    if isinstance(start_date, str):
        start_date = datetime.fromisoformat(start_date).astimezone(tz=UTC)
    end_date = args.end_date
    if isinstance(end_date, str):
        end_date = datetime.fromisoformat(end_date).astimezone(tz=UTC)

    print("Args:")
    pprint.pprint(dataclasses.asdict(args))

    _setup_logging(args.verbose)

    period = Period(start_date, end_date)

    all_clusters = _get_all_clusters(period.start_date, period.end_date)
    logger.info(f"All clusters: {all_clusters}")

    figures: list[Path] = []

    sampling_interval = timedelta(weeks=1)
    figures += make_milatools_usage_plots(
        period, cluster="mila", fig_suffix="mila", sampling_interval=sampling_interval
    )
    figures += make_milatools_usage_plots(
        period,
        cluster=sorted(set(all_clusters) - {"mila"}),
        fig_suffix="drac",
        sampling_interval=sampling_interval,
    )
    figures += make_milatools_usage_plots(
        period, cluster=None, fig_suffix="all", sampling_interval=sampling_interval
    )
    # figures = make_usage_plots(args, job_name="mila-code")
    # figures += make_usage_plots(args, job_name="mila-cpu")

    # upload_figures_to_google_drive(figures)


def _get_cache_dir():
    return Path(os.environ.get("SCRATCH", tempfile.gettempdir()))


def _get_all_clusters(start_date: datetime, end_date: datetime):
    cache_dir = _get_cache_dir()
    job_db: pymongo.collection.Collection = _jobs_collection().get_collection()

    if (
        all_clusters_file := cache_dir / f"all_clusters_{start_date}_{end_date}.pkl"
    ).exists():
        with all_clusters_file.open("rb") as f:
            all_clusters = pickle.load(f)
        assert _is_iterable_of(all_clusters, str) and isinstance(all_clusters, list)
    else:
        _period_filter = _get_filter(
            start_date=start_date, end_date=end_date, cluster_name=None, name=None
        )
        all_clusters: list[str] = list(
            job_db.distinct("cluster_name", filter=_period_filter)
        )
        with all_clusters_file.open("wb") as f:
            pickle.dump(all_clusters, f)
    return sorted(all_clusters)


def make_milatools_usage_plots(
    period: Period,
    cluster: str | list[str] | None,
    fig_suffix: str,
    sampling_interval: timedelta = timedelta(days=7),
) -> list[Path]:
    if cluster is None:
        cluster_suffix = " on all slurm clusters"
    elif isinstance(cluster, str):
        cluster_suffix = f" on the {cluster} cluster"
    else:
        cluster_suffix = f" on the {cluster} clusters"

    df = _get_milatools_usage_data(
        period, cluster=cluster, sampling_interval=sampling_interval
    )
    df["using_milatools"] = df["milatools_users"] / df["cluster_users"]
    df["used_milatools_before"] = (
        df["users_this_period_that_used_milatools_before"] / df["cluster_users"]
    )
    # print(df)

    # daily_counts = df.resample(rule="D").size()
    axes: list[matplotlib.axes.Axes]
    fig, axes = plt.subplots(sharex=True, sharey=False, ncols=2, nrows=1)
    fig.suptitle(f"Statistics on the use of Milatools{cluster_suffix}")
    (ax1, ax2) = axes
    ax1.set_title("Adoption")
    ax1.set_ylim(0, 1)
    ax2.set_title(f"Users {cluster_suffix}")
    df["not using milatools"] = 1 - df["using_milatools"]
    df["never used milatools"] = 1 - df["used_milatools_before"]
    df[
        [
            "using_milatools",
            "used_milatools_before",
            # "not using milatools",
            # "never used milatools",
        ]
    ].plot(
        kind="line",
        ax=ax1,
        legend=True,
        xlabel="Date",
        ylabel="Percentage of users using milatools",
        linewidth=2.5,
        color=["green", "blue"],  # "lightgray", "gray"],
    )
    _annotate_start_and_end(df, ax1, "using_milatools", percentage=True)
    _annotate_start_and_end(df, ax1, "used_milatools_before", percentage=True)

    # In a stacked area plot, the second column is stacked on top of the first
    df["cluster users"] = df["cluster_users"] - df["milatools_users"]
    df[["milatools_users", "cluster users"]].plot(
        kind="area",
        ax=ax2,
        stacked=True,
        legend=True,
        linewidth=2.5,
        color=["green", "silver"],
    )
    _annotate_start_and_end(df, ax2, "milatools_users", percentage=False)
    # need to annotate using 'cluster_users' (before the subtraction)
    _annotate_start_and_end(df, ax2, "cluster_users", percentage=False)

    ax1.set_yticklabels([f"{x:.0%}" for x in ax1.get_yticks()])

    # Make all labels gray, then select the added ones and make them darker.
    ax1.set_yticklabels(ax1.get_yticklabels(), color="dimgray")
    ax1.get_yticklabels()[-2].set_color("black")
    ax1.get_yticklabels()[-1].set_color("black")

    ax2.set_yticklabels(ax2.get_yticklabels(), color="dimgray")
    ax2.get_yticklabels()[-2].set_color("black")
    ax2.get_yticklabels()[-1].set_color("black")

    # Set x-ticks and labels
    assert isinstance(df.index, DatetimeIndex)
    ax1.set_xticks(df.index)  # Set all possible x-tick positions
    ax2.set_xticks(df.index)  # Set all possible x-tick positions

    # Apply all labels with rotation
    label_every_week = df.index.strftime("%Y-%m-%d")
    if sampling_interval == timedelta(days=7):
        # one label every month
        ticks = [
            label_every_week[i] if i % 4 == 0 else ""
            for i in range(len(label_every_week))
        ]
    else:
        # one label every month
        ticks = label_every_week
    ax1.set_xticklabels(ticks, rotation=45)
    ax2.set_xticklabels(ticks, rotation=45)

    fig.tight_layout()
    # fig.layout
    fig_path = Path(
        f"milatools_usage_{period.start_date.date()}_{period.end_date.date()}_{fig_suffix}.png"
    )
    # plt.show()
    fig.set_size_inches(12, 6)
    fig.savefig(fig_path)
    print(f"Figure saved at {fig_path}")
    return [fig_path]


# Annotate the start and end values for the plots
def _annotate_start_and_end(
    df: pd.DataFrame, ax: matplotlib.axes.Axes, row: str, percentage: bool
):
    def _format(v):
        return f"{v:.0%}" if percentage else f"{v}"

    ax.set_yticks(list(ax.get_yticks()) + [df[row].iloc[0]])

    # ax.annotate(
    #     text=_format(df[row].iloc[0]),
    #     xy=(df.index[0], df[row].iloc[0]),
    #     xycoords="data",
    #     xytext=(-45, -15),
    #     textcoords="offset points",
    #     # add color maybe?
    #     # color="blue",
    #     # arrowprops=dict(arrowstyle="->", color="black"),
    #     fontsize="12",
    # )
    ax.annotate(
        _format(df[row].iloc[-1]),
        (df.index[-1], df[row].iloc[-1]),
        fontsize="12",
    )


def _get_milatools_usage_data(
    args: Period, cluster: str | list[str] | None, sampling_interval: timedelta
):
    cluster_suffix = f" on the {cluster} cluster" if cluster else ""
    logger.info(
        f"Getting milatools usage data from {args.start_date} to {args.end_date}{cluster_suffix}"
    )

    milatools_users_so_far: set[str] = set()
    cluster_users_so_far: set[str] = set()

    num_milatools_users_each_period: list[int] = []
    num_cluster_users_each_period: list[int] = []

    num_milatools_users_so_far: list[int] = []
    num_cluster_users_so_far: list[int] = []

    num_users_this_period_that_have_used_milatools_before: list[int] = []

    date_range = pd.date_range(
        args.start_date, args.end_date, freq=sampling_interval, inclusive="both"
    )
    for interval_start, interval_end in zip(
        date_range.to_list()[:-1], date_range.to_list()[1:]
    ):
        milatools_users_that_period, cluster_users_that_period = _get_unique_users(
            interval_start, interval_end, cluster=cluster
        )
        if not cluster_users_that_period:
            raise RuntimeError(
                f"No users of the {cluster=} cluster in the period from {interval_start} to {interval_end}??"
            )

        cluster_users_so_far.update(cluster_users_that_period)
        milatools_users_so_far.update(milatools_users_that_period)

        users_this_period_that_have_used_milatools_before: set[str] = set(
            user for user in cluster_users_that_period if user in milatools_users_so_far
        )

        # adoption_pct_overall = len(milatools_users_so_far) / len(cluster_users_so_far)
        # logger.info(f"Adoption percentage so far: {adoption_pct_overall:.2%}")

        num_milatools_users_each_period.append(len(milatools_users_that_period))
        num_cluster_users_each_period.append(len(cluster_users_that_period))
        num_milatools_users_so_far.append(len(milatools_users_so_far))
        num_cluster_users_so_far.append(len(cluster_users_so_far))
        num_users_this_period_that_have_used_milatools_before.append(
            len(users_this_period_that_have_used_milatools_before)
        )

    assert (
        len(date_range) - 1
        == len(num_milatools_users_each_period)
        == len(num_cluster_users_each_period)
        == len(num_milatools_users_so_far)
        == len(num_cluster_users_so_far)
        == len(num_users_this_period_that_have_used_milatools_before)
    ), (len(date_range), len(num_milatools_users_each_period))

    return pd.DataFrame(
        {
            "milatools_users": num_milatools_users_each_period,
            "cluster_users": num_cluster_users_each_period,
            "milatools_users_so_far": num_milatools_users_so_far,
            "cluster_users_so_far": num_cluster_users_so_far,
            "users_this_period_that_used_milatools_before": num_users_this_period_that_have_used_milatools_before,
        },
        index=date_range[:-1],
    )


def _setup_logging(verbose: int):
    import rich.logging

    logging.basicConfig(
        handlers=[rich.logging.RichHandler()],
        format="%(message)s",
        level=logging.ERROR,
    )

    match verbose:
        case 0:
            logger.setLevel("WARNING")
        case 1:
            logger.setLevel("INFO")
        case _:
            logger.setLevel("DEBUG")


InQuery = TypedDict("InQuery", {"$in": list[Any]})
OrQuery = TypedDict("OrQuery", {"$or": list[Any]})
RegexQuery = TypedDict("RegexQuery", {"$regex": list[str]})
Query = Union[InQuery, OrQuery, RegexQuery]


def _get_filter(
    start_date: datetime,
    end_date: datetime,
    cluster_name: str | Query | None,
    name: str | Query | None,
):
    query: dict = {
        "submit_time": {"$gte": start_date, "$lt": end_date},
    }
    if cluster_name is not None:
        query["cluster_name"] = cluster_name

    if name is not None:
        query["name"] = name

    # _filtre = {
    #     "$and": [
    #         {**_filtre, "submit_time": {"$gte": start}},
    #         {**_filtre, "submit_time": {"$lt": end}},
    #     ]
    # }
    return query


def _get_unique_users(
    start_date: datetime,
    end_date: datetime,
    cluster: str | list[str] | Query | None = None,
) -> tuple[set[str], set[str]]:
    milatools_job_names: list[str] = ["mila-code", "mila-cpu"]
    cluster_suffix = f" on the {cluster} cluster" if cluster else ""

    if isinstance(cluster, list):
        cluster = {"$in": cluster}

    cache_dir = Path(os.environ.get("SCRATCH", tempfile.gettempdir()))
    # _hash = hashlib.md5(f"{start_date}-{end_date}-{cluster}".encode()).hexdigest()
    cached_results_path = (
        Path(cache_dir)
        / f"milatools-unique_users-{cluster}-{start_date}-{end_date}.pkl"
    )

    if cached_results_path.exists():
        logger.debug(f"Reading data from {cached_results_path}")
        with cached_results_path.open("rb") as f:
            milatools_users, all_users = pickle.load(f)
        assert _is_iterable_of(milatools_users, str) and isinstance(
            milatools_users, set
        )
        assert _is_iterable_of(all_users, str) and isinstance(all_users, set)
        return milatools_users, all_users

    job_structured_db = _jobs_collection()
    job_db: pymongo.collection.Collection = job_structured_db.get_collection()

    _period_filter = _get_filter(start_date, end_date, cluster, name=None)
    _milatools_filter = _get_filter(
        start_date, end_date, cluster, name={"$in": milatools_job_names}
    )

    all_users: set[str] = set(job_db.distinct("user", filter=_period_filter))
    milatools_users: set[str] = set(job_db.distinct("user", filter=_milatools_filter))
    logger.debug(f"All users:\n{all_users}")
    logger.debug(f"Milatools users:\n{milatools_users}")

    n_milatools = len(milatools_users)
    n_total = len(all_users)
    logger.info(
        f"{n_milatools} out of {n_total} ({n_milatools / n_total:.2%}) of users used milatools between "
        f"{start_date.date()} and {end_date.date()}{cluster_suffix}."
    )

    assert milatools_users <= all_users

    cached_results_path.parent.mkdir(exist_ok=True)
    with cached_results_path.open("wb") as f:
        logger.debug(f"Saving data at {cached_results_path}")
        pickle.dump((milatools_users, all_users), f)

    return milatools_users, all_users


T = TypeVar("T")


def _is_iterable_of(v: Any, t: type[T]) -> TypeGuard[Iterable[T]]:
    try:
        return all(isinstance(v_i, t) for v_i in v)
    except TypeError:
        return False


if __name__ == "__main__":
    main()
