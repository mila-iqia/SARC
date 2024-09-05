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
from typing import Any, Iterable, TypeVar

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

from sarc.config import MTL
from sarc.jobs.job import (
    Query,
    _compute_jobs_query,
    jobs_collection,
)

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
    ).astimezone(tz=MTL) - timedelta(days=30)

    end_date: datetime = (
        datetime.today()
        .replace(hour=0, minute=0, second=0, microsecond=0)
        .astimezone(tz=MTL)
    )

    verbose: int = simple_parsing.field(
        alias="-v", action="count", default=0, hash=False
    )


@dataclass(frozen=True, unsafe_hash=True)
class Period:
    start_date: datetime = datetime.today().replace(
        hour=0, minute=0, second=0, microsecond=0
    ).astimezone(tz=MTL) - timedelta(days=30)
    end_date: datetime = (
        datetime.today()
        .replace(hour=0, minute=0, second=0, microsecond=0)
        .astimezone(tz=MTL)
    )


def main():
    parser = simple_parsing.ArgumentParser(description="Analyze the milatools usage.")
    parser.add_arguments(Args, dest="args")
    args: Args = parser.parse_args().args
    if isinstance(args.start_date, str):
        args = dataclasses.replace(
            args, start_date=datetime.fromisoformat(args.start_date).astimezone(tz=MTL)
        )
    if isinstance(args.end_date, str):
        args = dataclasses.replace(
            args, end_date=datetime.fromisoformat(args.end_date).astimezone(tz=MTL)
        )
    print("Args:")
    pprint.pprint(dataclasses.asdict(args))

    _setup_logging(args.verbose)

    assert isinstance(args.start_date, datetime)
    assert isinstance(args.end_date, datetime)
    period = Period(args.start_date, args.end_date)

    all_clusters = _get_all_clusters(args.start_date, args.end_date)
    logger.info(f"All clusters: {all_clusters}")

    figures: list[Path] = []

    figures += make_milatools_usage_plots(period, cluster="mila", fig_suffix="mila")
    figures += make_milatools_usage_plots(
        period, cluster=sorted(set(all_clusters) - {"mila"}), fig_suffix="drac"
    )
    figures += make_milatools_usage_plots(period, cluster=None, fig_suffix="all")
    # figures = make_usage_plots(args, job_name="mila-code")
    # figures += make_usage_plots(args, job_name="mila-cpu")

    # upload_figures_to_google_drive(figures)


def _get_cache_dir():
    return Path(os.environ.get("SCRATCH", tempfile.gettempdir()))


def _get_all_clusters(start_date: datetime, end_date: datetime):
    cache_dir = _get_cache_dir()
    job_db: pymongo.collection.Collection = jobs_collection().get_collection()

    if (
        all_clusters_file := cache_dir / f"all_clusters_{start_date}_{end_date}.pkl"
    ).exists():
        with all_clusters_file.open("rb") as f:
            all_clusters = pickle.load(f)
        assert _is_iterable_of(all_clusters, str) and isinstance(all_clusters, list)
    else:
        _period_filter = _compute_jobs_query(start=start_date, end=end_date)
        all_clusters: list[str] = list(
            job_db.distinct("cluster_name", filter=_period_filter)
        )
        with all_clusters_file.open("wb") as f:
            pickle.dump(all_clusters, f)
    return sorted(all_clusters)


def make_milatools_usage_plots(
    period: Period, cluster: str | list[str] | None, fig_suffix: str
) -> list[Path]:
    if cluster is None:
        cluster_suffix = " on all slurm clusters"
    elif isinstance(cluster, str):
        cluster_suffix = f" on the {cluster} cluster"
    else:
        cluster_suffix = f" on the {cluster} clusters"

    df = _get_milatools_usage_data(period, cluster=cluster)
    df["using_milatools"] = (
        df["milatools_users_this_period"] / df["cluster_users_this_period"]
    )
    df["used_milatools_before"] = (
        df["users_this_period_that_used_milatools_before"]
        / df["cluster_users_this_period"]
    )
    # print(df)

    # daily_counts = df.resample(rule="D").size()
    axes: list[matplotlib.axes.Axes]
    fig, axes = plt.subplots(sharex=True, ncols=2, nrows=1)
    fig.suptitle(f"Statistics on the use of Milatools{cluster_suffix}")
    (ax1, ax2) = axes
    ax1.set_title("Percentage of users using milatools")
    ax1.set_ylim(0, 1)
    ax1.annotate(
        f'{df["using_milatools"].iloc[-1]:.2%}',
        (df.index[-1], df["using_milatools"].iloc[-1]),
    )
    ax1.annotate(
        f'{df["used_milatools_before"].iloc[-1]:.2%}',
        (df.index[-1], df["used_milatools_before"].iloc[-1]),
    )

    ax2.set_title("Number of users using milatools")

    df[["using_milatools", "used_milatools_before"]].plot(
        kind="line", ax=ax1, legend=True, ylabel="Percentage of users using milatools"
    )
    df[["milatools_users_this_period", "cluster_users_this_period"]].plot(
        kind="area",
        ax=ax2,
        legend=True,
        # label=["Using milatools", "Not using milatools"],
    )
    # Set x-ticks and labels

    assert isinstance(df.index, DatetimeIndex)
    ax1.set_xticks(df.index)  # Set all possible x-tick positions
    ax2.set_xticks(df.index)  # Set all possible x-tick positions
    # Apply all labels with rotation
    label_every_week = df.index.strftime("%Y-%m-%d")
    label_every_month = [
        label_every_week[i] if i % 4 == 0 else "" for i in range(len(label_every_week))
    ]
    ax1.set_xticklabels(label_every_month, rotation=45)
    ax2.set_xticklabels(label_every_month, rotation=45)

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


def _get_milatools_usage_data(args: Period, cluster: str | list[str] | None):
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

    interval = timedelta(days=7)

    date_range = pd.date_range(
        args.start_date, args.end_date, freq=interval, inclusive="both"
    )
    for interval_start, interval_end in zip(
        date_range.to_list()[:-1], date_range.to_list()[1:]
    ):
        milatools_users_that_period, cluster_users_that_period = _get_unique_users(
            interval_start, interval_end, cluster=cluster
        )
        if not cluster_users_that_period:
            raise RuntimeError(
                f"No users of the {cluster + ' ' if cluster else ''}cluster in the period from {interval_start} to {interval_end}??"
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
            "milatools_users_this_period": num_milatools_users_each_period,
            "cluster_users_this_period": num_cluster_users_each_period,
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


def _get_unique_users(
    start_date: datetime,
    end_date: datetime,
    cluster: str | list[str] | Query | None = None,
) -> tuple[set[str], set[str]]:
    milatools_job_names: Query = {"$in": ["mila-code", "mila-cpu"]}
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

    job_structured_db = jobs_collection()
    job_db: pymongo.collection.Collection = job_structured_db.get_collection()

    _filter = _compute_jobs_query(start=start_date, end=end_date, cluster=cluster)
    all_users: set[str] = set(job_db.distinct("user", filter=_filter))

    cluster_suffix = f" on the {cluster} cluster" if cluster else ""
    _filter = _compute_jobs_query(
        start=start_date,
        end=end_date,
        name=milatools_job_names,
        cluster=cluster,
    )
    milatools_users: set[str] = set(job_db.distinct("user", filter=_filter))
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
