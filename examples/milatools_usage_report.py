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

from dataclasses import dataclass
import dataclasses
import hashlib
import logging
import os
import pickle
import pprint
import tempfile
from datetime import datetime, timedelta
from logging import getLogger as get_logger
from pathlib import Path
from typing import Any, Iterable, Sequence, TypeVar

import matplotlib
import matplotlib.axes
import matplotlib.figure
import matplotlib.pyplot as plt
import pandas as pd
import pymongo
import pymongo.collection
from sarc.config import MTL
from sarc.jobs.job import Query, SlurmJob, get_jobs, jobs_collection
import simple_parsing
from typing_extensions import TypeGuard
from typing import Literal

from sarc.storage import mila

logger = get_logger(__name__)

# Remember to set up the port forwarding if you want
# to access data from SARC.
#    ssh -L 27017:localhost:27017 sarc
# (or using the LocalForward option in your ~/.ssh/config file)

# Change this to the path to your config file.

sarc_config_file = Path(__file__).parent / "milatools-sarc-client.json"
assert sarc_config_file.exists()
os.environ["SARC_CONFIG"] = str(sarc_config_file)


@dataclass(frozen=True, unsafe_hash=True)
class Args:
    start_date: datetime = datetime.today().replace(
        hour=0, minute=0, second=0, microsecond=0
    ).astimezone(tz=MTL) - timedelta(days=30)
    end_date: datetime = (
        datetime.today()
        .replace(hour=0, minute=0, second=0, microsecond=0)
        .astimezone(tz=MTL)
    )
    cluster: Literal["mila", "narval", "cedar", "beluga", "graham"] | None = None

    verbose: int = simple_parsing.field(
        alias="-v", action="count", default=0, hash=False
    )


def main():
    parser = simple_parsing.ArgumentParser(description="Analyze the milatools usage.")
    parser.add_arguments(Args, dest="args")
    args: Args = parser.parse_args().args

    print("Args:")
    pprint.pprint(dataclasses.asdict(args))

    _setup_logging(args.verbose)
    make_adoption_plots(args)
    # figures = make_usage_plots(args, job_name="mila-code")
    # figures += make_usage_plots(args, job_name="mila-cpu")

    # upload_figures_to_google_drive(figures)


def make_adoption_plots(args: Args):
    df = get_adoption_data(args)
    print(df)
    # daily_counts = df.resample(rule="D").size()

    fig, (ax1, ax2) = plt.subplots(sharex=True, ncols=2, nrows=2)
    ax1.set_title("Usage of milatools")
    ax2.set_title("Percentage of users using milatools")

    df["percentage"] = df["milatools_users"] / df["cluster_users"]
    df["percentage_so_far"] = df["milatools_users_so_far"] / df["cluster_users_so_far"]

    df[["percentage", "percentage_so_far"]].plot(kind="line", ax=ax1, legend=True)
    # df[["percentage_so_far"]].plot(kind="line", ax=ax, legend=True)
    # daily_counts = df.resample(rule="W-MON")

    # Calculate the percentage of milatools_users relative to cluster_users
    df[["milatools_users", "cluster_users"]].plot(kind="area", ax=ax2, legend=True)
    # df[["milatools_users_so_far", "cluster_users_so_far"]].plot(kind="area", ax=ax2, legend=True)
    # Set x-ticks and labels
    # ax.set_xticks(range(len(df.index)))  # Set all possible x-tick positions
    ax1.set_xticklabels(
        df.index.strftime("%Y-%m-%d"), rotation=90
    )  # Apply all labels with rotation

    fig.tight_layout()
    fig_path = Path("adoption.png")
    fig.savefig(fig_path)
    plt.show()
    return fig_path


def get_adoption_data(args: Args):
    logger.info(
        f"Getting milatools adoption data from {args.start_date} to {args.end_date}"
    )

    cache_dir = Path(os.environ.get("SCRATCH", tempfile.gettempdir()))
    # hash = hashlib.md5(f"{args.start_date}-{args.end_date}".encode()).hexdigest()
    logger.debug(f"Args {args} has hash {hash(args)}")
    cached_results_path = Path(cache_dir) / f"milatools-adoption-{hash(args)}.pkl"

    if cached_results_path.exists():
        logger.info(f"Reading data {cached_results_path}")
        return pd.read_pickle(cached_results_path)

    milatools_users_so_far: set[str] = set()
    cluster_users_so_far: set[str] = set()

    num_milatools_users_each_period: list[int] = []
    num_cluster_users_each_period: list[int] = []

    num_milatools_users_so_far: list[int] = []
    num_cluster_users_so_far: list[int] = []

    interval = timedelta(days=7)

    date_range = pd.date_range(
        args.start_date, args.end_date, freq=interval, inclusive="both"
    )
    for interval_start, interval_end in zip(
        date_range.to_list()[:-1], date_range.to_list()[1:]
    ):
        milatools_users_that_period, cluster_users_that_period = get_unique_users(
            interval_start, interval_end
        )
        if not cluster_users_that_period:
            logger.warning(
                f"No users of the cluster in the period from {interval_start} to {interval_end}?"
            )
            continue

        cluster_users_so_far.update(cluster_users_that_period)
        milatools_users_so_far.update(milatools_users_that_period)

        adoption_pct_that_week = len(milatools_users_that_period) / len(
            cluster_users_that_period
        )
        logger.info(f"Adoption percentage that period: {adoption_pct_that_week:.2%}")
        adoption_pct_overall = len(milatools_users_so_far) / len(cluster_users_so_far)
        logger.info(f"Adoption percentage so far: {adoption_pct_overall:.2%}")

        num_milatools_users_each_period.append(len(milatools_users_that_period))
        num_cluster_users_each_period.append(len(cluster_users_that_period))
        num_milatools_users_so_far.append(len(milatools_users_so_far))
        num_cluster_users_so_far.append(len(cluster_users_so_far))

    assert (
        len(date_range) - 1
        == len(num_milatools_users_each_period)
        == len(num_cluster_users_each_period)
        == len(num_milatools_users_so_far)
        == len(num_cluster_users_so_far)
    ), (len(date_range), len(num_milatools_users_each_period))

    df = pd.DataFrame(
        {
            "milatools_users": num_milatools_users_each_period,
            "cluster_users": num_cluster_users_each_period,
            "milatools_users_so_far": num_milatools_users_so_far,
            "cluster_users_so_far": num_cluster_users_so_far,
        },
        index=date_range[:-1],
    )
    logger.info(f"Saving data to {cached_results_path}")
    df.to_pickle(cached_results_path)
    return df


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


def get_unique_users(
    start_date: datetime, end_date: datetime, cluster: str | Query | None = None
) -> tuple[set[str], set[str]]:
    job_structured_db = jobs_collection()
    job_db: pymongo.collection.Collection = job_structured_db.get_collection()
    from sarc.jobs.job import _compute_jobs_query

    _filter = _compute_jobs_query(start=start_date, end=end_date)
    all_users: set[str] = set(job_db.distinct("user", filter=_filter))

    cluster_suffix = f"in cluster {cluster}" if cluster else ""

    logger.info(
        f"We have {len(all_users)} unique cluster users from {start_date.date()} to {end_date.date()}{cluster_suffix}."
    )

    _filter = _compute_jobs_query(
        start=start_date,
        end=end_date,
        name={"$in": ["mila-code", "mila-cpu"]},
        cluster=cluster,
    )
    milatools_users: set[str] = set(job_db.distinct("user", filter=_filter))
    logger.info(
        f"We have {len(milatools_users)} unique users of milatools from {start_date.date()} to {end_date.date()}{cluster_suffix}."
    )
    assert milatools_users <= all_users
    return milatools_users, all_users


def make_usage_plots(args: Args, job_name: str = "mila-code") -> list[Path]:
    date_start = args.start_date  # datetime(year=2024, month=1, day=1, tzinfo=MTL)
    date_end = args.end_date  # datetime(year=2025, month=1, day=1, tzinfo=MTL)

    unfiltered_jobs = retrieve_data(
        date_start,
        date_end,
        job_name=job_name,
        cache_dir=Path(os.environ.get("SCRATCH", tempfile.gettempdir())),
    )
    filtered_jobs = preprocess_and_filter_jobs(unfiltered_jobs, date_start, date_end)
    jobs = filtered_jobs

    # Not really necessary.
    milacode_jobs = [job for job in jobs if job.name == job_name]
    logger.info(f"We have {len(milacode_jobs)} {job_name} jobs.")

    n_jobs_over_30_minutes = len(
        list(job for job in milacode_jobs if job.duration.total_seconds() > 30 * 60)
    )
    print(n_jobs_over_30_minutes)
    print(f"We have {n_jobs_over_30_minutes} {job_name} jobs over 30 minutes.")

    milacode_jobs_over_10_minutes = [
        job for job in jobs if job.duration.total_seconds() >= 10 * 60
    ]
    print(
        f"We have {len(milacode_jobs_over_10_minutes)} {job_name} jobs over 10 minutes."
    )
    fig = plot_total_jobs_per_week(milacode_jobs_over_10_minutes, job_name=job_name)
    name = f"{job_name}-jobs-over-10-minutes.png"
    fig_path = Path.cwd() / name
    fig.savefig(fig_path)
    print(f"Saved figure at {fig_path}.")

    fig = plot_unique_users_each_week(jobs, job_name=job_name)
    name2 = f"unique-users-{job_name}.png"
    fig2_path = Path.cwd() / name2
    fig.savefig(fig2_path)
    print(f"Saved figure at {fig2_path}.")
    # print(list(job.duration.total_seconds() for job in L_milacode_jobs))
    # print(list(job.user for job in L_milacode_jobs))
    return [fig_path, fig2_path]


T = TypeVar("T")


def is_sequence_of(v: Any, t: type[T]) -> TypeGuard[Sequence[T]]:
    try:
        return all(isinstance(v_i, t) for v_i in v)
    except TypeError:
        return False


def retrieve_data(
    start: datetime,
    end: datetime,
    cache_dir: str | Path,
    job_name: str = "mila-code",
    cluster_name: str | None = None,
) -> list[SlurmJob]:
    hash = hashlib.md5(f"{start}-{end}-{job_name}-{cluster_name}".encode()).hexdigest()
    cached_results_path = Path(cache_dir) / f"milatools-jobs-{hash}.pkl"
    if cached_results_path.exists():
        print(f"Reading from {cached_results_path}.")
        with cached_results_path.open("rb") as f_input:
            unfiltered_jobs = pickle.load(f_input)
        assert is_sequence_of(unfiltered_jobs, SlurmJob)
        assert isinstance(unfiltered_jobs, list)
    else:
        print(f"Retrieving jobs from {start} to {end} with name {job_name!r}.")
        unfiltered_jobs = list(
            get_jobs(start=start, end=end, name=job_name, cluster=cluster_name)
        )
        print(f"Writing to {cached_results_path}.")
        with cached_results_path.open("wb") as f_output:
            pickle.dump(unfiltered_jobs, f_output, pickle.HIGHEST_PROTOCOL)
    return unfiltered_jobs


def fix_job_start_end_elapsed_time(job: SlurmJob) -> SlurmJob | None:
    """Fixes the start time, end time, and elapsed time of a job if they are not
    consistent with each other.

    This is a common issue with jobs coming from SARC.
    """

    if job.start_time is None:
        if job.end_time is None:
            logger.info(f"Job {job.job_id} has no start time or end time.")
            return None
        if job.elapsed_time == 0:
            logger.info(f"Job {job.job_id} has no start time or elapsed time.")
            return None
        job = job.replace(start_time=job.end_time - timedelta(seconds=job.elapsed_time))
        duration_seconds = job_duration_seconds(job)
        assert job.elapsed_time == duration_seconds
        return job

    if job.end_time is None:
        if job.elapsed_time == 0:
            logger.info(f"Job {job.job_id} has no end time or elapsed time.")
            return None
        end_time = job.start_time + timedelta(seconds=job.elapsed_time)
        job = job.replace(end_time=end_time)
        duration_seconds = job_duration_seconds(job)
        assert job.elapsed_time == duration_seconds
        return job

    duration_seconds = job_duration_seconds(job)
    if job.elapsed_time == 0:
        if duration_seconds != 0:
            logger.info(
                f"Job {job.job_id} has a no elapsed time but does have a duration? "
                f"{duration_seconds=} != {job.elapsed_time=}."
            )
            return None
        return job  # job has 0 duration and 0 calc time?

    return job


def job_duration_seconds(job: SlurmJob) -> int:
    assert job.start_time
    assert job.end_time
    return int(job.duration.total_seconds())


def clip_job_within_bounds(job: SlurmJob, start: datetime, end: datetime) -> SlurmJob:
    # Clip the job to the time range we are interested in.
    assert job.start_time and job.end_time
    if job.start_time < start:
        logger.info(
            f"Job {job.job_id}'s Start time is earlier than what we asked for: "
            f"{job.start_time} < {start}"
        )
        job = job.replace(start_time=start)
        job = job.replace(elapsed_time=job_duration_seconds(job))

    assert job.start_time and job.end_time

    if job.end_time > end:
        logger.info(
            f"Job {job.job_id}'s End time is later than what we asked for: "
            f"{job.end_time} > {end}"
        )
        job = job.replace(end_time=end)
        job = job.replace(elapsed_time=job_duration_seconds(job))

    assert job.start_time and job.end_time
    return job


def preprocess_and_filter_jobs(
    unfiltered_jobs: Iterable[SlurmJob], start: datetime, end: datetime
) -> list[SlurmJob]:
    """Just a simple filter to regulate certain jobs coming from SARC that might not
    have a proper start time or end time."""

    filtered_jobs: list[SlurmJob] = []
    job_index = 0
    for job_index, job in enumerate(unfiltered_jobs):
        if job.user == "normandf":
            # don't count jobs run for unit tests
            continue

        job = fix_job_start_end_elapsed_time(job)
        if not job:
            continue

        job = clip_job_within_bounds(job, start, end)

        # We only care about jobs that actually ran.
        if job.elapsed_time <= 0:
            continue

        # LD_jobs_output.append(job.json())
        filtered_jobs.append(job)

    print(f"Filtered {len(filtered_jobs)} out of a total of {job_index+1} jobs.")
    return filtered_jobs


def plot_total_jobs_per_week(
    jobs: list[SlurmJob], job_name: str
) -> matplotlib.figure.Figure:
    df = pd.DataFrame(
        [job.start_time for job in jobs],
        columns=["Timestamp"],
    )

    df.set_index("Timestamp", inplace=True)
    # daily_counts = df.resample(rule="D").size()
    daily_counts = df.resample(rule="W-MON").size()
    daily_counts.index = daily_counts.index.strftime("%Y-%m-%d")

    fig, ax = plt.subplots()
    daily_counts.plot(kind="bar", ax=ax)
    # ax.set_xlabel('Date')
    ax.set_ylabel("jobs")
    ax.set_title(f'"{job_name}" jobs over 10 minutes in duration each week')

    # ticks_to_show = daily_counts.index[::5]  # Show every 5th label
    ticks_to_show = daily_counts.index  # Show every 5th label
    ax.set_xticks(range(len(daily_counts.index)))  # Set all possible x-tick positions
    ax.set_xticklabels(
        daily_counts.index, rotation=90
    )  # Apply all labels with rotation
    ax.set_xticklabels(
        [label if label in ticks_to_show else "" for label in daily_counts.index]
    )  # Hide non-selected labels
    # plt.tight_layout()
    fig.tight_layout()
    return fig

    # Plot the histogram
    # daily_counts.plot(kind='bar')
    # plt.xlabel('Day')
    # plt.ylabel('Number of Items')
    # plt.title('Items by Day')
    # plt.xticks(rotation=45)  # Rotate labels to improve readability
    # plt.show()

    # Let's not care about the fact that usernames might differ between clusters.
    # That would be perfectionism at this early exploratory moment.


def plot_unique_users_each_week(
    jobs: list[SlurmJob], job_name: str
) -> matplotlib.figure.Figure:
    # unique_users_each_day: dict[str, set[str]] = defaultdict(set)
    # for job in jobs:
    #     assert job.start_time
    #     day = job.start_time.strftime("%Y-%m-%d")
    #     unique_users_each_day[day].add(job.user)

    df = pd.DataFrame(
        {
            "job_id": [job.job_id for job in jobs],
            "user": [job.user for job in jobs],
            "start_date": [job.start_time.strftime("%Y-%m-%d") for job in jobs],
        }
    )
    df.set_index("job_id", inplace=True)
    df["start_date"] = pd.to_datetime(df["start_date"])

    # Unique users each week:
    df = df.groupby(pd.Grouper(key="start_date", freq="W-MON"))["user"].nunique()

    daily_counts = df.sort_index()
    ax: matplotlib.axes._axes.Axes
    fig, ax = plt.subplots()
    daily_counts.plot(kind="bar", ax=ax)
    # ax.set_xlabel('Date')
    ax.set_ylabel("Users")
    ax.set_title(f'Unique users of "{job_name}" each week')

    ax.set_xticks(range(len(daily_counts.index)))  # Set all possible x-tick positions
    ax.set_xticklabels(
        daily_counts.index.strftime("%Y-%m-%d"), rotation=90
    )  # Apply all labels with rotation

    fig.tight_layout()
    return fig


def upload_figures_to_google_drive(figures: list[Path]):
    # gauth = GoogleAuth()
    # drive = GoogleDrive(gauth)

    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    from googleapiclient.http import MediaFileUpload
    from google.oauth2.credentials import Credentials

    # SCOPES = ["https://www.googleapis.com/auth/drive.metadata.write"]

    # creds = os.environ.get("GOOGLE_DRIVE_API_KEY")
    try:
        creds = Credentials.from_authorized_user_file("token.json")
    except Exception as exception:
        print(
            "Unable to authenticate with google drive API, skipping uploading figures."
        )
        print(f"exc: {exception}")
        return

    try:
        # create drive api client
        service = build("drive", "v3", credentials=creds)

        for figure in figures:
            file_metadata = {"name": figure.name}
            media = MediaFileUpload(
                filename=figure.name,
                mimetype=f"image/{figure.suffix.removeprefix('.')}",
            )
            # pylint: disable=maybe-no-member
            file = (
                service.files()
                .create(body=file_metadata, media_body=media, fields="id")
                .execute()
            )
            print(f'File ID: {file.get("id")}')

    except HttpError as error:
        print(f"An error occurred: {error}")
        file = None


if __name__ == "__main__":
    main()
