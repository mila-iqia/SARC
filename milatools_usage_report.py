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
from sarc.config import MTL
from sarc.jobs.job import SlurmJob, get_jobs
import simple_parsing
from typing_extensions import TypeGuard

SARC_DIR = Path.home() / "repos" / "SARC"
# Remember to set up the port forwarding if you want
# to access data from SARC.
#    ssh -L 27017:localhost:27017 sarc
# Change this to the path to your config file.
sarc_config_file = Path("sarc-client.json")
assert sarc_config_file.exists()
os.environ["SARC_CONFIG"] = str(sarc_config_file)

logger = get_logger(__name__)

@dataclass
class Args:
    start_date: datetime = datetime.now(tz=MTL) - timedelta(days=30)
    end_date: datetime = datetime.now(tz=MTL)

def main():
    
    parser = simple_parsing.ArgumentParser(description="Analyze the milatools usage.")
    parser.add_arguments(Args, dest="args")
    args: Args = parser.parse_args().args

    print("Args:")
    print(pprint.pprint(dataclasses.asdict(args)))
    date_start = args.start_date #datetime(year=2024, month=1, day=1, tzinfo=MTL)
    date_end = args.end_date  #datetime(year=2025, month=1, day=1, tzinfo=MTL)

    # Don't forget to `ssh -L 27017:localhost:27017 sarc` before running this
    # or else you won't be able to connect to the SARC database.
    unfiltered_jobs = retrieve_data(
        date_start,
        date_end,
        job_name="mila-code",
        cache_dir=Path(os.environ.get("SCRATCH", tempfile.gettempdir())),
    )
    filtered_jobs = preprocess_and_filter_jobs(unfiltered_jobs, date_start, date_end)
    jobs = filtered_jobs

    milacode_jobs = [job for job in jobs if job.name == "mila-code"]
    print(f"We have {len(milacode_jobs)} mila-code jobs.")

    n_jobs_over_30_minutes = len(
        list(job for job in milacode_jobs if job.duration.total_seconds() > 30 * 60)
    )
    print(n_jobs_over_30_minutes)
    print(f"We have {n_jobs_over_30_minutes} mila-code jobs over 30 minutes.")

    milacode_jobs_over_10_minutes = [
        job for job in jobs if job.duration.total_seconds() >= 10 * 60
    ]
    print(
        f"We have {len(milacode_jobs_over_10_minutes)} mila-code jobs over 10 minutes."
    )
    fig = plot_total_jobs_per_week(milacode_jobs_over_10_minutes)
    name = "mila-code-jobs-over-10-minutes.png"
    fig.savefig(name)
    print(f"Saved figure at {Path.cwd() / 'mila-code-jobs-over-10-minutes.png'}.")
    
    fig = plot_unique_users_each_week(jobs)
    name2 = "unique-users-mila-code.png"
    fig.savefig(name2)
    print(f"Saved figure at {Path.cwd() / name2}.")
    # print(list(job.duration.total_seconds() for job in L_milacode_jobs))
    # print(list(job.user for job in L_milacode_jobs))


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


def plot_total_jobs_per_week(jobs: list[SlurmJob]) -> matplotlib.figure.Figure:
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
    ax.set_ylabel("Number of jobs")
    ax.set_title('Number of "mila-code" jobs over 10 minutes in duration each week')

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


def plot_unique_users_each_week(jobs: list[SlurmJob]) -> matplotlib.figure.Figure:
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
    ax.set_ylabel("Number of unique users with jobs")
    ax.set_title('Number of unique users of "mila-code" each week')

    ax.set_xticks(range(len(daily_counts.index)))  # Set all possible x-tick positions
    ax.set_xticklabels(
        daily_counts.index.strftime("%Y-%m-%d"), rotation=90
    )  # Apply all labels with rotation

    fig.tight_layout()
    return fig


if __name__ == "__main__":
    main()
