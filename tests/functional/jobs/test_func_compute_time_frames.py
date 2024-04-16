import zoneinfo
from datetime import datetime, timedelta

import pandas
import pytest

from sarc.jobs.series import (
    compute_cost_and_waste,
    compute_time_frames,
    load_job_series,
)

from .test_func_load_job_series import MOCK_TIME

FIELDS = [
    "job_id",
    "user",
    "cluster_name",
    "start_time",
    "end_time",
    "elapsed_time",
    "cpu_cost",
]

FRAME_FIELDS = FIELDS + ["duration", "timestamp"]


def _df_to_pretty_str(df: pandas.DataFrame, fields: list) -> str:
    return df[fields].to_markdown()


@pytest.mark.freeze_time(MOCK_TIME)
@pytest.mark.usefixtures("read_only_db", "tzlocal_is_mtl")
def test_compute_time_frames(file_regression):
    jobs = load_job_series()
    compute_cost_and_waste(jobs)
    time_frames = compute_time_frames(
        jobs, columns=["elapsed_time", "cpu_cost"], frame_size=timedelta(days=1)
    )
    assert len(jobs) < len(time_frames)
    file_regression.check(
        f"Compute time frames for {jobs.shape[0]} job(s):"
        f"\n\n{_df_to_pretty_str(jobs, FIELDS)}"
        f"\n\nTime frames with {time_frames.shape[0]} rows:"
        f"\n\n{_df_to_pretty_str(time_frames, FRAME_FIELDS)}"
    )


@pytest.mark.freeze_time(MOCK_TIME)
@pytest.mark.usefixtures("read_only_db", "tzlocal_is_mtl")
def test_compute_time_frames_default_frame_size(file_regression):
    # Default frame size is 7 days and should cover all tested jobs,
    # so, there should be only one time frame in output, with same line number as initial jobs.
    jobs = load_job_series()
    compute_cost_and_waste(jobs)
    time_frames = compute_time_frames(jobs, columns=["elapsed_time", "cpu_cost"])
    assert len(jobs) == len(time_frames)
    file_regression.check(
        f"Compute time frames for {jobs.shape[0]} job(s):"
        f"\n\n{_df_to_pretty_str(jobs, FIELDS)}"
        f"\n\nTime frames with {time_frames.shape[0]} rows:"
        f"\n\n{_df_to_pretty_str(time_frames, FRAME_FIELDS)}"
    )


@pytest.mark.freeze_time(MOCK_TIME)
@pytest.mark.usefixtures("read_only_db", "tzlocal_is_mtl")
def test_compute_time_frames_explicit_start(file_regression):
    jobs = load_job_series()
    compute_cost_and_waste(jobs)
    time_frames = compute_time_frames(
        jobs,
        columns=["elapsed_time", "cpu_cost"],
        start=datetime(2023, 2, 17, tzinfo=zoneinfo.ZoneInfo("America/Montreal")),
        frame_size=timedelta(days=1),
    )
    assert len(jobs) > len(time_frames)
    file_regression.check(
        f"Compute time frames for {jobs.shape[0]} job(s):"
        f"\n\n{_df_to_pretty_str(jobs, FIELDS)}"
        f"\n\nTime frames with {time_frames.shape[0]} rows:"
        f"\n\n{_df_to_pretty_str(time_frames, FRAME_FIELDS)}"
    )


@pytest.mark.freeze_time(MOCK_TIME)
@pytest.mark.usefixtures("read_only_db", "tzlocal_is_mtl")
def test_compute_time_frames_explicit_end(file_regression):
    jobs = load_job_series()
    compute_cost_and_waste(jobs)
    time_frames = compute_time_frames(
        jobs,
        columns=["elapsed_time", "cpu_cost"],
        end=datetime(2023, 2, 17, tzinfo=zoneinfo.ZoneInfo("America/Montreal")),
        frame_size=timedelta(days=1),
    )
    assert len(jobs) > len(time_frames)
    file_regression.check(
        f"Compute time frames for {jobs.shape[0]} job(s):"
        f"\n\n{_df_to_pretty_str(jobs, FIELDS)}"
        f"\n\nTime frames with {time_frames.shape[0]} rows:"
        f"\n\n{_df_to_pretty_str(time_frames, FRAME_FIELDS)}"
    )


@pytest.mark.freeze_time(MOCK_TIME)
@pytest.mark.usefixtures("read_only_db", "tzlocal_is_mtl")
def test_compute_time_frames_explicit_start_and_end(file_regression):
    jobs = load_job_series()
    compute_cost_and_waste(jobs)
    time_frames = compute_time_frames(
        jobs,
        columns=["elapsed_time", "cpu_cost"],
        start=datetime(2023, 2, 16, tzinfo=zoneinfo.ZoneInfo("America/Montreal")),
        end=datetime(2023, 2, 18, tzinfo=zoneinfo.ZoneInfo("America/Montreal")),
        frame_size=timedelta(days=1),
    )
    assert len(jobs) > len(time_frames)
    file_regression.check(
        f"Compute time frames for {jobs.shape[0]} job(s):"
        f"\n\n{_df_to_pretty_str(jobs, FIELDS)}"
        f"\n\nTime frames with {time_frames.shape[0]} rows:"
        f"\n\n{_df_to_pretty_str(time_frames, FRAME_FIELDS)}"
    )
