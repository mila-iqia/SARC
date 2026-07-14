"""
Tests for compute_time_frames function.
Use a load_job_series wrapper around JobSeriesDB.
"""

import zoneinfo
from datetime import datetime, timedelta

import pandas
import pytest

from sarc.client.series import compute_time_frames
from sarc.config import UTC
from tests.functional.job_series.test_func_load_job_series import (
    sql_load_job_series as load_job_series,
)

# Local MOCK_TIME close to the fixture jobs' submit_times (Feb 14-19), so that
# running jobs (NULL end_time, filled with now() by the shim) get a tight span
# rather than spanning to the global MOCK_TIME (November).
_LOCAL_MOCK_TIME = datetime(2023, 2, 22, tzinfo=UTC)

FIELDS = [
    "job_id",
    "cluster_user",
    "cluster_name",
    "start_time",
    "end_time",
    "elapsed_time",
    "requested_cpu_cost",
]

FRAME_FIELDS = FIELDS + ["duration", "timestamp"]


def _df_to_pretty_str(df: pandas.DataFrame, fields: list) -> str:
    return df[fields].to_markdown()


@pytest.mark.time_machine(_LOCAL_MOCK_TIME, tick=False)
@pytest.mark.usefixtures("tzlocal_is_mtl")
def test_compute_time_frames(read_only_db, file_regression):
    jobs = load_job_series(read_only_db)
    time_frames = compute_time_frames(
        jobs, columns=["elapsed_time", "requested_cpu_cost"], frame_size=timedelta(days=1)
    )
    assert len(jobs) < len(time_frames)
    file_regression.check(
        f"Compute time frames for {jobs.shape[0]} job(s):"
        f"\n\n{_df_to_pretty_str(jobs, FIELDS)}"
        f"\n\nTime frames with {time_frames.shape[0]} rows:"
        f"\n\n{_df_to_pretty_str(time_frames, FRAME_FIELDS)}"
    )


@pytest.mark.time_machine(_LOCAL_MOCK_TIME, tick=False)
@pytest.mark.usefixtures("tzlocal_is_mtl")
def test_compute_time_frames_default_frame_size(read_only_db, file_regression):
    # Default frame size is 7 days
    jobs = load_job_series(read_only_db)
    time_frames = compute_time_frames(jobs, columns=["elapsed_time", "requested_cpu_cost"])
    assert len(jobs) < len(time_frames)
    file_regression.check(
        f"Compute time frames for {jobs.shape[0]} job(s):"
        f"\n\n{_df_to_pretty_str(jobs, FIELDS)}"
        f"\n\nTime frames with {time_frames.shape[0]} rows:"
        f"\n\n{_df_to_pretty_str(time_frames, FRAME_FIELDS)}"
    )


@pytest.mark.time_machine(_LOCAL_MOCK_TIME, tick=False)
@pytest.mark.usefixtures("tzlocal_is_mtl")
def test_compute_time_frames_full_frame_size(read_only_db, file_regression):
    # Use a frame size wide enough to cover all jobs
    # so, there should be only one time frame in output, with same line number as initial jobs.
    jobs = load_job_series(read_only_db)
    time_frames = compute_time_frames(
        jobs, columns=["elapsed_time", "requested_cpu_cost"], frame_size=timedelta(days=30)
    )
    assert len(jobs) == len(time_frames)
    file_regression.check(
        f"Compute time frames for {jobs.shape[0]} job(s):"
        f"\n\n{_df_to_pretty_str(jobs, FIELDS)}"
        f"\n\nTime frames with {time_frames.shape[0]} rows:"
        f"\n\n{_df_to_pretty_str(time_frames, FRAME_FIELDS)}"
    )


@pytest.mark.time_machine(_LOCAL_MOCK_TIME, tick=False)
@pytest.mark.usefixtures("tzlocal_is_mtl")
def test_compute_time_frames_explicit_start(read_only_db, file_regression):
    jobs = load_job_series(read_only_db)
    start = datetime(2023, 2, 17, tzinfo=zoneinfo.ZoneInfo("America/Montreal"))
    time_frames = compute_time_frames(
        jobs,
        columns=["elapsed_time", "requested_cpu_cost"],
        start=start,
        frame_size=timedelta(days=1),
    )
    assert (time_frames["timestamp"] >= start).all()
    file_regression.check(
        f"Compute time frames for {jobs.shape[0]} job(s):"
        f"\n\n{_df_to_pretty_str(jobs, FIELDS)}"
        f"\n\nTime frames with {time_frames.shape[0]} rows:"
        f"\n\n{_df_to_pretty_str(time_frames, FRAME_FIELDS)}"
    )


@pytest.mark.time_machine(_LOCAL_MOCK_TIME, tick=False)
@pytest.mark.usefixtures("tzlocal_is_mtl")
def test_compute_time_frames_explicit_end(read_only_db, file_regression):
    jobs = load_job_series(read_only_db)
    end = datetime(2023, 2, 17, tzinfo=zoneinfo.ZoneInfo("America/Montreal"))
    time_frames = compute_time_frames(
        jobs,
        columns=["elapsed_time", "requested_cpu_cost"],
        end=end,
        frame_size=timedelta(days=1),
    )
    assert (time_frames["timestamp"] <= end).all()
    file_regression.check(
        f"Compute time frames for {jobs.shape[0]} job(s):"
        f"\n\n{_df_to_pretty_str(jobs, FIELDS)}"
        f"\n\nTime frames with {time_frames.shape[0]} rows:"
        f"\n\n{_df_to_pretty_str(time_frames, FRAME_FIELDS)}"
    )


@pytest.mark.time_machine(_LOCAL_MOCK_TIME, tick=False)
@pytest.mark.usefixtures("tzlocal_is_mtl")
def test_compute_time_frames_explicit_start_and_end(read_only_db, file_regression):
    jobs = load_job_series(read_only_db)
    start = datetime(2023, 2, 16, tzinfo=zoneinfo.ZoneInfo("America/Montreal"))
    end = datetime(2023, 2, 18, tzinfo=zoneinfo.ZoneInfo("America/Montreal"))
    time_frames = compute_time_frames(
        jobs,
        columns=["elapsed_time", "requested_cpu_cost"],
        start=start,
        end=end,
        frame_size=timedelta(days=1),
    )
    assert (time_frames["timestamp"] >= start).all()
    assert (time_frames["timestamp"] <= end).all()
    file_regression.check(
        f"Compute time frames for {jobs.shape[0]} job(s):"
        f"\n\n{_df_to_pretty_str(jobs, FIELDS)}"
        f"\n\nTime frames with {time_frames.shape[0]} rows:"
        f"\n\n{_df_to_pretty_str(time_frames, FRAME_FIELDS)}"
    )
