import logging
from datetime import datetime, timedelta

import pandas
from pandas import DataFrame

from sarc.config import UTC

logger = logging.getLogger(__name__)


def compute_time_frames(
    jobs: DataFrame,
    columns: list[str] | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    frame_size: timedelta = timedelta(days=7),
) -> DataFrame:
    """Slice jobs into time frames and adjust columns to fit the time frames.

    Jobs that start before `start` or ends after `end` will have their running
    time clipped to fitting within the interval (`start`, `end`).

    Jobs spanning multiple time frames will have their running time sliced
    according to the time frames.

    The resulting DataFrame will have the additional columns 'duration' and 'timestamp'
    which represent the duration of a job within a time frame and the start of the time frame.

    Parameters
    ----------
    jobs: DataFrame
        Pandas DataFrame containing jobs data. Typically generated with `load_job_series`.
        Must contain columns `start_time` and `end_time`.
    columns: list of str
        Columns to adjust based on time frames.
    start: datetime, optional
        Start of the time frame. If naive, as in local timezone. If None, use the first job start time.
    end: datetime, optional
        End of the time frame. If naive, as in local timezone. If None, use the last job end time.
    frame_size: timedelta, optional
        Size of the time frames used to compute histograms. Default to 7 days.

    Examples
    --------
    >>> data = pd.DataFrame(
        [
            [datetime(2023, 3, 5), datetime(2023, 3, 6), "a", "A", 10],
            [datetime(2023, 3, 6), datetime(2023, 3, 9), "a", "B", 10],
            [datetime(2023, 3, 6), datetime(2023, 3, 7), "b", "B", 20],
            [datetime(2023, 3, 6), datetime(2023, 3, 8), "b", "B", 20],
        ],
        columns=["start_time", "end_time", "user", "cluster", 'cost'],
    )
    >>> compute_time_frames(data, columns=['cost'], frame_size=timedelta(days=2))
      start_time   end_time user cluster       cost  duration  timestamp
    0 2023-03-05 2023-03-06    a       A  10.000000   86400.0 2023-03-05
    1 2023-03-06 2023-03-07    a       B   3.333333   86400.0 2023-03-05
    2 2023-03-06 2023-03-07    b       B  20.000000   86400.0 2023-03-05
    3 2023-03-06 2023-03-07    b       B  10.000000   86400.0 2023-03-05
    1 2023-03-07 2023-03-09    a       B   6.666667  172800.0 2023-03-07
    3 2023-03-07 2023-03-08    b       B  10.000000   86400.0 2023-03-07
    """
    col_start = "start_time"
    col_end = "end_time"

    if columns is None:
        columns = []

    if start is None:
        start = jobs[col_start].min()
    else:
        start = start.astimezone(UTC)

    if end is None:
        end = jobs[col_end].max()
    else:
        end = end.astimezone(UTC)

    data_frames: list[pandas.DataFrame] = []

    total_durations: pandas.Series[float] = (
        jobs[col_end] - jobs[col_start]
    ).dt.total_seconds()  # type: ignore[attr-defined]
    for frame_start in pandas.date_range(start, end, freq=frame_size):
        frame_end = frame_start + frame_size

        mask = (jobs[col_start] < frame_end) & (jobs[col_end] > frame_start)
        frame = jobs[mask].copy()
        total_durations_in_frame = total_durations[mask]
        frame[col_start] = frame[col_start].clip(frame_start, frame_end)  # type: ignore[call-overload]
        frame[col_end] = frame[col_end].clip(frame_start, frame_end)  # type: ignore[call-overload]
        frame["duration"] = (frame[col_end] - frame[col_start]).dt.total_seconds()  # type: ignore[attr-defined]

        # Adjust columns to fit the time frame.
        for column in columns:
            frame[column] *= frame["duration"] / total_durations_in_frame

        frame["timestamp"] = frame_start

        data_frames.append(frame)

    return pandas.concat(data_frames, axis=0)
