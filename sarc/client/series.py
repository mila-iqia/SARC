from __future__ import annotations

from datetime import datetime, timedelta
from typing import Callable

import numpy as np
import pandas
from flatten_dict import flatten

from sarc.client.job import count_jobs, get_jobs
from sarc.client.users.api import User, get_users
from sarc.common.utils import tqdm
from sarc.config import MTL
from sarc.traces import trace_decorator

DUMMY_STATS = {
    label: np.nan
    for label in [
        "gpu_utilization",
        "cpu_utilization",
        "gpu_memory",
        "gpu_power",
        "system_memory",
    ]
}


# pylint: disable=too-many-branches,too-many-statements,fixme
@trace_decorator()
def load_job_series(
    *,
    fields: None | list[str] | dict[str, str] = None,
    clip_time: bool = False,
    callback: None | Callable = None,
    **jobs_args,
) -> pandas.DataFrame:
    """
    Query jobs from the database and return them in a DataFrame, including full user info
    for each job.

    Parameters
    ----------
    fields: list or dict
        Job fields to include in the DataFrame. By default, include all fields.
        A dictionary may be passed to select fields and rename them in the DataFrame.
        In such case, the keys are the fields' names and the values are the names
        they will have in the DataFrame.
    clip_time: bool
        Whether the duration time of the jobs should be clipped within `start` and `end`.
        ValueError will be raised if `clip_time` is True and either of `start` or `end` is None.
        Defaults to False.
    callback: Callable
        Callable taking the list of job dictionaries in the format it would be included in the DataFrame.
    **jobs_args
        Arguments to be passed to `get_jobs` to query jobs from the database.

    Returns
    -------
    DataFrame
        Panda's data frame containing jobs, with following columns:
        - All fields returned by method SlurmJob.dict(), except "requested" and "allocated"
          which are flattened into `requested.<attribute>` and `allocated.<attribute>` fields.
        - Job series fields:
          "gpu_utilization", "cpu_utilization", "gpu_memory", "gpu_power", "system_memory"
        - Optional job series fields, added if clip_time is True:
          "unclipped_start" and "unclipped_end"
        - Optional user info fields if job users found.
          Fields from `User.dict()` in format `user.<flattened dot-separated field>`,
          + special field `user.primary_email` containing either `user.mila.email` or fallback `job.user`.
    """

    # If fields is a list, convert it to a renaming dict with same old and new names.
    if isinstance(fields, list):
        fields = {key: key for key in fields}
    elif fields is None:
        fields = {}

    start = jobs_args.get("start", None)
    end = jobs_args.get("end", None)

    total = count_jobs(**jobs_args)

    # Get users data frame.
    users_frame = _get_user_data_frame()

    rows = []
    now = datetime.now(tz=MTL)
    # Fetch all jobs from the clusters
    for job in tqdm(get_jobs(**jobs_args), total=total, desc="load job series"):
        if job.end_time is None:
            job.end_time = now

        # For some reason start time is not reliable, often equal to submit time,
        # so we infer it based on end_time and elapsed_time.
        job.start_time = job.end_time - timedelta(seconds=job.elapsed_time)

        unclipped_start = None
        unclipped_end = None
        if clip_time:
            if start is None:
                raise ValueError("Clip time: missing start")
            if end is None:
                raise ValueError("Clip time: missing end")
            # Clip the job to the time range we are interested in.
            unclipped_start = job.start_time
            job.start_time = max(job.start_time, start)
            unclipped_end = job.end_time
            job.end_time = min(job.end_time, end)
            # Could be negative if job started after end. We don't want to filter
            # them out because they have been submitted before end, and we want to
            # compute their wait time.
            job.elapsed_time = max((job.end_time - job.start_time).total_seconds(), 0)

        if job.stored_statistics is None:
            job_series = DUMMY_STATS.copy()
        else:
            job_series = job.stored_statistics.dict()
            job_series = {k: _select_stat(k, v) for k, v in job_series.items()}

            # Replace `gpu_utilization > 1` with nan.
            if (
                job.stored_statistics.gpu_utilization
                and job_series["gpu_utilization"] > 1
            ):
                job_series["gpu_utilization"] = np.nan

        # Flatten job.requested and job.allocated into job_series
        job_series.update(
            {f"requested.{key}": value for key, value in job.requested.dict().items()}
        )
        job_series.update(
            {f"allocated.{key}": value for key, value in job.allocated.dict().items()}
        )
        # Additional computations for job.allocated flattened fields.
        # TODO: Why is it possible to have billing smaller than gres_gpu???
        billing = job.allocated.billing or 0
        gres_gpu = job.requested.gres_gpu or 0
        if gres_gpu:
            job_series["allocated.gres_gpu"] = max(billing, gres_gpu)
            job_series["allocated.cpu"] = job.allocated.cpu
        else:
            job_series["allocated.gres_gpu"] = 0
            job_series["allocated.cpu"] = (
                max(billing, job.allocated.cpu) if job.allocated.cpu else 0
            )

        if clip_time:
            job_series["unclipped_start"] = unclipped_start
            job_series["unclipped_end"] = unclipped_end

        # Merge job series and job,
        # with job series overriding job fields if necessary.
        # Do not include raw requested and allocated anymore.
        final_job_dict = job.dict(exclude={"requested", "allocated"})
        final_job_dict.update(job_series)
        job_series = final_job_dict

        if fields:
            job_series = {
                new_name: job_series[old_name] for old_name, new_name in fields.items()
            }
        rows.append(job_series)
        if callback:
            callback(rows)

    jobs_frame = pandas.DataFrame(rows)

    # Merge jobs with users info, only if users available.
    if users_frame.shape[0]:
        # Get name pf fields used to merge frames.
        # We must use `fields`, as fields may have been renamed.
        field_cluster_name = fields.get("cluster_name", "cluster_name")
        field_job_user = fields.get("user", "user")

        df_mila_mask = jobs_frame[field_cluster_name] == "mila"
        df_drac_mask = jobs_frame[field_cluster_name] != "mila"

        merged_mila = jobs_frame[df_mila_mask].merge(
            users_frame,
            left_on=field_job_user,
            right_on="user.mila.username",
            how="left",
        )
        merged_drac = jobs_frame[df_drac_mask].merge(
            users_frame,
            left_on=field_job_user,
            right_on="user.drac.username",
            how="left",
        )

        # Concat merged frames.
        output = pandas.concat([merged_mila, merged_drac])
        # Try to sort output to keep initial jobs order, by using first column from jobs frame.
        # Sort inplace to avoid producing a supplementary frame.
        output.sort_values(by=jobs_frame.columns[0], inplace=True, ignore_index=True)

        # Replace NaN in column `user.primary_email` with corresponding value in `job.user`
        df_primary_email_nan_mask = output["user.primary_email"].isnull()
        output.loc[df_primary_email_nan_mask, "user.primary_email"] = output[
            field_job_user
        ][df_primary_email_nan_mask]

        return output
    else:
        return jobs_frame


def _get_user_data_frame() -> pandas.DataFrame:
    """
    Get all available users in a pandas DataFrame.

    Returns
    -------
    DataFrame
        A data frame containing all users
        with flattened dot-separated columns from User class.
    """
    uf = UserFlattener()
    return pandas.DataFrame([uf.flatten(user) for user in get_users()])


class UserFlattener:
    """
    Helper class to flatten a user.

    The goal of this class is to make sure that
    User's complex attributes are not flattened if set to None,
    to prevent having both attribute columns and attribute nested columns
    in final data frame.

    For example, current User class has optional attribute `drac` to be flattened as
    `user.drac.username`, `user.drac.email` and `user.drac.active`.
    If a user does not have a DRAC account, default behaviour will produce a key
    `user.drac` with value None.
    Instead, we want to avoid having `user.drac` key, to make sure
    output data frame only contains the 3 `user.drac.*` expanded columns
    and simply set them to NaN for users who don't have a drac account.
    """

    def __init__(self):
        # List "plain" attributes, i.e. attributes that are not objects.
        # This will exclude both nested Model objects as well a nested dicts.
        # Note that a `date` is described as a 'string' in schemas.
        schema = User.schema()
        schema_props = schema["properties"]
        self.plain_attributes = {
            key
            for key, prop_desc in schema_props.items()
            if "type" in prop_desc and prop_desc["type"] != "object"
        }

    def flatten(self, user: User) -> dict:
        """Flatten given user."""
        # Get user dict.
        base_user_dict = user.dict(exclude={"id"})
        # Keep only plain attributes, or complex attributes that are not None.
        base_user_dict = {
            key: value
            for key, value in base_user_dict.items()
            if key in self.plain_attributes or value is not None
        }
        # Now flatten user dict.
        user_dict = flatten({"user": base_user_dict}, reducer="dot")
        # And add special key `user.primary_email`.
        user_dict["user.primary_email"] = user.mila.email
        return user_dict


def _select_stat(name, dist):
    if not dist:
        return np.nan

    if name in ["system_memory", "gpu_memory"]:
        return dist["max"]

    return dist["median"]


def compute_cost_and_waste(full_df: pandas.DataFrame) -> pandas.DataFrame:
    """
    Compute cost and waste for given pandas DataFrame.

    Parameters
    ----------
    full_df: DataFrame
        A pandas DataFrame returned by function load_job_series().

    Returns
    -------
    DataFrame
        Input data frame with additional columns:
        "cpu_cost", "cpu_waste", "cpu_equivalent_cost", "cpu_equivalent_waste", "cpu_overbilling_cost",
        "gpu_cost", "gpu_waste", "gpu_equivalent_cost", "gpu_equivalent_waste", "gpu_overbilling_cost".
    """
    full_df = _compute_cost_and_wastes(full_df, "cpu")
    full_df = _compute_cost_and_wastes(full_df, "gpu")
    return full_df


def _compute_cost_and_wastes(data, device):
    device_col = {"cpu": "cpu", "gpu": "gres_gpu"}[device]

    data[f"{device}_cost"] = data["elapsed_time"] * data[f"requested.{device_col}"]
    data[f"{device}_waste"] = (1 - data[f"{device}_utilization"]) * data[
        f"{device}_cost"
    ]

    data[f"{device}_equivalent_cost"] = (
        data["elapsed_time"] * data[f"allocated.{device_col}"]
    )
    data[f"{device}_equivalent_waste"] = (1 - data[f"{device}_utilization"]) * data[
        f"{device}_equivalent_cost"
    ]

    data[f"{device}_overbilling_cost"] = data["elapsed_time"] * (
        data[f"allocated.{device_col}"] - data[f"requested.{device_col}"]
    )

    return data


def compute_time_frames(
    jobs,
    columns: list[str] | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    frame_size: timedelta = timedelta(days=7),
):
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
        Start of the time frame. If None, use the first job start time.
    end: datetime, optional
        End of the time frame. If None, use the last job end time.
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

    if end is None:
        end = jobs[col_end].max()

    data_frames = []

    total_durations = (jobs[col_end] - jobs[col_start]).dt.total_seconds()
    for frame_start in pandas.date_range(start, end, freq=frame_size):
        frame_end = frame_start + frame_size

        mask = (jobs[col_start] < frame_end) * (jobs[col_end] > frame_start)
        frame = jobs[mask].copy()
        total_durations_in_frame = total_durations[mask]
        frame[col_start] = frame[col_start].clip(frame_start, frame_end)
        frame[col_end] = frame[col_end].clip(frame_start, frame_end)
        frame["duration"] = (frame[col_end] - frame[col_start]).dt.total_seconds()

        # Adjust columns to fit the time frame.
        for column in columns:
            frame[column] *= frame["duration"] / total_durations_in_frame

        frame["timestamp"] = frame_start

        data_frames.append(frame)

    return pandas.concat(data_frames, axis=0)
