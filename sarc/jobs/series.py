from __future__ import annotations

import json
import logging
import os.path
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Callable

import numpy as np
import pandas
from flatten_dict import flatten
from pandas import DataFrame
from prometheus_api_client import MetricRangeDataFrame
from tqdm import tqdm

from sarc.config import MTL, UTC, ClusterConfig, config
from sarc.jobs.job import JobStatistics, Statistics, count_jobs, get_jobs
from sarc.ldap.api import User, get_users
from sarc.traces import trace_decorator

if TYPE_CHECKING:
    from sarc.jobs.sacct import SlurmJob


# pylint: disable=too-many-branches
@trace_decorator()
def get_job_time_series(
    job: SlurmJob,
    metric: str,
    min_interval: int = 30,
    max_points: int = 100,
    measure: str | None = None,
    aggregation: str = "total",
    dataframe: bool = True,
):
    """Fetch job metrics.

    Arguments:
        cluster: The cluster on which to fetch metrics.
        job: The job for which to fetch metrics.
        metric: The metric, which must be in ``slurm_job_metric_names``.
        min_interval: The minimal reporting interval, in seconds.
        max_points: The maximal number of data points to return.
        measure: The aggregation measure to use ("avg_over_time", etc.)
            A format string can be passed, e.g. ("quantile_over_time(0.5, {})")
            to get the median.
        aggregation: Either "total", to aggregate over the whole range, or
            "interval", to aggregate over each interval.
        dataframe: If True, return a DataFrame. Otherwise, return the list of
            dicts returned by Prometheus's API.
    """

    if aggregation not in ("interval", "total", None):
        raise ValueError(
            f"Aggregation must be one of ['total', 'interval', None]: {aggregation}"
        )

    if job.job_state != "RUNNING" and not job.elapsed_time:
        return None if dataframe else []
    if metric not in slurm_job_metric_names:
        raise ValueError(f"Unknown metric name: {metric}")

    selector = f'{metric}{{slurmjobid=~"{job.job_id}"}}'

    now = datetime.now(tz=UTC).astimezone(MTL)

    ago = now - job.start_time
    duration = (job.end_time or now) - job.start_time

    offset = int((ago - duration).total_seconds())
    offset_string = f" offset {offset}s" if offset > 0 else ""

    duration_seconds = int(duration.total_seconds())

    # Duration should not be looking in the future
    if offset < 0:
        duration_seconds += offset

    if duration_seconds <= 0:
        return None if dataframe else []

    interval = int(max(duration_seconds / max_points, min_interval))

    query = selector

    if measure and aggregation:
        if aggregation == "interval":
            range_seconds = interval
        elif aggregation == "total":
            offset += duration_seconds
            range_seconds = duration_seconds

        query = f"{query}[{range_seconds}s]"
        if "(" in measure:
            query = measure.format(f"{query} {offset_string}")
        else:
            query = f"{measure}({query} {offset_string})"
        query = f"{query}[{duration_seconds}s:{range_seconds}s]"
    else:
        query = f"{query}[{duration_seconds}s:{interval}s] {offset_string}"

    results = job.fetch_cluster_config().prometheus.custom_query(query)
    if dataframe:
        return MetricRangeDataFrame(results) if results else None
    else:
        return results


def get_job_time_series_metric_names():
    """Return all the metric names that relate to slurm jobs."""
    return slurm_job_metric_names


@trace_decorator()
def compute_job_statistics_from_dataframe(
    df: DataFrame,
    statistics,
    normalization=float,
    unused_threshold=0.01,
    is_time_counter=False,
):
    df = df.reset_index()

    groupby = ["instance", "core", "gpu"]
    groupby = [col for col in groupby if col in df]

    gdf = df.groupby(groupby)

    if is_time_counter:
        # This is a time-based counter like the cpu counters in /proc/stat, with
        # a resolution of 1 nanosecond.
        df["timediffs"] = gdf["timestamp"].diff().map(lambda x: x.total_seconds())
        df["value"] = gdf["value"].diff() / df["timediffs"] / 1e9
        df = df.drop(index=0)

    if unused_threshold is not None:
        means = gdf["value"].mean()
        n_unused = (means < unused_threshold).astype(bool).sum()

        df_with_means = pandas.merge(
            df, means.reset_index().rename(columns={"value": "mean"}), on=groupby
        )

        df = df_with_means[df_with_means["mean"] >= unused_threshold]
    else:
        n_unused = 0

    rval = {name: normalization(fn(df["value"])) for name, fn in statistics.items()}
    return {**rval, "unused": n_unused}


def compute_job_statistics_one_metric(
    job: SlurmJob,
    metric_name,
    statistics,
    normalization=float,
    unused_threshold=0.01,
    is_time_counter=False,
):
    df = job.series(metric=metric_name, max_points=10_000)
    if df is None:
        return None
    return compute_job_statistics_from_dataframe(
        df=df,
        statistics=statistics,
        normalization=normalization,
        unused_threshold=unused_threshold,
        is_time_counter=is_time_counter,
    )


@trace_decorator()
def compute_job_statistics(job: SlurmJob):
    statistics_dict = {
        "mean": DataFrame.mean,
        "std": DataFrame.std,
        "max": DataFrame.max,
        "q25": lambda self: self.quantile(0.25),
        "median": DataFrame.median,
        "q75": lambda self: self.quantile(0.75),
        "q05": lambda self: self.quantile(0.05),
    }

    gpu_utilization = compute_job_statistics_one_metric(
        job,
        "slurm_job_utilization_gpu",
        statistics=statistics_dict,
        unused_threshold=0.01,
        normalization=lambda x: float(x / 100),
    )

    gpu_utilization_fp16 = compute_job_statistics_one_metric(
        job,
        "slurm_job_fp16_gpu",
        statistics=statistics_dict,
        unused_threshold=0.01,
        normalization=lambda x: float(x / 100),
    )

    gpu_utilization_fp32 = compute_job_statistics_one_metric(
        job,
        "slurm_job_fp32_gpu",
        statistics=statistics_dict,
        unused_threshold=0.01,
        normalization=lambda x: float(x / 100),
    )

    gpu_utilization_fp64 = compute_job_statistics_one_metric(
        job,
        "slurm_job_fp64_gpu",
        statistics=statistics_dict,
        unused_threshold=0.01,
        normalization=lambda x: float(x / 100),
    )

    gpu_sm_occupancy = compute_job_statistics_one_metric(
        job,
        "slurm_job_sm_occupancy_gpu",
        statistics=statistics_dict,
        unused_threshold=0.01,
        normalization=lambda x: float(x / 100),
    )

    gpu_memory = compute_job_statistics_one_metric(
        job,
        "slurm_job_utilization_gpu_memory",
        statistics=statistics_dict,
        normalization=lambda x: float(x / 100),
        unused_threshold=None,
    )

    gpu_power = compute_job_statistics_one_metric(
        job,
        "slurm_job_power_gpu",
        statistics=statistics_dict,
        unused_threshold=None,
    )

    cpu_utilization = compute_job_statistics_one_metric(
        job,
        "slurm_job_core_usage",
        statistics=statistics_dict,
        unused_threshold=0.01,
        is_time_counter=True,
    )

    system_memory = compute_job_statistics_one_metric(
        job,
        "slurm_job_memory_usage",
        statistics=statistics_dict,
        normalization=lambda x: float(x / 1e6 / job.allocated.mem),
        unused_threshold=None,
    )

    return JobStatistics(
        gpu_utilization=gpu_utilization and Statistics(**gpu_utilization),
        gpu_utilization_fp16=gpu_utilization_fp16
        and Statistics(**gpu_utilization_fp16),
        gpu_utilization_fp32=gpu_utilization_fp32
        and Statistics(**gpu_utilization_fp32),
        gpu_utilization_fp64=gpu_utilization_fp64
        and Statistics(**gpu_utilization_fp64),
        gpu_sm_occupancy=gpu_sm_occupancy and Statistics(**gpu_sm_occupancy),
        gpu_memory=gpu_memory and Statistics(**gpu_memory),
        gpu_power=gpu_power and Statistics(**gpu_power),
        cpu_utilization=cpu_utilization and Statistics(**cpu_utilization),
        system_memory=system_memory and Statistics(**system_memory),
    )


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


# pylint: disable=too-many-statements,fixme
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
        - Optional user info fields if job users found. See `_user_to_series` for user fields.
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


def update_cluster_job_series_rgu(
    df: pandas.DataFrame, cluster_config: ClusterConfig
) -> pandas.DataFrame:
    """
    Compute RGU information for jobs related to given cluster config in a data frame.

    Parameters
    ----------
    df: DataFrame
        Data frame to update, typically returned by `load_job_series`.
        Should contain fields:
        "cluster_name", "start_time", "allocated.gpu_type", "allocated.gres_gpu".
    cluster_config: ClusterConfig
        Configuration of cluster to which jobs to update belong.
        Should define following config:
        "rgu_start_date": date since when billing is given as RGU.
        "gpu_to_rgu_billing": path to a JSON file containing a dict which maps
        GPU type to RGU cost per GPU.

    Returns
    -------
    DataFrame
        Input data frame with:
        - column `allocated.gres_gpu` updated if necessary.
        - column `allocated.gres_rgu` added or updated to contain RGU billing.
          Set to NaN (or unchanged if already present) for jobs from other clusters.
        - column `gpu_type_rgu` added or updated to contain RGU cost per GPU (RGU/GPU ratio).
          Set to NaN (or unchanged if already present) for jobs from other clusters.

    Pseudocode describing how we update data frame:
    for each job: if job.cluster_name == cluster_config.name:
        if start_time < cluster_config.rgu_start_date:
            # We are BEFORE transition to RGU
            if allocated.gpu_type in gpu_to_rgu_billing:
                # compute rgu columns
                allocated.gres_rgu = allocated.gres_gpu * gpu_to_rgu_billing[allocated.gpu_type]
                allocated.gpu_type_rgu = gpu_to_rgu_billing[allocated.gpu_type]
            else:
                # set rgu columns to nan
                allocated.gres_rgu = nan
                allocated.gpu_type_rgu = nan
        else:
            # We are AFTER transition to RGU
            # Anyway, we assume gres_rgu is current gres_gpu
            allocated.gres_rgu = allocated.gres_gpu

            if allocated.gpu_type in gpu_to_rgu_billing:
                # we fix gres_gpu by dividing it with RGU/GPU ratio
                allocated.gres_gpu = allocated.gres_gpu / gpu_to_rgu_billing[allocated.gpu_type]
                # we save RGU/GPU ratio
                allocated.gpu_type_rgu = gpu_to_rgu_billing[allocated.gpu_type]
            else:
                # we cannot fix gres_gpu, so we set it to nan
                allocated.gres_gpu = nan
                # we cannot get RGU/GPU ratio, so we set it to nan
                allocated.gpu_type_rgu = nan
    """

    # Make sure frame will have new RGU columns anyway, with NaN as default value.
    if "allocated.gres_rgu" not in df.columns:
        df["allocated.gres_rgu"] = np.nan
    if "allocated.gpu_type_rgu" not in df.columns:
        df["allocated.gpu_type_rgu"] = np.nan

    if cluster_config.rgu_start_date is None:
        logging.warning(
            f"RGU update: no RGU start date for cluster {cluster_config.name}"
        )
        return df

    if cluster_config.gpu_to_rgu_billing is None:
        logging.warning(
            f"RGU update: no RGU/GPU JSON path for cluster {cluster_config.name}"
        )
        return df

    if not os.path.isfile(cluster_config.gpu_to_rgu_billing):
        logging.warning(
            f"RGU update: RGU/GPU JSON file not found for cluster {cluster_config.name} "
            f"at: {cluster_config.gpu_to_rgu_billing}"
        )
        return df

    # Otherwise, parse RGU start date.
    rgu_start_date = datetime.fromisoformat(cluster_config.rgu_start_date).astimezone(
        MTL
    )

    # Get RGU/GPU ratios.
    with open(cluster_config.gpu_to_rgu_billing, "r", encoding="utf-8") as file:
        gpu_to_rgu_billing = json.load(file)
        assert isinstance(gpu_to_rgu_billing, dict)
    if not gpu_to_rgu_billing:
        logging.warning(
            f"RGU update: no RGU/GPU available for cluster {cluster_config.name}"
        )
        return df

    # We have now both RGU stare date and RGU/GPU ratios. We can update columns.

    # Compute column allocated.gpu_type_rgu
    # If a GPU type is not found in RGU/GPU ratios,
    # then ratio will be set to NaN in output column.
    col_ratio_rgu_by_gpu = df["allocated.gpu_type"].map(gpu_to_rgu_billing)

    # Compute slices for both before and since RGU start date.
    slice_before_rgu_time = (df["cluster_name"] == cluster_config.name) & (
        df["start_time"] < rgu_start_date
    )
    slice_after_rgu_time = (df["cluster_name"] == cluster_config.name) & (
        df["start_time"] >= rgu_start_date
    )

    # We can already set column allocated.gpu_type_rgu anyway.
    df.loc[slice_before_rgu_time, "allocated.gpu_type_rgu"] = col_ratio_rgu_by_gpu[
        slice_before_rgu_time
    ]
    df.loc[slice_after_rgu_time, "allocated.gpu_type_rgu"] = col_ratio_rgu_by_gpu[
        slice_after_rgu_time
    ]

    # Compute allocated.gres_rgu where job started before RGU time.
    df.loc[slice_before_rgu_time, "allocated.gres_rgu"] = (
        df["allocated.gres_gpu"][slice_before_rgu_time]
        * col_ratio_rgu_by_gpu[slice_before_rgu_time]
    )

    # Set allocated.gres_rgu with previous allocated.gres_gpu where job started after RGU time.
    df.loc[slice_after_rgu_time, "allocated.gres_rgu"] = df["allocated.gres_gpu"][
        slice_after_rgu_time
    ]
    # Then update allocated.gres_gpu where job started after RGU time.
    df.loc[slice_after_rgu_time, "allocated.gres_gpu"] = (
        df["allocated.gres_gpu"][slice_after_rgu_time]
        / col_ratio_rgu_by_gpu[slice_after_rgu_time]
    )

    return df


def update_job_series_rgu(df: DataFrame):
    """
    Compute RGU information for jobs in given data frame.

    Parameters
    ----------
    df: DataFrame
        Data frame to update, typically returned by `load_job_series`.
        Should contain fields:
         "cluster_name", "start_time", "allocated.gpu_type", "allocated.gres_gpu".

    Returns
    -------
    DataFrame
        Input data frame with:
        - column `allocated.gres_gpu` updated if necessary.
        - column `allocated.gres_rgu` added or updated to contain RGU billing.
          Set to NaN (or unchanged if already present) for jobs from clusters without RGU.
        - column `gpu_type_rgu` added or updated to contain RGU cost per GPU (RGU/GPU ratio).
          Set to NaN (or unchanged if already present) for jobs from clusters without RGU.

    For more details about implementation, see function `update_cluster_job_series_rgu`
    """
    for cluster_config in config().clusters.values():
        update_cluster_job_series_rgu(df, cluster_config)
    return df


def _select_stat(name, dist):
    if not dist:
        return np.nan

    if name in ["system_memory", "gpu_memory"]:
        return dist["max"]

    return dist["median"]


slurm_job_metric_names = [
    "slurm_job_core_usage",
    "slurm_job_core_usage_total",
    "slurm_job_fp16_gpu",
    "slurm_job_fp32_gpu",
    "slurm_job_fp64_gpu",
    "slurm_job_memory_active_file",
    "slurm_job_memory_cache",
    "slurm_job_memory_inactive_file",
    "slurm_job_memory_limit",
    "slurm_job_memory_mapped_file",
    "slurm_job_memory_max",
    "slurm_job_memory_rss",
    "slurm_job_memory_rss_huge",
    "slurm_job_memory_unevictable",
    "slurm_job_memory_usage",
    "slurm_job_memory_usage_gpu",
    "slurm_job_nvlink_gpu",
    "slurm_job_nvlink_gpu_total",
    "slurm_job_pcie_gpu",
    "slurm_job_pcie_gpu_total",
    "slurm_job_power_gpu",
    "slurm_job_process_count",
    "slurm_job_sm_occupancy_gpu",
    "slurm_job_states",
    "slurm_job_tensor_gpu",
    "slurm_job_threads_count",
    "slurm_job_utilization_gpu",
    "slurm_job_utilization_gpu_memory",
]


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
