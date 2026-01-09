from __future__ import annotations

import bisect
import math
from collections.abc import Sequence
from datetime import datetime, time, timedelta
from enum import Enum
from typing import Any, Iterable, Literal, overload

from pandas import DataFrame
from pydantic import field_validator
from pydantic_mongo import AbstractRepository, PydanticObjectId

from sarc.client.gpumetrics import get_rgus, get_cluster_gpu_billings
from sarc.config import MTL, TZLOCAL, UTC, ClusterConfig, config, scraping_mode_required
from sarc.model import BaseModel
from sarc.traces import trace_decorator


class SlurmState(str, Enum):
    """Possible Slurm job states.

    Reference: https://slurm.schedmd.com/squeue.html#SECTION_JOB-STATE-CODES
    """

    BOOT_FAIL = "BOOT_FAIL"
    CANCELLED = "CANCELLED"
    COMPLETED = "COMPLETED"
    CONFIGURING = "CONFIGURING"
    COMPLETING = "COMPLETING"
    DEADLINE = "DEADLINE"
    FAILED = "FAILED"
    NODE_FAIL = "NODE_FAIL"
    OUT_OF_MEMORY = "OUT_OF_MEMORY"
    PENDING = "PENDING"
    PREEMPTED = "PREEMPTED"
    RUNNING = "RUNNING"
    RESV_DEL_HOLD = "RESV_DEL_HOLD"
    REQUEUE_FED = "REQUEUE_FED"
    REQUEUE_HOLD = "REQUEUE_HOLD"
    REQUEUED = "REQUEUED"
    RESIZING = "RESIZING"
    REVOKED = "REVOKED"
    SIGNALING = "SIGNALING"
    SPECIAL_EXIT = "SPECIAL_EXIT"
    STAGE_OUT = "STAGE_OUT"
    STOPPED = "STOPPED"
    SUSPENDED = "SUSPENDED"
    TIMEOUT = "TIMEOUT"


class Statistics(BaseModel):
    """Statistics for a timeseries."""

    mean: float
    std: float
    q05: float
    q25: float
    median: float
    q75: float
    max: float
    unused: int


class JobStatistics(BaseModel):
    """Statistics for a job."""

    gpu_utilization: Statistics | None = None
    gpu_utilization_fp16: Statistics | None = None
    gpu_utilization_fp32: Statistics | None = None
    gpu_utilization_fp64: Statistics | None = None
    gpu_sm_occupancy: Statistics | None = None
    gpu_memory: Statistics | None = None
    gpu_power: Statistics | None = None

    cpu_utilization: Statistics | None = None
    system_memory: Statistics | None = None

    def empty(self) -> bool:
        return (
            self.gpu_utilization is None
            and self.gpu_utilization_fp16 is None
            and self.gpu_utilization_fp32 is None
            and self.gpu_utilization_fp64 is None
            and self.gpu_sm_occupancy is None
            and self.gpu_memory is None
            and self.gpu_power is None
            and self.cpu_utilization is None
            and self.system_memory is None
        )


class SlurmResources(BaseModel):
    """Counts for various resources."""

    cpu: int | None = None
    mem: int | None = None
    node: int | None = None
    billing: int | None = None
    gres_gpu: int | None = None
    gpu_type: str | None = None


class SlurmJob(BaseModel):
    """Holds data for a Slurm job."""

    # Database ID
    id: PydanticObjectId | None = None

    # job identification
    cluster_name: str
    account: str
    job_id: int
    array_job_id: int | None = None
    task_id: int | None = None
    name: str
    user: str
    group: str

    # status
    job_state: SlurmState
    exit_code: int | None = None
    signal: int | None = None

    # allocation information
    partition: str
    nodes: list[str]
    work_dir: str

    # Miscellaneous
    constraints: str | None = None
    priority: int | None = None
    qos: str | None = None

    # Flags
    CLEAR_SCHEDULING: bool = False
    STARTED_ON_SUBMIT: bool = False
    STARTED_ON_SCHEDULE: bool = False
    STARTED_ON_BACKFILL: bool = False

    # temporal fields
    time_limit: int | None = None
    submit_time: datetime
    start_time: datetime | None = None
    end_time: datetime | None = None
    elapsed_time: float
    # Latest period the job was scraped with sacct
    latest_scraped_start: datetime | None = None
    latest_scraped_end: datetime | None = None

    # tres
    requested: SlurmResources
    allocated: SlurmResources

    # statistics
    stored_statistics: JobStatistics | None = None

    @field_validator(
        "submit_time",
        "start_time",
        "end_time",
        "latest_scraped_start",
        "latest_scraped_end",
    )
    @classmethod
    def _ensure_timezone(cls, v: datetime | None) -> datetime | None:
        # We'll store in MTL timezone because why not
        # TODO: Could we instead just store it in UTC ?
        return v and v.replace(tzinfo=UTC).astimezone(MTL)

    @property
    def duration(self) -> timedelta:
        if self.end_time is not None:
            assert self.start_time is not None
            return self.end_time - self.start_time

        return timedelta(seconds=0)

    @overload
    def series(
        self,
        metric: str | Sequence[str],
        min_interval: int = 30,
        max_points: int = 100,
        measure: str | None = None,
        aggregation: Literal["total", "interval"] | None = "total",
        dataframe: Literal[True] = True,
    ) -> DataFrame | None: ...

    @overload
    def series(
        self,
        metric: str | Sequence[str],
        min_interval: int = 30,
        max_points: int = 100,
        measure: str | None = None,
        aggregation: Literal["total", "interval"] | None = "total",
        dataframe: Literal[False] = False,
    ) -> list | None: ...

    @scraping_mode_required
    def series(
        self,
        metric: str | Sequence[str],
        min_interval: int = 30,
        max_points: int = 100,
        measure: str | None = None,
        aggregation: Literal["total", "interval"] | None = "total",
        dataframe: bool = True,
    ) -> DataFrame | list | None:
        from sarc.jobs.series import get_job_time_series

        return get_job_time_series(
            job=self,
            metric=metric,
            min_interval=min_interval,
            max_points=max_points,
            measure=measure,
            aggregation=aggregation,
            dataframe=dataframe,
        )  # type: ignore[call-overload]

    @trace_decorator()
    @scraping_mode_required
    def statistics(
        self,
        recompute: bool = False,
        save: bool = True,
        overwrite_when_empty: bool = False,
    ) -> JobStatistics | None:
        from sarc.jobs.series import compute_job_statistics

        if self.stored_statistics is not None and not recompute:
            return self.stored_statistics
        elif (
            self.end_time is not None
            and self.fetch_cluster_config().prometheus_url is not None
        ):
            statistics = compute_job_statistics(self)
            if save and (
                overwrite_when_empty
                or not self.stored_statistics
                or not statistics.empty()
            ):
                self.stored_statistics = statistics
                self.save()
            return statistics

        return None

    @scraping_mode_required
    def save(self):
        _jobs_collection().save_job(self)

    @scraping_mode_required
    def fetch_cluster_config(self) -> ClusterConfig:
        """This function is only available on the admin side"""
        return config("scraping").clusters[self.cluster_name]

    @property
    def gpu_type_rgu(self) -> float:
        """Get RGU value for the GPU type of this job, or NaN if not applicable."""
        gpu_type = self.allocated.gpu_type
        if gpu_type is None:
            return math.nan
        else:
            gpu_to_rgu = get_rgus()
            # NB: If GPU type is a MIG
            # (e.g: "A100-SXM4-40GB : a100_1g.5gb"),
            # we currently return RGU for the main GPU type
            # (in this example: "A100-SXM4-40GB")
            return gpu_to_rgu.get(gpu_type.split(":")[0].rstrip(), math.nan)

    @property
    def rgu(self) -> float:
        """
        Get RGU billing for this job, or NaN if not applicable.
        Same algorithm as in series functions
        load_job_series() and update_job_series_rgu().

        RGU billing for a job is equivalent to:
        Number of GPUs used by this job
        x
        RGU value for a single GPU (self.gpu_type_rgu)
        """
        end_time = self.end_time
        if end_time is None:
            end_time = datetime.now(tz=TZLOCAL)
        start_time = end_time - timedelta(seconds=self.elapsed_time)
        gpu_type = self.allocated.gpu_type
        if start_time is None or gpu_type is None:
            return math.nan

        billing = self.allocated.billing or 0
        gres_gpu = self.requested.gres_gpu or 0
        if gres_gpu:
            gres_gpu = max(billing, gres_gpu)

        gpu_type_rgu = self.gpu_type_rgu
        # Use get_available_clusters() instead of self.fetch_cluster_config()
        # so that this code can be executed even in client mode
        (cluster,) = [
            cluster
            for cluster in get_available_clusters()
            if cluster.cluster_name == self.cluster_name
        ]
        if cluster.billing_is_gpu:
            # Compute RGU from gpu count
            gpu_count = gres_gpu
            gres_rgu = gpu_count * gpu_type_rgu
        else:
            # Job billing is in its own unit.
            # We must first infer gpu count
            # before computing RGU
            all_cluster_billings = get_cluster_gpu_billings(
                cluster_name=cluster.cluster_name
            )
            if not all_cluster_billings:
                # No gpu->billing mapping available, cannot compute RGU
                gres_rgu = math.nan
            elif start_time < all_cluster_billings[0].since:
                # Before the oldest gpu->billing mapping available
                # We assume gres_gpu is gpu count
                gpu_count = gres_gpu
                gres_rgu = gpu_count * gpu_type_rgu
            else:
                # gpu->billing mappings available
                # Find mapping for this job, based on start_time
                index_billing = max(
                    0,
                    bisect.bisect_right(
                        [billing.since for billing in all_cluster_billings], start_time
                    )
                    - 1,
                )
                cluster_billing = all_cluster_billings[index_billing]
                # Then find billing for this job GPU type
                gpu_billing = cluster_billing.gpu_to_billing.get(gpu_type, math.nan)
                # gres_gpu is job billing
                job_billing = gres_gpu
                # So, gpu count == job billing / gpu billing
                gres_rgu = (job_billing / gpu_billing) * gpu_type_rgu
        return gres_rgu


class SlurmJobRepository(AbstractRepository[SlurmJob]):
    class Meta:
        collection_name = "jobs"

    @scraping_mode_required
    def save_job(self, model: SlurmJob) -> None:
        """Save a SlurmJob into the database.

        Note: This overrides AbstractRepository's save function to do an upsert when
        the id is provided.
        """
        document = self.to_document(model)
        # Resubmitted jobs have the same job ID can be distinguished by their submit time,
        # as per sacct's documentation.
        self.get_collection().update_one(
            {
                "job_id": model.job_id,
                "cluster_name": model.cluster_name,
                "submit_time": model.submit_time,
            },
            {"$set": document},
            upsert=True,
        )


def _jobs_collection() -> SlurmJobRepository:
    """Return the jobs collection in the current MongoDB."""
    db = config().mongo.database_instance
    return SlurmJobRepository(database=db)


# pylint: disable=too-many-branches,dangerous-default-value
def _compute_jobs_query(
    *,
    cluster: str | ClusterConfig | None = None,
    job_id: int | list[int] | None = None,
    job_state: str | SlurmState | None = None,
    user: str | None = None,
    start: str | datetime | None = None,
    end: str | datetime | None = None,
) -> dict:
    """Compute the MongoDB query dict to be used to match given arguments.

    Arguments:
        cluster: The cluster on which to search for jobs.
        job_id: The id or a list of ids to select.
        start: Get all jobs that have a status after that time.
        end: Get all jobs that have a status before that time.
        query_options: Additional options to pass to MongoDB (limit, etc.)
    """
    cluster_name = cluster
    if isinstance(cluster, ClusterConfig):
        cluster_name = cluster.name

    if isinstance(start, str):
        start = datetime.combine(
            datetime.strptime(start, "%Y-%m-%d"), time.min
        ).replace(tzinfo=TZLOCAL)
    if isinstance(end, str):
        end = (datetime.combine(datetime.strptime(end, "%Y-%m-%d"), time.min)).replace(
            tzinfo=TZLOCAL
        )

    if start is not None:
        start = start.astimezone(UTC)
    if end is not None:
        end = end.astimezone(UTC)

    query: dict[str, Any] = {}
    if cluster_name:
        query["cluster_name"] = cluster_name

    if isinstance(job_id, int):
        query["job_id"] = job_id
    elif isinstance(job_id, list):
        query["job_id"] = {"$in": job_id}
    elif job_id is not None:
        raise TypeError(f"job_id must be an int or a list of ints: {job_id}")

    if end:
        # Select any job that had a status before the given end time.
        query["submit_time"] = {"$lt": end}

    if user:
        query["user"] = user

    if job_state:
        query["job_state"] = job_state

    if start:
        # Select jobs that had a status after the given time. This is a bit special
        # since we need to get both jobs that did not finish, and any job that ended after
        # the given time. This appears to require an $or, so we handle it after the others.
        query = {
            "$or": [
                {**query, "end_time": None},
                {**query, "end_time": {"$gt": start}},
            ]
        }

    return query


def count_jobs(
    *,
    cluster: str | ClusterConfig | None = None,
    job_id: int | list[int] | None = None,
    job_state: str | SlurmState | None = None,
    user: str | None = None,
    start: str | datetime | None = None,
    end: str | datetime | None = None,
    query_options: dict | None = None,
) -> int:
    """Count jobs that match the query.

    Arguments:
        cluster: The cluster on which to search for jobs.
        job_id: The id or a list of ids to select.
        start: Get all jobs that have a status after that time.
        end: Get all jobs that have a status before that time.
        query_options: Additional options to pass to MongoDB (limit, etc.)
    """
    query = _compute_jobs_query(
        cluster=cluster,
        job_id=job_id,
        job_state=job_state,
        user=user,
        start=start,
        end=end,
    )
    if query_options is None:
        query_options = {}
    return config().mongo.database_instance.jobs.count_documents(query, **query_options)


def get_jobs(
    *,
    cluster: str | ClusterConfig | None = None,
    job_id: int | list[int] | None = None,
    job_state: str | SlurmState | None = None,
    user: str | None = None,
    start: str | datetime | None = None,
    end: str | datetime | None = None,
    query_options: dict | None = None,
) -> Iterable[SlurmJob]:
    """Get jobs that match the query.

    Arguments:
        cluster: The cluster on which to search for jobs.
        job_id: The id or a list of ids to select.
        start: Get all jobs that have a status after that time.
        end: Get all jobs that have a status before that time.
        query_options: Additional options to pass to MongoDB (limit, etc.)
    """
    if query_options is None:
        query_options = {}

    query = _compute_jobs_query(
        cluster=cluster,
        job_id=job_id,
        job_state=job_state,
        user=user,
        start=start,
        end=end,
    )

    coll = _jobs_collection()

    return coll.find_by(query, **query_options)


# pylint: disable=dangerous-default-value
def get_job(*, query_options: dict = {}, **kwargs) -> SlurmJob | None:
    """Get a single job that matches the query, or None if nothing is found.

    Same signature as `get_jobs`.
    """
    # Sort by submit_time descending, which ensures we get the most recent version
    # of the job.
    jobs = get_jobs(
        **kwargs,
        query_options={**query_options, "sort": [("submit_time", -1)], "limit": 1},
    )
    for job in jobs:
        return job
    return None


class SlurmCLuster(BaseModel):
    """Hold data for a Slurm cluster."""

    # Database ID
    id: PydanticObjectId | None = None

    cluster_name: str
    start_date: str | None = None
    end_time_sacct: str | None = None
    end_time_prometheus: str | None = None
    billing_is_gpu: bool = False


class SlurmClusterRepository(AbstractRepository[SlurmCLuster]):
    class Meta:
        collection_name = "clusters"


def get_available_clusters() -> Iterable[SlurmCLuster]:
    """Get clusters available in database."""
    db = config().mongo.database_instance
    return SlurmClusterRepository(database=db).find_by({})
