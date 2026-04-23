from __future__ import annotations

import bisect
import math
from datetime import datetime, timedelta
from typing import Any, Iterable

from pydantic_mongo import AbstractRepository, PydanticObjectId

from sarc.client.gpumetrics import get_cluster_gpu_billings, get_rgus
from sarc.config import UTC, ClusterConfig, config, scraping_mode_required
from sarc.core.models.cluster import SlurmCluster as SlurmClusterBase
from sarc.core.models.job import SlurmJob as SlurmJobBase
from sarc.core.models.job import SlurmState


class SlurmJob(SlurmJobBase):
    """Holds data for a Slurm job."""

    # Database ID
    id: PydanticObjectId | None = None

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
            end_time = datetime.now(tz=UTC)
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
        job_state: Job state to filter on.
        user: User to filter on.
        start: Get all jobs that have a status after that time.
            If str, parsed as a day (YYYY-MM-DD) at 00:00 local timezone.
        end: Get all jobs that have a status before that time.
            If str, parsed as a day (YYYY-MM-DD) at 00:00 local timezone.
    """
    cluster_name = cluster
    if isinstance(cluster, ClusterConfig):
        cluster_name = cluster.name

    if isinstance(start, str):
        start = datetime.strptime(start, "%Y-%m-%d").astimezone()
    if isinstance(end, str):
        end = datetime.strptime(end, "%Y-%m-%d").astimezone()

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
            "$or": [{**query, "end_time": None}, {**query, "end_time": {"$gt": start}}]
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
        job_state: Job state to filter on.
        user: User to filter on.
        start: Get all jobs that have a status after that time.
            If str, parsed as a day (YYYY-MM-DD) at 00:00 local timezone.
        end: Get all jobs that have a status before that time.
            If str, parsed as a day (YYYY-MM-DD) at 00:00 local timezone.
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
        job_state: Job state to filter on.
        user: User to filter on.
        start: Get all jobs that have a status after that time.
            If str, parsed as a day (YYYY-MM-DD) at 00:00 local timezone.
        end: Get all jobs that have a status before that time.
            If str, parsed as a day (YYYY-MM-DD) at 00:00 local timezone.
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


class SlurmCluster(SlurmClusterBase):
    """Hold data for a Slurm cluster."""

    # Database ID
    id: PydanticObjectId | None = None


class SlurmClusterRepository(AbstractRepository[SlurmCluster]):
    class Meta:
        collection_name = "clusters"


def get_available_clusters() -> Iterable[SlurmCluster]:
    """Get clusters available in database."""
    db = config().mongo.database_instance
    return SlurmClusterRepository(database=db).find_by({})
