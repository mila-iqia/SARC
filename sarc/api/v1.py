from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status

from sarc.client.job import SlurmJob, SlurmState
from sarc.client.job import get_job as _get_job
from sarc.config import config

from .auth import PermsType

router = APIRouter(prefix="/v1")


class JobQuery:
    def __init__(
        self,
        cluster: str | None = None,
        job_id: int | list[int] | None = None,
        job_state: str | SlurmState | None = None,
        username: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ):
        conf = config()
        if cluster not in conf.clusters.keys():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No such cluster '{cluster}'",
            )
        self.cluster = cluster
        self.job_id = job_id
        if job_state is not None:
            try:
                self.job_state = SlurmState(job_state)
            except ValueError:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid slurm state '{job_state}'",
                )
        else:
            self.job_state = None
        self.username = username
        self.start = start
        self.end = end


JobQueryType = Annotated[JobQuery, Depends(JobQuery)]


@router.get("/job/{job_id}")
def get_job(job_id: int, perms: PermsType) -> SlurmJob:
    job = _get_job(query_options={"job_id": job_id})
    if job is None or job.user not in perms.detail.get(job.cluster_name, []):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
    return job


@router.get("/jobs/count")
def get_job_count(perms: PermsType, query_opt: JobQueryType) -> int:
    # TODO
    return 0
