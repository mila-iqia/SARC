from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status

from sarc.client.job import SlurmJob, SlurmState
from sarc.client.job import get_job as _get_job
from sarc.client.job import get_jobs as _get_jobs
from sarc.config import config

router = APIRouter(prefix="/v0")


class JobQuery:
    cluster: str | None
    job_id: int | list[int] | None
    job_state: SlurmState | None
    username: str | None
    start: datetime | None
    end: datetime | None

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


@router.get("/job/query")
def get_jobs(query_opt: JobQueryType) -> list[SlurmJob]:
    jobs = _get_jobs(
        cluster=query_opt.cluster,
        job_id=query_opt.job_id,
        job_state=query_opt.job_state,
        user=query_opt.username,
        start=query_opt.start,
        end=query_opt.end,
    )
    return list(jobs)


@router.get("/job/{job_id}")
def get_job(job_id: int) -> SlurmJob:
    job = _get_job(query_options={"id": job_id})
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Job not found"
        )
    return job
