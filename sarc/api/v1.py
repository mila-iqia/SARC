from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status

from sarc.client.job import SlurmJob
from sarc.client.job import get_job as _get_job

from .auth import Permissions, get_permissions

router = APIRouter(prefix="/v1")


@router.get("/job/{job_id}")
def get_job(
    job_id: int, perms: Annotated[Permissions, Depends(get_permissions)]
) -> SlurmJob:
    job = _get_job(query_options={"job_id": job_id})
    if job is None or job.user not in perms.detail.get(job.cluster_name, []):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
    return job
