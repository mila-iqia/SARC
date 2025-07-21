from datetime import datetime
from typing import Annotated, Any

from bson import ObjectId
from bson.errors import InvalidId
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import AfterValidator, BaseModel

from sarc.client.job import SlurmJob, SlurmState, _jobs_collection
from sarc.config import config

router = APIRouter(prefix="/v0")


def valid_cluster(cluster: str):
    conf = config("scraping")
    if cluster is not None:
        if cluster not in conf.clusters.keys():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No such cluster '{cluster}'",
            )
    return cluster


class JobQuery(BaseModel):
    cluster: Annotated[str, AfterValidator(valid_cluster)] | None = None
    job_id: int | None = None
    job_state: SlurmState | None = None
    username: str | None = None
    start: datetime | None = None
    end: datetime | None = None

    def get_query(self) -> dict[str, Any]:
        query: dict[str, Any] = {}
        if self.cluster is not None:
            query["cluster_name"] = self.cluster
        if self.job_id is not None:
            if isinstance(self.job_id, list):
                query["job_id"] = {"$in": self.job_id}
            else:
                query["job_id"] = self.job_id
        if self.job_state is not None:
            query["job_state"] = self.job_state
        if self.username is not None:
            query["user"] = self.username
        if self.end is not None:
            query["submit_time"] = {"$lt": self.end}
        if self.start is not None:
            query = {
                "$or": [
                    {**query, "end_time": None},
                    {**query, "end_time": {"$gt": self.start}},
                ]
            }
        return query


JobQueryType = Annotated[JobQuery, Depends(JobQuery)]


@router.get("/job/query")
def get_jobs(query_opt: JobQueryType) -> list[SlurmJob]:
    coll = _jobs_collection()
    jobs = coll.find_by(query=query_opt.get_query())
    return list(jobs)


@router.get("/job/id/{oid}")
def get_job(oid: str) -> SlurmJob:
    try:
        boid = ObjectId(oid)
    except InvalidId as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Invalid object id"
        ) from e
    coll = _jobs_collection()
    job = coll.find_one_by_id(boid)
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Job not found"
        )
    return job
