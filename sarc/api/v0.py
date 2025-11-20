from datetime import datetime
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import AfterValidator, BaseModel, UUID4
from pydantic_mongo import PydanticObjectId

from sarc.client import get_rgus
from sarc.client.job import (
    SlurmJob,
    SlurmState,
    _jobs_collection,
    get_available_clusters,
)
from sarc.core.models.users import MemberType, UserData
from sarc.users.db import get_user_collection

router = APIRouter(prefix="/v0")


def valid_cluster(cluster: str):
    cluster_names = list(cl.cluster_name for cl in get_available_clusters())
    if cluster is not None:
        if cluster not in cluster_names:
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


class UserQuery(BaseModel):
    display_name: str | None = None
    email: str | None = None
    member_type: MemberType | None = None
    supervisor: UUID4 | None = None
    co_supervisor: UUID4 | None = None

    def get_query(self) -> dict[str, Any]:
        query: dict[str, Any] = {}
        if self.display_name is not None:
            query["display_name"] = {"$regex": self.display_name, "$options": "i"}
        if self.email is not None:
            query["email"] = self.email
        if self.member_type is not None:
            query["member_type.values.value"] = self.member_type
        if self.supervisor is not None:
            query["supervisor.values.value"] = self.supervisor
        if self.co_supervisor is not None:
            query["co_supervisors.values.value"] = self.co_supervisor
        return query


UserQueryType = Annotated[UserQuery, Depends(UserQuery)]


@router.get("/job/query")
def get_jobs(query_opt: JobQueryType) -> list[PydanticObjectId]:
    coll = _jobs_collection()
    jobs = coll.get_collection().find(query_opt.get_query(), ["_id"])
    return list(j["_id"] for j in jobs)


@router.get("/job/count")
def count_jobs(query_opt: JobQueryType) -> int:
    coll = _jobs_collection()
    return coll.get_collection().count_documents(query_opt.get_query())


@router.get("/job/id/{oid}")
def get_job(oid: PydanticObjectId) -> SlurmJob:
    coll = _jobs_collection()
    job = coll.find_one_by_id(oid)
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Job not found"
        )
    return job


@router.get("/cluster/list")
def get_cluster_names() -> list[str]:
    """Return the names of available clusters."""
    return sorted(cl.cluster_name for cl in get_available_clusters())


@router.get("/gpu/rgu")
def get_rgu_value_per_gpu() -> dict[str, float]:
    """Return the mapping GPU->RGU."""
    return get_rgus()


@router.get("/user/query")
def query_users(query_opt: UserQueryType) -> list[UUID4]:
    """Search users. Return user UUIDs."""
    coll = get_user_collection()
    users = coll.get_collection().find(query_opt.get_query(), ["uuid"])
    return [user["uuid"] for user in users]


@router.get("/user/id/{uuid}")
def get_user_by_id(uuid: UUID4) -> UserData:
    """Get user with given UUID."""
    user = get_user_collection().find_one_by({"uuid": uuid})
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="User not found"
        )
    return user


@router.get("/user/email/{email}")
def get_user_by_email(email: str) -> UserData:
    """Get user with given email."""
    user = get_user_collection().find_one_by({"email": email})
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="User not found"
        )
    return user
