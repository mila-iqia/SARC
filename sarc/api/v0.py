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
from sarc.config import UTC
from sarc.core.models import validators
from sarc.core.models.users import MemberType, UserData
from sarc.users.db import get_user_collection

router = APIRouter(prefix="/v0")

PAGE_SIZE = 100


class SlurmJobList(BaseModel):
    jobs: list[SlurmJob]
    page: int
    per_page: int
    total: int


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
    job_id: int | list[int] | None = None
    job_state: SlurmState | None = None
    username: str | None = None
    start: datetime | None = None
    end: datetime | None = None

    def get_query(self) -> dict[str, Any]:
        query: dict[str, Any] = {}
        if self.cluster is not None:
            query["cluster_name"] = self.cluster
        if isinstance(self.job_id, int):
            query["job_id"] = self.job_id
        elif isinstance(self.job_id, list) and all(
            isinstance(el, int) for el in self.job_id
        ):
            query["job_id"] = {"$in": self.job_id}
        elif self.job_id is not None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"job_id must be an int or a list of ints: {self.job_id}",
            )
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
    member_start: datetime | None = None
    member_end: datetime | None = None

    supervisor: UUID4 | None = None
    supervisor_start: datetime | None = None
    supervisor_end: datetime | None = None

    co_supervisor: UUID4 | None = None
    co_supervisor_start: datetime | None = None
    co_supervisor_end: datetime | None = None

    def _get_valid_tag_query(
        self,
        value_field: str,
        start_field: str,
        end_field: str,
        field: str | None = None,
    ) -> dict[str, Any]:
        value = getattr(self, value_field)
        start = getattr(self, start_field)
        end = getattr(self, end_field)
        if value is not None:
            now = datetime.now(UTC)
            if start is None and end is None:
                # Look for tag matching current time [now, now]
                start = now
                end = now
            elif end is None:
                # Look for tags that overlap [start, +inf)
                end = validators.END_TIME
            elif start is None:
                # Look for tags that overlap (-inf, end]
                start = validators.START_TIME
            if start > end:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Expected {start_field} <= {end_field}",
                )
            field = field or value_field
            return {
                f"{field}.values": {
                    "$elemMatch": {
                        "value": value,
                        "valid_start": {"$lte": end},
                        "valid_end": {"$gte": start},
                    }
                }
            }
        return {}

    def get_query(self) -> dict[str, Any]:
        query: dict[str, Any] = {}
        if self.display_name is not None:
            query["display_name"] = {"$regex": self.display_name, "$options": "i"}
        if self.email is not None:
            query["email"] = self.email
        query.update(
            self._get_valid_tag_query("member_type", "member_start", "member_end")
        )
        query.update(
            self._get_valid_tag_query(
                "supervisor", "supervisor_start", "supervisor_end"
            )
        )
        query.update(
            self._get_valid_tag_query(
                "co_supervisor",
                "co_supervisor_start",
                "co_supervisor_end",
                field="co_supervisors",
            )
        )
        return query


UserQueryType = Annotated[UserQuery, Depends(UserQuery)]


class UserList(BaseModel):
    users: list[UserData]
    page: int
    per_page: int
    total: int


@router.get("/job/query")
def get_jobs(query_opt: JobQueryType) -> list[PydanticObjectId]:
    coll = _jobs_collection()
    jobs = coll.get_collection().find(query_opt.get_query(), ["_id"])
    return list(j["_id"] for j in jobs)


@router.get("/job/list")
def list_jobs(query_opt: JobQueryType, page: int = 1) -> SlurmJobList:
    if page < 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Page must be >= 1"
        )

    coll = _jobs_collection()
    query = query_opt.get_query()
    total = coll.get_collection().count_documents(query)

    cursor = (
        coll.get_collection()
        .find(query)
        .sort([("submit_time", -1), ("_id", 1)])
        .skip((page - 1) * PAGE_SIZE)
        .limit(PAGE_SIZE)
    )

    return SlurmJobList(
        jobs=[SlurmJob.model_validate(doc) for doc in cursor],
        page=page,
        per_page=PAGE_SIZE,
        total=total,
    )


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


@router.get("/user/list")
def list_users(query_opt: UserQueryType, page: int = 1) -> UserList:
    """List users with details and pagination."""
    if page < 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Page must be >= 1"
        )

    coll = get_user_collection()
    query = query_opt.get_query()
    total = coll.get_collection().count_documents(query)

    cursor = (
        coll.get_collection()
        .find(query)
        .sort([("email", 1), ("uuid", 1)])
        .skip((page - 1) * PAGE_SIZE)
        .limit(PAGE_SIZE)
    )

    return UserList(
        users=[UserData.model_validate(doc) for doc in cursor],
        page=page,
        per_page=PAGE_SIZE,
        total=total,
    )


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
