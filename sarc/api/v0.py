from datetime import datetime
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import ORJSONResponse
from pydantic import UUID4, BaseModel
from pydantic_mongo import PydanticObjectId

from sarc.client import get_rgus
from sarc.client.job import (
    SlurmJob,
    SlurmState,
    _async_clusters_collection,
    _async_jobs_collection,
    async_get_available_clusters,
)
from sarc.config import UTC
from sarc.core.models import validators
from sarc.core.models.users import MemberType, UserData
from sarc.users.db import get_async_user_collection

# Use `orjson` module to handle JSON.
# `orjson` automatically converts float NaN values to None,
# and is expected to be faster than builtin `json`.
router = APIRouter(prefix="/v0", default_response_class=ORJSONResponse)

DEFAULT_PAGE_SIZE = 100
MAX_PAGE_SIZE = 5_000


async def validate_cluster(cluster: str):
    cluster_names = list(cl.cluster_name for cl in await async_get_available_clusters())
    if cluster is not None and cluster not in cluster_names:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=f"No such cluster '{cluster}'"
        )


class JobQuery(BaseModel):
    cluster: str | None = None
    # job_id supports None, an integer, a list of integers, or an empty list.
    job_id: list[int] | None = None
    job_state: SlurmState | None = None
    username: str | None = None
    start: datetime | None = None
    end: datetime | None = None

    def get_query(self) -> dict[str, Any]:
        query: dict[str, Any] = {}
        if self.cluster is not None:
            query["cluster_name"] = self.cluster
        if self.job_id is not None:
            if len(self.job_id) == 1:
                query["job_id"] = self.job_id[0]
            else:
                query["job_id"] = {"$in": self.job_id}
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


async def job_query_params(
    cluster: str | None = None,
    job_id: Annotated[list[str] | None, Query()] = None,
    job_state: SlurmState | None = None,
    username: str | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
) -> JobQuery:
    """
    Annotation function for JobQueryType below.
    Annotates `job_id` with `Query()` annotation,
    so that Pydantic/FastAPI correctly parses `job_id`.

    We use `list[str]` to support empty lists (sent as `job_id=`).
    We then convert to `list[int]` to match `JobQuery` model.
    """
    await validate_cluster(cluster)

    job_id_ints = None
    if job_id is not None:
        try:
            job_id_ints = [int(jid) for jid in job_id if jid]
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                detail="job_id must be a list of integers",
            )

    return JobQuery(
        cluster=cluster,
        job_id=job_id_ints,
        job_state=job_state,
        username=username,
        start=start,
        end=end,
    )


JobQueryType = Annotated[JobQuery, Depends(job_query_params)]


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


class SlurmJobList(BaseModel):
    jobs: list[SlurmJob]
    page: int
    per_page: int
    total: int


class UserList(BaseModel):
    users: list[UserData]
    page: int
    per_page: int
    total: int


@router.get("/job/query")
async def get_jobs(query_opt: JobQueryType) -> list[PydanticObjectId]:
    coll = _async_jobs_collection()
    jobs = coll.get_collection().find(query_opt.get_query(), ["_id"])
    return [j["_id"] async for j in jobs]


@router.get("/job/list")
async def list_jobs(
    query_opt: JobQueryType, page: int = 1, per_page: int = DEFAULT_PAGE_SIZE
) -> SlurmJobList:
    if page < 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Page must be >= 1"
        )
    if per_page < 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Page size must be >= 1"
        )
    if per_page > MAX_PAGE_SIZE:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Page size must be <= {MAX_PAGE_SIZE}",
        )

    coll = _async_jobs_collection()
    query = query_opt.get_query()
    total = await coll.get_collection().count_documents(query)

    cursor = (
        coll.get_collection()
        .find(query, allow_disk_use=True)
        .sort([("submit_time", -1), ("_id", 1)])
        .skip((page - 1) * per_page)
        .limit(per_page)
    )

    return SlurmJobList(
        jobs=[SlurmJob.model_validate(doc) async for doc in cursor],
        page=page,
        per_page=per_page,
        total=total,
    )


@router.get("/job/count")
async def count_jobs(query_opt: JobQueryType) -> int:
    coll = _async_jobs_collection()
    return await coll.get_collection().count_documents(query_opt.get_query())


@router.get("/job/id/{oid}")
async def get_job(oid: PydanticObjectId) -> SlurmJob:
    coll = _async_jobs_collection()
    job = await coll.find_one_by_id(oid)
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Job not found"
        )
    return job


@router.get("/cluster/list")
async def get_cluster_names() -> list[str]:
    """Return the names of available clusters."""
    coll = _async_clusters_collection()
    return sorted([cl.cluster_name for cl in await coll.find_by({})])


@router.get("/gpu/rgu")
async def get_rgu_value_per_gpu() -> dict[str, float]:
    """Return the mapping GPU->RGU."""
    return get_rgus()


@router.get("/user/query")
async def query_users(query_opt: UserQueryType) -> list[UUID4]:
    """Search users. Return user UUIDs."""
    coll = get_async_user_collection()
    users = coll.get_collection().find(query_opt.get_query(), ["uuid"])
    return [user["uuid"] async for user in users]


@router.get("/user/list")
async def list_users(
    query_opt: UserQueryType, page: int = 1, per_page: int = DEFAULT_PAGE_SIZE
) -> UserList:
    if page < 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Page must be >= 1"
        )
    if per_page < 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Page size must be >= 1"
        )
    if per_page > MAX_PAGE_SIZE:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Page size must be <= {MAX_PAGE_SIZE}",
        )

    coll = get_async_user_collection()
    query = query_opt.get_query()
    total = await coll.get_collection().count_documents(query)

    cursor = (
        coll.get_collection()
        .find(query, allow_disk_use=True)
        .sort([("email", 1), ("uuid", 1)])
        .skip((page - 1) * per_page)
        .limit(per_page)
    )

    return UserList(
        users=[UserData.model_validate(doc) async for doc in cursor],
        page=page,
        per_page=per_page,
        total=total,
    )


@router.get("/user/id/{uuid}")
async def get_user_by_id(uuid: UUID4) -> UserData:
    """Get user with given UUID."""
    user = await get_async_user_collection().find_one_by({"uuid": uuid})
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="User not found"
        )
    return user


@router.get("/user/email/{email}")
async def get_user_by_email(email: str) -> UserData:
    """Get user with given email."""
    user = await get_async_user_collection().find_one_by({"email": email})
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="User not found"
        )
    return user
