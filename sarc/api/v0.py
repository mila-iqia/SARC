from collections.abc import Generator
from dataclasses import dataclass
from datetime import datetime
from typing import Annotated, Self

from easy_oauth.cap import Capability
from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import UUID4, AfterValidator, BaseModel, Field
from pydantic.functional_validators import model_validator
from serieux import deserialize
from sqlalchemy.dialects.postgresql import Range
from sqlalchemy.orm import Mapped
from sqlmodel import Session, and_, col, func, or_, select
from sqlmodel.sql.expression import SelectOfScalar

from sarc.alerts.healthcheck_state import (
    HealthCheckState,
    get_healthcheck_state_collection,
)
from sarc.config import UTC, Config, config
from sarc.db.cluster import SlurmClusterDB, get_available_clusters
from sarc.db.job import SlurmJobDB, SlurmState, get_rgus
from sarc.db.users import (
    CoSupervisorDB,
    CoSupervisorsHelper,
    MatchingID,
    MemberType,
    MemberTypeDB,
    SupervisorDB,
    UserDB,
)
from sarc.models.api import SlurmJob, SlurmJobList, User, UserList


def _ensure_datetime_utc(v: datetime) -> datetime:
    """
    Convert a datetime object to UTC timezone.
    Raise an exception if date is naive.
    """
    if v.tzinfo is None:
        raise ValueError(
            "Time-aware datetime required. E.g: 2025-01-01T10:00Z (UTC), 2025-01-01T05:00-05:00 (UTC-5 hours)"
        )
    return v.astimezone(UTC)


datetime_api = Annotated[datetime, AfterValidator(_ensure_datetime_utc)]

router = APIRouter(prefix="/v0")


def config_dep() -> Config:
    # We don't use Depends(config) directly because then the 'mode' argument to
    # config would be added to the API signature, and we don't want that
    return config("scraping")


def session_dep() -> Generator[Session]:
    with config().db.session() as sess:
        yield sess


def hascap(cap):
    async def check(request: Request, cfg: Config = Depends(config_dep)):
        if not cfg.api.auth:
            yield "__admin__"
        else:
            async for name in cfg.api.auth.get_email_capability(cap)(request):
                yield name

    return check


can_query = hascap("query")


@dataclass
class Requestor:
    email: str
    user: UserDB | None = None
    is_admin: bool = False


def is_admin(
    user: Annotated[str, Depends(can_query)], cfg: Config = Depends(config_dep)
) -> bool:
    auth = cfg.api.auth
    if not auth:
        return True
    admin: Capability = deserialize(auth.capabilities.captype, "admin")
    return auth.capabilities.check(user, admin)


def require_admin(admin: bool = Depends(is_admin)):
    if not admin:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN)


def requestor(
    email: str = Depends(can_query),
    admin: bool = Depends(is_admin),
    sess: Session = Depends(session_dep),
):
    userdb = sess.exec(
        select(UserDB)
        .join(MatchingID)
        .where(MatchingID.plugin_name == "mila_ldap", MatchingID.match_id == email)
    ).one_or_none()
    if userdb is None and not admin:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN)
    return Requestor(email=email, user=userdb, is_admin=admin)


def validate_cluster(sess: Session, cluster: str | None):
    cluster_names = sess.exec(select(SlurmClusterDB.name)).all()
    if cluster is not None and cluster not in cluster_names:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=f"No such cluster '{cluster}'"
        )


class PageOptions(BaseModel):
    page: int | None = Field(default=None, ge=1)
    last_id: int | None = None
    per_page: int = Field(default=100, le=100)

    @model_validator(mode="after")
    def only_one(self) -> Self:
        if self.page is not None and self.last_id is not None:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                detail="Use only one of last_id and page",
            )
        return self

    def add_page_options[T](
        self, query: SelectOfScalar[T], id_col: Mapped[int]
    ) -> SelectOfScalar[T]:
        query = query.order_by(id_col).limit(self.per_page)
        if self.page is not None:
            if self.page > 1:
                query = query.offset((self.page - 1) * self.per_page)
        elif self.last_id is not None:
            query = query.where(id_col > self.last_id)
        # Otherwise we are on the first page, so no need for additional things
        return query


class PageOptionsWithTime(BaseModel):
    page: int | None = Field(default=None, ge=1)
    last_id: int | None = None
    last_time: datetime | None = None
    per_page: int = Field(default=100, le=100)

    @model_validator(mode="after")
    def only_one(self) -> Self:
        if self.page is not None and self.last_id is not None:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                detail="Use only one of last_id and page",
            )
        return self

    @model_validator(mode="after")
    def check_id_time(self) -> Self:
        if (self.last_id is not None) ^ (self.last_time is not None):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="You must specify both last_id and last_time or none of them",
            )
        return self

    def add_page_options[T](
        self, query: SelectOfScalar[T], id_col: Mapped[int], time_col: Mapped[datetime]
    ) -> SelectOfScalar[T]:
        query = query.order_by(time_col, id_col).limit(self.per_page)
        if self.page is not None:
            if self.page > 1:
                query = query.offset((self.page - 1) * self.per_page)
        elif self.last_id is not None:
            query = query.where(
                or_(
                    time_col > self.last_time,
                    and_(time_col == self.last_time, id_col > self.last_id),
                )
            )
        # Otherwise we are on the first page, so no need for additional things
        return query


def page_options(
    page: int | None = None, last_id: int | None = None, per_page: int = 100
) -> PageOptions:
    return PageOptions(page=page, last_id=last_id, per_page=per_page)


def page_options_with_time(
    page: int | None = None,
    last_id: int | None = None,
    last_time: datetime | None = None,
    per_page: int = 100,
) -> PageOptionsWithTime:
    return PageOptionsWithTime(
        page=page, last_id=last_id, last_time=last_time, per_page=per_page
    )


PageOptionsType = Annotated[PageOptions, Depends(page_options)]
PageOptionsWithTimeType = Annotated[
    PageOptionsWithTime, Depends(page_options_with_time)
]


class JobQuery(BaseModel):
    cluster: str | None = None
    # job_id supports None, an integer, a list of integers, or an empty list.
    job_id: list[int] | None = None
    job_state: SlurmState | None = None
    username: str | None = None
    start: datetime_api | None = None
    end: datetime_api | None = None
    requestor: Requestor

    def get_query[T: SelectOfScalar](self, query: T) -> T:
        if not self.requestor.is_admin:
            # TODO: implement query for general users
            raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED)

        if self.cluster is not None:
            query = query.join(SlurmClusterDB).where(
                SlurmClusterDB.name == self.cluster
            )
        if self.job_id is not None:
            if len(self.job_id) == 1:
                query = query.where(SlurmJobDB.job_id == self.job_id[0])
            else:
                query = query.where(col(SlurmJobDB.job_id).in_(self.job_id))
        if self.job_state is not None:
            query = query.where(SlurmJobDB.job_state == self.job_state)
        if self.username is not None:
            # TODO: Do we want to make this query the sarc user associated with the job?
            query = query.where(SlurmJobDB.user == self.username)
        if self.end is not None:
            query = query.where(col(SlurmJobDB.submit_time) < self.end)
        if self.start is not None:
            query = query.where(
                or_(
                    SlurmJobDB.end_time == None,  # noqa: E711
                    col(SlurmJobDB.end_time) > self.start,
                )
            )
        return query


def job_query_params(
    cluster: str | None = None,
    job_id: Annotated[list[str] | None, Query()] = None,
    job_state: SlurmState | None = None,
    username: str | None = None,
    start: datetime_api | None = None,
    end: datetime_api | None = None,
    requestor: Requestor = Depends(requestor),
    sess: Session = Depends(session_dep),
) -> JobQuery:
    """
    Annotation function for JobQueryType below.
    Annotates `job_id` with `Query()` annotation,
    so that Pydantic/FastAPI correctly parses `job_id`.

    We use `list[str]` to support empty lists (sent as `job_id=`).
    We then convert to `list[int]` to match `JobQuery` model.
    """
    validate_cluster(sess, cluster)

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
        requestor=requestor,
    )


JobQueryType = Annotated[JobQuery, Depends(job_query_params)]


class UserQuery(BaseModel):
    display_name: str | None = None
    email: str | None = None

    member_type: MemberType | None = None
    member_start: datetime_api | None = None
    member_end: datetime_api | None = None

    supervisor: UUID4 | None = None
    supervisor_start: datetime_api | None = None
    supervisor_end: datetime_api | None = None

    co_supervisor: UUID4 | None = None
    co_supervisor_start: datetime_api | None = None
    co_supervisor_end: datetime_api | None = None

    requestor: Requestor = Depends(requestor)

    def get_query[T: SelectOfScalar](self, query: T) -> T:
        if not self.requestor.is_admin:
            # TODO: implement query for general users
            raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED)

        if self.display_name is not None:
            query = query.where(col(UserDB.display_name).ilike(self.display_name))
        if self.email is not None:
            query = query.where(UserDB.email == self.email)
        if self.member_type is not None:
            query = query.join(MemberTypeDB).where(
                MemberTypeDB.member_type == self.member_type,
                MemberTypeDB.valid.overlaps(
                    Range(lower=self.member_start, upper=self.member_end)
                ),
            )
        if self.supervisor is not None:
            query = query.join(SupervisorDB).where(
                SupervisorDB.supervisor == self.supervisor,
                SupervisorDB.valid.overlaps(
                    Range(lower=self.supervisor_start, upper=self.supervisor_end)
                ),
            )
        if self.co_supervisor is not None:
            query = (
                query.join(CoSupervisorsHelper)
                .join(CoSupervisorDB)
                .where(
                    CoSupervisorsHelper.co_supervisor == self.co_supervisor,
                    CoSupervisorDB.valid.overlaps(
                        Range(
                            lower=self.co_supervisor_start, upper=self.co_supervisor_end
                        )
                    ),
                )
            )
        return query


UserQueryType = Annotated[UserQuery, Depends(UserQuery)]


@router.get("/job/query", dependencies=[Depends(require_admin)])
def get_jobs(
    query_opt: JobQueryType, sess: Session = Depends(session_dep)
) -> list[int]:
    return list(sess.exec(query_opt.get_query(select(UserDB.id))).all())  # type: ignore [arg-type]


@router.get("/job/list")
def list_jobs(
    query_opt: JobQueryType,
    page_opt: PageOptionsWithTimeType,
    sess: Session = Depends(session_dep),
) -> SlurmJobList:
    query_c = query_opt.get_query(select(func.count(col(SlurmJobDB.id))))
    query_c = page_opt.add_page_options(
        query_c,
        col(SlurmJobDB.id),  # type: ignore [arg-type]
        col(SlurmJobDB.submit_time),
    )
    total = sess.exec(query_c).one()

    query = query_opt.get_query(select(SlurmJobDB))
    query = page_opt.add_page_options(
        query,
        col(SlurmJobDB.id),  # type: ignore [arg-type]
        col(SlurmJobDB.submit_time),
    )

    jobs = [SlurmJob.model_validate(doc) for doc in sess.exec(query)]

    return SlurmJobList(
        jobs=jobs,
        page=page_opt.page,
        last_id=jobs[-1].id,
        last_time=jobs[-1].submit_time,
        per_page=page_opt.per_page,
        total=total,
    )


@router.get("/job/count")
def count_jobs(query_opt: JobQueryType, sess: Session = Depends(session_dep)) -> int:
    return sess.exec(query_opt.get_query(select(func.count(col(SlurmJobDB.id))))).one()


@router.get("/job/id/{id}", dependencies=[Depends(require_admin)])
def get_job(id: int, sess: Session = Depends(session_dep)) -> SlurmJob:
    job = sess.get(SlurmJobDB, id)
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Job not found"
        )
    return SlurmJob.model_validate(job)


@router.get("/cluster/list", dependencies=[Depends(requestor)])
def get_cluster_names(sess: Session = Depends(session_dep)) -> list[str]:
    """Return the names of available clusters."""
    # TODO: should this return cluster objects instead of just names?
    return sorted([cl.name for cl in get_available_clusters(sess)])


@router.get("/gpu/rgu", dependencies=[Depends(requestor)])
def get_rgu_value_per_gpu() -> dict[str, float]:
    """Return the mapping GPU->RGU."""
    return get_rgus()


@router.get("/user/query", dependencies=[Depends(require_admin)])
def query_users(
    query_opt: UserQueryType, sess: Session = Depends(session_dep)
) -> list[int]:
    """Search users. Return user IDs."""
    return list(sess.exec(query_opt.get_query(select(UserDB.id))).all())  # type: ignore [arg-type]


@router.get("/user/list")
def list_users(
    query_opt: UserQueryType,
    page_opt: PageOptionsType,
    sess: Session = Depends(session_dep),
) -> UserList:
    query_c = query_opt.get_query(select(func.count(col(UserDB.id))))
    query_c = page_opt.add_page_options(query_c, col(UserDB.id))  # type: ignore [arg-type]
    total = sess.exec(query_c).one()

    query = query_opt.get_query(select(UserDB))
    query = page_opt.add_page_options(query, col(UserDB.id))  # type: ignore [arg-type]

    users = [User.model_validate(doc) for doc in sess.exec(query)]

    return UserList(
        users=users,
        page=page_opt.page,
        last_id=users[-1].id,
        per_page=page_opt.per_page,
        total=total,
    )


@router.get("/user/id/{id}", dependencies=[Depends(require_admin)])
def get_user_by_id(id: int, sess: Session = Depends(session_dep)) -> User:
    """Get user with given ID."""
    user = sess.get(UserDB, id)
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="User not found"
        )
    return User.model_validate(user)


@router.get("/user/email/{email}", dependencies=[Depends(require_admin)])
def get_user_by_email(email: str, sess: Session = Depends(session_dep)) -> User:
    """Get user with given email."""
    user = sess.exec(select(UserDB).where(UserDB.email == email)).one_or_none()
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="User not found"
        )
    return User.model_validate(user)


@router.get("/health/list")
def health_list() -> list[HealthCheckState]:
    """Get current health check states (check definition and last result) saved in database."""
    # TODO: apparently I forgot this table, so this will be done during the health check pass
    states = list(get_healthcheck_state_collection().get_states())
    return states
