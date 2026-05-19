import operator
from collections.abc import Generator
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import reduce
from typing import Annotated

from easy_oauth.cap import Capability
from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import AfterValidator, BaseModel, Field
from serieux import deserialize
from sqlalchemy.dialects.postgresql import Range
from sqlalchemy.orm import Mapped
from sqlmodel import Session, and_, col, func, or_, select
from sqlmodel.sql.expression import SelectOfScalar

from sarc.config import UTC, Config, config
from sarc.db.cluster import SlurmClusterDB, get_available_clusters
from sarc.db.healthcheck import HealthCheckStateDB
from sarc.db.job import SlurmJobDB, SlurmState
from sarc.db.job_series import JobSeriesDB
from sarc.db.support import GpuRguDB
from sarc.db.users import (
    MatchingID,
    MemberType,
    MemberTypeDB,
    SupervisorsDB,
    SupervisorsHelper,
    UserDB,
)
from sarc.models.api import JobSeriesList, SlurmJob, SlurmJobList, User, UserList
from sarc.models.cluster import SlurmCluster
from sarc.models.healthcheck_state import HealthCheckState
from sarc.models.job import Statistics
from sarc.models.series import JobSeries


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
        if not cfg.server.auth:
            yield "__admin__"
        else:
            async for name in cfg.server.auth.get_email_capability(cap)(request):
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
    auth = cfg.server.auth
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


class ListOptions(BaseModel):
    limit: int = Field(default=100, ge=1, le=100)
    cursor: int | str | None = None

    def add_list_options[T](
        self,
        query: SelectOfScalar[T],
        id_col: Mapped[int],
        time_col: Mapped[datetime] | None,
    ) -> SelectOfScalar[T]:
        query = query.limit(self.limit)
        if time_col:
            query = query.order_by(time_col.desc())
        query = query.order_by(id_col)
        if isinstance(self.cursor, int):
            query = query.offset(self.cursor)
        elif self.cursor:
            if time_col:
                id_val, _, time_val = self.cursor.partition(";")
                id_val = int(id_val)
                time_val = datetime.fromisoformat(time_val)
                query = query.where(
                    or_(
                        time_col < time_val, and_(time_col == time_val, id_col > id_val)
                    )
                )
            else:
                query = query.where(id_col > int(self.cursor))
        return query


def list_options(limit: int = 100, cursor: int | str | None = None) -> ListOptions:
    return ListOptions(limit=limit, cursor=cursor)


ListOptionsType = Annotated[ListOptions, Depends(list_options)]


class JobQuery(BaseModel):
    cluster_name: str | None = None
    # job_id supports None, an integer, a list of integers, or an empty list.
    job_id: list[int] | None = None
    job_state: SlurmState | None = None
    email: str | None = None
    sarc_user_id: int | None = None
    cluster_user: str | None = None
    start: datetime_api | None = None
    end: datetime_api | None = None
    requestor: Requestor

    def get_query[T: SelectOfScalar](self, query: T) -> T:
        if not self.requestor.is_admin:
            # TODO: implement query for general users
            raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED)

        if self.cluster_name is not None:
            query = query.join(SlurmClusterDB).where(
                SlurmClusterDB.name == self.cluster_name
            )
        if self.job_id is not None:
            if len(self.job_id) == 1:
                query = query.where(SlurmJobDB.job_id == self.job_id[0])
            else:
                query = query.where(col(SlurmJobDB.job_id).in_(self.job_id))
        if self.job_state is not None:
            query = query.where(SlurmJobDB.job_state == self.job_state)
        if self.cluster_user is not None:
            query = query.where(SlurmJobDB.cluster_user == self.cluster_user)
        if self.sarc_user_id is not None:
            query = query.where(SlurmJobDB.sarc_user_id == self.sarc_user_id)
        if self.email is not None:
            query = query.where(
                SlurmJobDB.sarc_user_id == UserDB.id, UserDB.email == self.email
            )
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
    cluster_name: str | None = None,
    job_id: Annotated[list[str] | None, Query()] = None,
    job_state: SlurmState | None = None,
    email: str | None = None,
    sarc_user_id: int | None = None,
    cluster_user: str | None = None,
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
    validate_cluster(sess, cluster_name)

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
        cluster_name=cluster_name,
        job_id=job_id_ints,
        job_state=job_state,
        email=email,
        sarc_user_id=sarc_user_id,
        cluster_user=cluster_user,
        start=start,
        end=end,
        requestor=requestor,
    )


JobQueryType = Annotated[JobQuery, Depends(job_query_params)]


class JobSeriesQuery(BaseModel):
    cluster_name: str | None = None
    job_id: list[int] | None = None
    job_state: SlurmState | None = None
    email: str | None = None
    sarc_user_id: int | None = None
    cluster_user: str | None = None
    start: datetime_api | None = None
    end: datetime_api | None = None
    requestor: Requestor

    def apply_filters[T: SelectOfScalar](self, query: T) -> T:
        if not self.requestor.is_admin:
            raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED)

        if self.cluster_name is not None:
            query = query.where(JobSeriesDB.cluster_name == self.cluster_name)
        if self.job_id is not None:
            if len(self.job_id) == 1:
                query = query.where(JobSeriesDB.job_id == self.job_id[0])
            else:
                query = query.where(col(JobSeriesDB.job_id).in_(self.job_id))
        if self.job_state is not None:
            query = query.where(JobSeriesDB.job_state == self.job_state)
        if self.cluster_user is not None:
            query = query.where(JobSeriesDB.cluster_user == self.cluster_user)
        if self.sarc_user_id is not None:
            query = query.where(JobSeriesDB.sarc_user_id == self.sarc_user_id)
        if self.email is not None:
            query = query.where(JobSeriesDB.email == self.email)
        if self.end is not None:
            query = query.where(col(JobSeriesDB.submit_time) < self.end)
        if self.start is not None:
            query = query.where(
                or_(
                    JobSeriesDB.end_time == None,  # noqa: E711
                    col(JobSeriesDB.end_time) > self.start,
                )
            )
        return query


def job_series_query_params(
    cluster_name: str | None = None,
    job_id: Annotated[list[str] | None, Query()] = None,
    job_state: SlurmState | None = None,
    email: str | None = None,
    sarc_user_id: int | None = None,
    cluster_user: str | None = None,
    start: datetime_api | None = None,
    end: datetime_api | None = None,
    requestor: Requestor = Depends(requestor),
    sess: Session = Depends(session_dep),
) -> JobSeriesQuery:
    validate_cluster(sess, cluster_name)

    job_id_ints = None
    if job_id is not None:
        try:
            job_id_ints = [int(jid) for jid in job_id if jid]
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                detail="job_id must be a list of integers",
            )

    return JobSeriesQuery(
        cluster_name=cluster_name,
        job_id=job_id_ints,
        job_state=job_state,
        email=email,
        sarc_user_id=sarc_user_id,
        cluster_user=cluster_user,
        start=start,
        end=end,
        requestor=requestor,
    )


JobSeriesQueryType = Annotated[JobSeriesQuery, Depends(job_series_query_params)]


def _series_convert(row) -> JobSeries:
    row_dict = dict(row._mapping)
    valid = JobSeries.__dataclass_fields__.keys()
    return JobSeries(**{k: v for k, v in row_dict.items() if k in valid})


class UserQuery(BaseModel):
    display_name: str | None = None
    email: str | None = None

    start: datetime_api | None = None
    end: datetime_api | None = None

    member_type: MemberType | None = None
    supervisor: int | None = None

    requestor: Requestor = Depends(requestor)

    def get_query[T: SelectOfScalar](self, query: T) -> T:
        start = self.start
        end = self.end
        if start is None and end is None:
            start = datetime.now(tz=UTC)
            end = start + timedelta(microseconds=1)

        if not self.requestor.is_admin:
            # TODO: implement query for general users
            raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED)

        if self.display_name is not None:
            query = query.where(
                col(UserDB.display_name).ilike(f"%{self.display_name}%")
            )
        if self.email is not None:
            query = query.where(UserDB.email == self.email)
        if self.member_type is not None:
            query = query.join(MemberTypeDB).where(
                MemberTypeDB.member_type == self.member_type,
                MemberTypeDB.valid.overlaps(Range(lower=start, upper=end)),
            )
        if self.supervisor is not None:
            query = (
                query.join(SupervisorsDB, col(SupervisorsDB.user_id) == col(UserDB.id))
                .join(
                    SupervisorsHelper,
                    col(SupervisorsHelper.list_id) == col(SupervisorsDB.id),
                )
                .where(
                    SupervisorsHelper.supervisor == self.supervisor,
                    SupervisorsDB.valid.overlaps(Range(lower=start, upper=end)),
                )
            )
        return query


UserQueryType = Annotated[UserQuery, Depends(UserQuery)]


def job_convert(doc: SlurmJobDB, extra_fields: set[str]) -> SlurmJob:
    job = SlurmJob.model_validate(doc.model_dump())
    for field in extra_fields:
        match field:
            case "cluster_name":
                job.cluster_name = doc.cluster.name
            case "sarc_user":
                job.sarc_user = User.model_validate(doc.sarc_user.model_dump())
            case "statistics":
                job.statistics = {
                    k: Statistics.model_validate(v.model_dump())
                    for k, v in doc.statistics.items()
                }
            case unknown:
                raise HTTPException(
                    status_code=422, detail=f"Invalid extra_field: '{unknown}'"
                )
    return job


@router.get("/job/query")
def query_jobs(
    query_opt: JobQueryType,
    list_opt: ListOptionsType,
    extra_fields: str | None = None,
    sess: Session = Depends(session_dep),
) -> SlurmJobList:
    extra_fields_set = set(extra_fields.split(",")) if extra_fields else set()
    if query_opt.cluster_name:
        extra_fields_set.add("cluster_name")
    if query_opt.sarc_user_id or query_opt.email:
        extra_fields_set.add("sarc_user")

    query = query_opt.get_query(select(SlurmJobDB))
    query = list_opt.add_list_options(
        query,
        col(SlurmJobDB.id),  # ty:ignore[invalid-argument-type]
        col(SlurmJobDB.submit_time),
    )

    jobs = [job_convert(doc, extra_fields_set) for doc in sess.exec(query)]

    if len(jobs) < list_opt.limit:
        # There are no more results (note: limit > 0)
        cursor = False
    else:
        last = jobs[-1]
        cursor = f"{last.id};{last.submit_time.isoformat()}"

    return SlurmJobList(results=jobs, cursor=cursor)


@router.get("/job/count")
def count_jobs(query_opt: JobQueryType, sess: Session = Depends(session_dep)) -> int:
    return sess.exec(query_opt.get_query(select(func.count(col(SlurmJobDB.id))))).one()


@router.get("/job/id/{id}", dependencies=[Depends(require_admin)])
def get_job(
    id: int, extra_fields: str | None = None, sess: Session = Depends(session_dep)
) -> SlurmJob:
    job = sess.get(SlurmJobDB, id)
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Job not found"
        )
    return job_convert(job, set(extra_fields.split(",")) if extra_fields else set())


# Format: {extra_field_name: {columns_to_select ...}}
_EXTRA_FIELDS = {
    "cluster_name": {"cluster_name"},
    "sarc_user": {"display_name", "member_type", "email"},
    "supervisors": {"supervisors"},
    "statistics": {"statistics"},
    "rgu": {"gpu_type_rgu", "rgu"},
}

_SERIES_OPTIONAL_COLS = reduce(operator.or_, _EXTRA_FIELDS.values())


@router.get("/job/series")
def job_series(
    query_opt: JobSeriesQueryType,
    list_opt: ListOptionsType,
    extra_fields: str | None = None,
    sess: Session = Depends(session_dep),
) -> JobSeriesList:
    extra_fields_set = set(extra_fields.split(",")) if extra_fields else set()
    unknown = extra_fields_set - set(_EXTRA_FIELDS)
    if unknown:
        raise HTTPException(
            status_code=422, detail=f"Invalid extra_field(s): {sorted(unknown)}"
        )

    # Determine the set of columns to select
    names = {
        c.name
        for c in JobSeriesDB.__table__.columns  # ty:ignore[unresolved-attribute]
        if c.name not in _SERIES_OPTIONAL_COLS
    }
    for extra in extra_fields_set:
        names |= _EXTRA_FIELDS[extra]
    cols = [JobSeriesDB.__table__.c[name] for name in names]  # ty:ignore[unresolved-attribute]

    query = select(*cols)
    query = query_opt.apply_filters(query)
    query = list_opt.add_list_options(
        query,
        col(JobSeriesDB.job_db_id),  # type: ignore[arg-type]
        col(JobSeriesDB.submit_time),
    )

    rows = list(sess.exec(query))
    series = [_series_convert(row) for row in rows]

    if len(series) < list_opt.limit:
        cursor = False
    else:
        last = series[-1]
        cursor = f"{last.job_db_id};{last.submit_time.isoformat()}"

    return JobSeriesList(results=series, cursor=cursor)


@router.get("/cluster/list", dependencies=[Depends(requestor)])
def get_cluster_names(sess: Session = Depends(session_dep)) -> list[SlurmCluster]:
    """Return the names of available clusters."""
    # TODO: should this return cluster objects instead of just names?
    return [
        SlurmCluster.model_validate(cl.model_dump())
        for cl in get_available_clusters(sess)
    ]


@router.get("/gpu/rgu", dependencies=[Depends(requestor)])
def get_rgu_value_per_gpu(sess: Session = Depends(session_dep)) -> dict[str, float]:
    """Return the mapping GPU->RGU."""
    res: dict[str, float] = {}
    for entry in sess.exec(select(GpuRguDB)):
        res[entry.name] = entry.rgu
    return res


@router.post("/gpu/rgu", dependencies=[Depends(require_admin)])
def update_rgu(update: dict[str, float], sess: Session = Depends(session_dep)) -> bool:
    for name, val in update.items():
        sess.merge(GpuRguDB(name=name, rgu=val))
    sess.commit()
    return True


@router.get("/user/query")
def query_users(
    query_opt: UserQueryType,
    list_opt: ListOptionsType,
    sess: Session = Depends(session_dep),
) -> UserList:
    query = query_opt.get_query(select(UserDB))
    query = list_opt.add_list_options(query, col(UserDB.id), None)  # ty:ignore[invalid-argument-type]

    results = list(sess.exec(query))
    users = [User.model_validate(doc.model_dump()) for doc in results]

    if len(users) < list_opt.limit:
        # There are no more results (note: limit > 0)
        cursor = False
    else:
        cursor = str(users[-1].id)

    return UserList(results=users, cursor=cursor)


@router.get("/user/id/{id}", dependencies=[Depends(require_admin)])
def get_user_by_id(id: int, sess: Session = Depends(session_dep)) -> User:
    """Get user with given ID."""
    user = sess.get(UserDB, id)
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="User not found"
        )
    return User.model_validate(user.model_dump())


@router.get("/user/email/{email}", dependencies=[Depends(require_admin)])
def get_user_by_email(email: str, sess: Session = Depends(session_dep)) -> User:
    """Get user with given email."""
    user = sess.exec(select(UserDB).where(UserDB.email == email)).one_or_none()
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="User not found"
        )
    return User.model_validate(user.model_dump())


@router.get("/health/list")
def health_list(sess: Session = Depends(session_dep)) -> list[HealthCheckState]:
    """Get current health check states (check definition and last result) saved in database."""
    return [
        HealthCheckState(
            check=state.check,
            last_result=state.last_result,
            last_message=state.last_message,
        )
        for state in HealthCheckStateDB.get_states(sess)
    ]
