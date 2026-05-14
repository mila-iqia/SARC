from datetime import datetime

import pytest
from pandas import DataFrame
from sqlmodel import Session, col, or_, select

from sarc.config import UTC
from sarc.db.job_series import JobSeriesDB
from tests.functional.job_series.base import (
    BaseTestLoadJobSeries,
    LoadJobSeriesFn,
    _finalize_records,
    _parse_dt,
)


def _apply_view_filters(
    query,
    *,
    cluster: str | None = None,
    job_state: str | None = None,
    job_id: int | list[int] | None = None,
    user: str | None = None,
    start: datetime | str | None = None,
    end: datetime | str | None = None,
):
    if cluster is not None:
        query = query.where(JobSeriesDB.cluster_name == cluster)
    if job_state is not None:
        query = query.where(JobSeriesDB.job_state == job_state)
    if job_id is not None:
        if isinstance(job_id, list):
            query = query.where(col(JobSeriesDB.job_id).in_(job_id))
        else:
            query = query.where(JobSeriesDB.job_id == job_id)
    if user is not None:
        query = query.where(JobSeriesDB.cluster_user == user)
    if end is not None:
        query = query.where(col(JobSeriesDB.submit_time) < _parse_dt(end))
    if start is not None:
        dt = _parse_dt(start)
        query = query.where(
            or_(col(JobSeriesDB.end_time).is_(None), col(JobSeriesDB.end_time) > dt)
        )
    return query


def sql_load_job_series(sess: Session, **kwargs) -> DataFrame:
    """Query JobSeriesDB view and return a dataframe."""
    query = _apply_view_filters(
        select(JobSeriesDB).order_by(JobSeriesDB.job_db_id), **kwargs
    )
    records = [row.model_dump() for row in sess.exec(query).all()]
    _finalize_records(records, datetime.now(tz=UTC))
    return DataFrame(records)


class TestSqlLoadJobSeries(BaseTestLoadJobSeries):
    """Tests for SQL load_job_series"""

    @pytest.fixture
    def fn_load_job_series(self) -> LoadJobSeriesFn:
        return sql_load_job_series
