from dataclasses import asdict
from datetime import datetime

import pytest
from pandas import DataFrame
from sqlmodel import Session

from sarc.config import UTC
from tests.functional.job_series.base import (
    BaseTestLoadJobSeries,
    LoadJobSeriesFn,
    _finalize_records,
    _parse_dt,
)

# extra_fields needed so the REST response carries every column the SQL view
# returns (otherwise the resulting DataFrame would miss columns).
_ALL_EXTRA_FIELDS = ["cluster_name", "sarc_user", "supervisors", "statistics", "rgu"]


def _to_api_datetime(value: datetime | str | None) -> datetime | None:
    """str/naive -> tz-aware in UTC (the API requires tz-aware UTC datetimes)."""
    if value is None:
        return None
    return _parse_dt(value).astimezone(UTC)


class TestRestLoadJobSeries(BaseTestLoadJobSeries):
    """
    Runs the standard load_job_series test suite against the REST API implementation.
    """

    @pytest.fixture
    def fn_load_job_series(self, sarc_client) -> LoadJobSeriesFn:
        def rest_load_job_series(
            sess: Session,
            *,
            cluster: str | None = None,
            job_state: str | None = None,
            job_id: int | list[int] | None = None,
            user: str | None = None,
            start: datetime | str | None = None,
            end: datetime | str | None = None,
        ) -> DataFrame:
            # The REST endpoint reads from the configured DB session; `sess` is
            # passed for protocol compatibility but not used directly here.
            del sess

            api_job_id: list[int] | None
            if job_id is None:
                api_job_id = None
            elif isinstance(job_id, list):
                api_job_id = job_id
            else:
                api_job_id = [job_id]

            rows = list(
                sarc_client.get_job_series(
                    cluster_name=cluster,
                    cluster_user=user,
                    job_state=job_state,
                    job_id=api_job_id,
                    start=_to_api_datetime(start),
                    end=_to_api_datetime(end),
                    extra_fields=_ALL_EXTRA_FIELDS,
                )
            )
            records = [asdict(r) for r in rows]
            _finalize_records(records, datetime.now(tz=UTC))
            df = DataFrame(records)
            if not df.empty:
                df = df.sort_values("job_db_id").reset_index(drop=True)
            return df

        return rest_load_job_series
