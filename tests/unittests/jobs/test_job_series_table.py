"""The job_series table, as compiled SQL.

Compile-only tests (no database): the table is the single relation every
/dash query reads -- its denormalized columns must not reintroduce a join.
"""

from sqlalchemy.dialects import postgresql
from sqlmodel import col, select

from sarc.db.job_series import JobSeriesTable


def _sql(stmt) -> str:
    return str(
        stmt.compile(
            dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}
        )
    )


def test_is_a_single_relation():
    sql = _sql(
        select(
            col(JobSeriesTable.job_db_id),
            col(JobSeriesTable.submit_time),
            col(JobSeriesTable.cluster_id),
        )
    )
    assert "FROM job_series" in sql
    assert "JOIN" not in sql


def test_denormalized_columns_need_no_join():
    # Columns the old view computed by joining clusters/users/gpurgudb/stats are
    # plain table columns now: selecting them must add no join and no subquery.
    sql = _sql(
        select(
            col(JobSeriesTable.job_db_id),
            col(JobSeriesTable.cluster_name),
            col(JobSeriesTable.display_name),
            col(JobSeriesTable.allocated_gpu_cost),
            col(JobSeriesTable.allocated_gpu_waste),
        )
    )
    assert "JOIN" not in sql
    assert "jobstatisticdb" not in sql
    assert "gpurgudb" not in sql
    assert "membertypedb" not in sql
