"""job_series_select(): the client-side pruned SELECT over the job_series view.

Compile-only tests (no database): assert which view joins survive pruning for a
given set of columns, and the safety invariant the pruner relies on.
"""

import pytest
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql.expression import Join

from sarc.db.job_series import JobSeriesDB, job_series_select


def _sql(stmt) -> str:
    return str(
        stmt.compile(
            dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}
        )
    )


def test_job_columns_only_prunes_every_join():
    sql = _sql(job_series_select("job_db_id", "submit_time", "cluster_id"))
    assert "JOIN" not in sql
    assert "FROM slurm_jobs" in sql


def test_cluster_name_keeps_only_the_clusters_join():
    sql = _sql(job_series_select("job_db_id", "cluster_name"))
    assert "JOIN clusters" in sql
    assert "users" not in sql
    assert "gpurgudb" not in sql
    assert "jobstatisticdb" not in sql


def test_rgu_column_keeps_only_the_gpurgudb_join():
    sql = _sql(job_series_select("job_db_id", "allocated_gpu_cost"))
    assert "JOIN gpurgudb" in sql
    assert "JOIN clusters" not in sql
    assert "jobstatisticdb" not in sql


def test_user_column_keeps_only_the_users_join():
    sql = _sql(job_series_select("job_db_id", "display_name"))
    assert "JOIN users" in sql
    assert "JOIN clusters" not in sql


def test_stat_column_keeps_only_its_own_stat_join():
    # allocated_gpu_waste reads the gpu_sm_occupancy mean through its own
    # aliased jobstatisticdb join; asking for it must not drag the
    # cpu_utilization one.
    sql = _sql(job_series_select("job_db_id", "allocated_gpu_waste"))
    assert sql.count("JOIN jobstatisticdb") == 1
    assert "'gpu_sm_occupancy'" in sql
    assert "'cpu_utilization'" not in sql


def test_scalar_subquery_column_adds_no_join():
    # member_type is a correlated subquery, not a join: it must appear as a
    # subselect, without touching the FROM clause.
    sql = _sql(job_series_select("job_db_id", "member_type"))
    assert "FROM membertypedb" in sql
    assert "JOIN" not in sql


def test_all_columns_keep_all_joins():
    every = list(JobSeriesDB.__sql_view__.selected_columns.keys())
    sql = _sql(job_series_select(*every))
    for target in ("JOIN users", "JOIN clusters", "JOIN gpurgudb"):
        assert target in sql
    assert sql.count("JOIN jobstatisticdb") == 2


def test_unknown_column_raises():
    with pytest.raises(KeyError, match="no_such_column"):
        job_series_select("job_db_id", "no_such_column")


def test_every_view_join_is_left():
    # The pruner's safety contract: dropping a join must never change the row
    # set, which holds only for LEFT joins on unique keys. Guard the LEFT part;
    # uniqueness is by schema construction (PKs + unique(job_id, name)).
    def walk(node):
        if isinstance(node, Join):
            assert node.isouter, f"view join on {node.right} must be LEFT"
            walk(node.left)

    walk(JobSeriesDB.__sql_view__.get_final_froms()[0])
