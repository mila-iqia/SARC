"""job_series read model: materialize job_series_view

Revision ID: 7fe5b57ffa1d
Revises: 6037cb1af327
Create Date: 2026-09-03 19:00:31.981899+00:00

Replaces the `job_series_view` join-tree view with a materialized table (one
wide row per job: the slurm_jobs columns, copied users/clusters display
columns, denormalized RGU weights/costs/waste, and the jobstatisticdb stats
pivoted one column per mean/max) plus the trigger machinery that keeps it in
sync with its base tables. `job_series_view` survives as a thin wrapper that
adds back only the two time-ranged user subqueries (member_type, supervisors)
-- everything else is a single-relation read.

This migration intentionally contains NO data. After upgrading, run
`sarc db backfill-series` once to populate the table in chunks (the triggers
only see changes from that point on, so rows older than the migration are
invisible to readers until the backfill completes).

"""

from typing import Sequence, Union

import sqlalchemy as sa
import sqlmodel.sql.sqltypes
from alembic_utils.pg_function import PGFunction
from alembic_utils.pg_trigger import PGTrigger
from alembic_utils.pg_view import PGView
from sqlalchemy.dialects.postgresql import ENUM as PGEnum
from sqlalchemy.dialects.postgresql import JSONB

import sarc.db.sqlmodel
from alembic import op
from sarc.db.job_series import JOB_SERIES_FUNCTIONS, JOB_SERIES_TRIGGERS

# revision identifiers, used by Alembic.
revision: str = "7fe5b57ffa1d"
down_revision: Union[str, Sequence[str], None] = "6037cb1af327"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# The pre-materialization view definition, frozen here for downgrade().
LEGACY_JOB_SERIES_VIEW = """SELECT slurm_jobs.id AS job_db_id, slurm_jobs.sarc_user_id AS sarc_user_id, slurm_jobs.cluster_id, slurm_jobs.account, slurm_jobs.job_id, slurm_jobs.array_job_id, slurm_jobs.task_id, slurm_jobs.name, slurm_jobs.cluster_user, slurm_jobs."group", slurm_jobs.job_state, slurm_jobs.exit_code, slurm_jobs.signal, slurm_jobs.partition, slurm_jobs.nodes, slurm_jobs.work_dir, slurm_jobs.submit_line, slurm_jobs.constraints, slurm_jobs.priority, slurm_jobs.qos, slurm_jobs."CLEAR_SCHEDULING", slurm_jobs."STARTED_ON_SUBMIT", slurm_jobs."STARTED_ON_SCHEDULE", slurm_jobs."STARTED_ON_BACKFILL", slurm_jobs.time_limit, slurm_jobs.submit_time, slurm_jobs.start_time, slurm_jobs.end_time, slurm_jobs.elapsed_time, slurm_jobs.requested_cpu, slurm_jobs.requested_mem, slurm_jobs.requested_node, slurm_jobs.requested_billing, slurm_jobs.requested_gres_gpu, slurm_jobs.requested_gpu_type, slurm_jobs.allocated_cpu, slurm_jobs.allocated_mem, slurm_jobs.allocated_node, slurm_jobs.allocated_billing, slurm_jobs.allocated_gres_gpu, slurm_jobs.allocated_gpu_type, slurm_jobs.harmonized_gpu_type, users.display_name, users.email, clusters.name AS cluster_name, (SELECT membertypedb.member_type
FROM membertypedb
WHERE membertypedb.user_id = slurm_jobs.sarc_user_id AND membertypedb.valid @> slurm_jobs.submit_time) AS member_type, (SELECT json_agg(supervisorshelper.supervisor ORDER BY supervisorshelper.pos) AS json_agg_1
FROM user_supervisors JOIN supervisorshelper ON user_supervisors.id = supervisorshelper.list_id
WHERE user_supervisors.user_id = slurm_jobs.sarc_user_id AND user_supervisors.valid @> slurm_jobs.submit_time) AS supervisors, gpurgudb.rgu AS gpu_type_rgu, gpurgudb.drac_rgu AS gpu_type_rgu_drac, coalesce(slurm_jobs.requested_gres_gpu, 0) * gpurgudb.rgu AS requested_rgu, coalesce(slurm_jobs.requested_gres_gpu, 0) * gpurgudb.drac_rgu AS requested_rgu_drac, coalesce(slurm_jobs.allocated_gres_gpu, 0) * gpurgudb.rgu AS allocated_rgu, coalesce(slurm_jobs.allocated_gres_gpu, 0) * gpurgudb.drac_rgu AS allocated_rgu_drac, slurm_jobs.elapsed_time * slurm_jobs.requested_cpu AS requested_cpu_cost, (1 - jobstatisticdb_1.mean) * slurm_jobs.elapsed_time * slurm_jobs.requested_cpu AS requested_cpu_waste, slurm_jobs.elapsed_time * slurm_jobs.allocated_cpu AS allocated_cpu_cost, (1 - jobstatisticdb_1.mean) * slurm_jobs.elapsed_time * slurm_jobs.allocated_cpu AS allocated_cpu_waste, slurm_jobs.elapsed_time * (slurm_jobs.allocated_cpu - slurm_jobs.requested_cpu) AS cpu_overbilling_cost, slurm_jobs.elapsed_time * slurm_jobs.requested_gres_gpu * gpurgudb.drac_rgu AS requested_gpu_cost, (1 - jobstatisticdb_2.mean) * slurm_jobs.elapsed_time * slurm_jobs.requested_gres_gpu * gpurgudb.drac_rgu AS requested_gpu_waste, slurm_jobs.elapsed_time * slurm_jobs.allocated_gres_gpu * gpurgudb.drac_rgu AS allocated_gpu_cost, (1 - jobstatisticdb_2.mean) * slurm_jobs.elapsed_time * slurm_jobs.allocated_gres_gpu * gpurgudb.drac_rgu AS allocated_gpu_waste, slurm_jobs.elapsed_time * (slurm_jobs.allocated_gres_gpu - slurm_jobs.requested_gres_gpu) * gpurgudb.drac_rgu AS gpu_overbilling_cost, jobstatisticdb_2.mean AS usage_metric, jobstatisticdb_2.mean AS gpu_sm_occupancy_mean, jobstatisticdb_2.max AS gpu_sm_occupancy_max, jobstatisticdb_3.mean AS gpu_utilization_mean, jobstatisticdb_4.max AS gpu_memory_max
FROM slurm_jobs LEFT OUTER JOIN users ON slurm_jobs.sarc_user_id = users.id LEFT OUTER JOIN clusters ON slurm_jobs.cluster_id = clusters.id LEFT OUTER JOIN gpurgudb ON gpurgudb.name = slurm_jobs.harmonized_gpu_type LEFT OUTER JOIN jobstatisticdb AS jobstatisticdb_2 ON jobstatisticdb_2.job_id = slurm_jobs.id AND jobstatisticdb_2.name = 'gpu_sm_occupancy' LEFT OUTER JOIN jobstatisticdb AS jobstatisticdb_1 ON jobstatisticdb_1.job_id = slurm_jobs.id AND jobstatisticdb_1.name = 'cpu_utilization' LEFT OUTER JOIN jobstatisticdb AS jobstatisticdb_3 ON jobstatisticdb_3.job_id = slurm_jobs.id AND jobstatisticdb_3.name = 'gpu_utilization' LEFT OUTER JOIN jobstatisticdb AS jobstatisticdb_4 ON jobstatisticdb_4.job_id = slurm_jobs.id AND jobstatisticdb_4.name = 'gpu_memory'"""

# The wrapper view (kept in sync with the compiled JobSeriesDB.__sql_view__;
# `alembic check` flags any drift).
JOB_SERIES_WRAPPER_VIEW = """SELECT job_series.job_db_id, job_series.cluster_id, job_series.account, job_series.job_id, job_series.array_job_id, job_series.task_id, job_series.name, job_series.cluster_user, job_series."group", job_series.job_state, job_series.exit_code, job_series.signal, job_series.partition, job_series.nodes, job_series.work_dir, job_series.submit_line, job_series.constraints, job_series.priority, job_series.qos, job_series."CLEAR_SCHEDULING", job_series."STARTED_ON_SUBMIT", job_series."STARTED_ON_SCHEDULE", job_series."STARTED_ON_BACKFILL", job_series.time_limit, job_series.submit_time, job_series.start_time, job_series.end_time, job_series.elapsed_time, job_series.requested_cpu, job_series.requested_mem, job_series.requested_node, job_series.requested_billing, job_series.requested_gres_gpu, job_series.requested_gpu_type, job_series.allocated_cpu, job_series.allocated_mem, job_series.allocated_node, job_series.allocated_billing, job_series.allocated_gres_gpu, job_series.allocated_gpu_type, job_series.harmonized_gpu_type, job_series.sarc_user_id, job_series.display_name, job_series.email, job_series.cluster_name, job_series.gpu_type_rgu, job_series.gpu_type_rgu_drac, job_series.requested_rgu, job_series.requested_rgu_drac, job_series.allocated_rgu, job_series.allocated_rgu_drac, job_series.requested_cpu_cost, job_series.requested_cpu_waste, job_series.allocated_cpu_cost, job_series.allocated_cpu_waste, job_series.cpu_overbilling_cost, job_series.requested_gpu_cost, job_series.requested_gpu_waste, job_series.allocated_gpu_cost, job_series.allocated_gpu_waste, job_series.gpu_overbilling_cost, job_series.gpu_sm_occupancy_mean, job_series.gpu_sm_occupancy_max, job_series.gpu_utilization_mean, job_series.gpu_utilization_max, job_series.gpu_utilization_fp16_mean, job_series.gpu_utilization_fp16_max, job_series.gpu_utilization_fp32_mean, job_series.gpu_utilization_fp32_max, job_series.gpu_utilization_fp64_mean, job_series.gpu_utilization_fp64_max, job_series.gpu_memory_mean, job_series.gpu_memory_max, job_series.system_memory_mean, job_series.system_memory_max, job_series.cpu_utilization_mean, job_series.gpu_sm_occupancy_mean AS usage_metric, (SELECT membertypedb.member_type
FROM membertypedb
WHERE membertypedb.user_id = job_series.sarc_user_id AND membertypedb.valid @> job_series.submit_time) AS member_type, (SELECT json_agg(supervisorshelper.supervisor ORDER BY supervisorshelper.pos) AS json_agg_1
FROM user_supervisors JOIN supervisorshelper ON user_supervisors.id = supervisorshelper.list_id
WHERE user_supervisors.user_id = job_series.sarc_user_id AND user_supervisors.valid @> job_series.submit_time) AS supervisors
FROM job_series"""

ELIGIBILITY = "allocated_gres_gpu > 0 AND gpu_type_rgu_drac IS NOT NULL"

COVERING = [
    "job_db_id",
    "start_time",
    "elapsed_time",
    "allocated_rgu_drac",
    "cluster_id",
    "sarc_user_id",
    "job_state",
    "job_id",
    "submit_time",
    "harmonized_gpu_type",
    "requested_gres_gpu",
    "allocated_billing",
    "gpu_sm_occupancy_mean",
    "gpu_sm_occupancy_max",
    "gpu_utilization_mean",
    "gpu_utilization_max",
    "gpu_utilization_fp16_mean",
    "gpu_utilization_fp16_max",
    "gpu_utilization_fp32_mean",
    "gpu_utilization_fp32_max",
    "gpu_utilization_fp64_mean",
    "gpu_utilization_fp64_max",
    "gpu_memory_mean",
    "gpu_memory_max",
    "system_memory_mean",
    "system_memory_max",
]
SUBMIT_COVERING = [
    "job_db_id",
    "start_time",
    "elapsed_time",
    "time_limit",
    "allocated_rgu_drac",
    "cluster_id",
    "sarc_user_id",
    "job_state",
]
END_EXPR = sa.literal_column("slurm_job_end(start_time, elapsed_time)")


def _create_indexes() -> None:
    """The job_series indexes (see JobSeriesTable.__table_args__ for the why).

    Written out (rather than JobSeriesTable.__table__.create()) so this
    migration stays frozen if the model evolves later.
    """
    op.create_index(
        "ix_job_series_end",
        "job_series",
        [END_EXPR],
        postgresql_include=COVERING,
        postgresql_where=sa.text(ELIGIBILITY),
    )
    op.create_index(
        "ix_job_series_user",
        "job_series",
        ["sarc_user_id", END_EXPR],
        postgresql_include=[c for c in COVERING if c != "sarc_user_id"],
        postgresql_where=sa.text(ELIGIBILITY),
    )
    op.create_index(
        "ix_job_series_submit",
        "job_series",
        ["submit_time"],
        postgresql_include=SUBMIT_COVERING,
        postgresql_where=sa.text(ELIGIBILITY),
    )
    # ANALYZE-stats twin of ix_job_series_end (a partial index's expression
    # stats aren't used to plan a scan of itself; see ix_slurm_jobs_end_gpu_stats).
    op.create_index("ix_job_series_end_stats", "job_series", [END_EXPR])
    op.create_index("ix_job_series_submit_time", "job_series", ["submit_time"])
    op.create_index("ix_job_series_end_time", "job_series", ["end_time"])
    op.create_index("ix_job_series_job_id", "job_series", ["job_id"])
    op.create_index("ix_job_series_sarc_user_id", "job_series", ["sarc_user_id"])
    op.create_index("ix_job_series_email", "job_series", ["email"])


def _create_functions_and_triggers() -> None:
    for signature, definition in JOB_SERIES_FUNCTIONS.items():
        op.create_entity(
            PGFunction(schema="public", signature=signature, definition=definition)
        )
    for name, (table_name, definition) in JOB_SERIES_TRIGGERS.items():
        op.create_entity(
            PGTrigger(
                schema="public",
                signature=name,
                definition=definition,
                on_entity=f"public.{table_name}",
                is_constraint=False,
            )
        )


def _drop_functions_and_triggers() -> None:
    for name, (table_name, definition) in JOB_SERIES_TRIGGERS.items():
        op.drop_entity(
            PGTrigger(
                schema="public",
                signature=name,
                definition=definition,
                on_entity=f"public.{table_name}",
                is_constraint=False,
            )
        )
    for signature, definition in JOB_SERIES_FUNCTIONS.items():
        op.drop_entity(
            PGFunction(schema="public", signature=signature, definition=definition)
        )


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "job_series",
        sa.Column("job_db_id", sa.Integer(), nullable=False),
        sa.Column("cluster_id", sa.Integer(), nullable=False),
        sa.Column("account", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("job_id", sa.Integer(), nullable=False),
        sa.Column("array_job_id", sa.Integer(), nullable=True),
        sa.Column("task_id", sa.Integer(), nullable=True),
        sa.Column("name", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("cluster_user", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("group", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column(
            "job_state",
            PGEnum(
                "BOOT_FAIL",
                "CANCELLED",
                "COMPLETED",
                "CONFIGURING",
                "COMPLETING",
                "DEADLINE",
                "FAILED",
                "NODE_FAIL",
                "OUT_OF_MEMORY",
                "PENDING",
                "PREEMPTED",
                "RUNNING",
                "RESV_DEL_HOLD",
                "REQUEUE_FED",
                "REQUEUE_HOLD",
                "REQUEUED",
                "RESIZING",
                "REVOKED",
                "SIGNALING",
                "SPECIAL_EXIT",
                "STAGE_OUT",
                "STOPPED",
                "SUSPENDED",
                "TIMEOUT",
                name="slurmstate",
                create_type=False,
            ),
            nullable=False,
        ),
        sa.Column("exit_code", sa.Integer(), nullable=True),
        sa.Column("signal", sa.Integer(), nullable=True),
        sa.Column("partition", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("nodes", JSONB(), nullable=False),
        sa.Column("work_dir", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("submit_line", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column("constraints", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column("priority", sa.Integer(), nullable=True),
        sa.Column("qos", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column("CLEAR_SCHEDULING", sa.Boolean(), nullable=False),
        sa.Column("STARTED_ON_SUBMIT", sa.Boolean(), nullable=False),
        sa.Column("STARTED_ON_SCHEDULE", sa.Boolean(), nullable=False),
        sa.Column("STARTED_ON_BACKFILL", sa.Boolean(), nullable=False),
        sa.Column("time_limit", sa.Integer(), nullable=True),
        sa.Column("submit_time", sarc.db.sqlmodel.UTCDateTime(), nullable=False),
        sa.Column("start_time", sarc.db.sqlmodel.UTCDateTime(), nullable=True),
        sa.Column("end_time", sarc.db.sqlmodel.UTCDateTime(), nullable=True),
        sa.Column("elapsed_time", sa.Float(), nullable=False),
        sa.Column("requested_cpu", sa.BIGINT(), nullable=True),
        sa.Column("requested_mem", sa.BIGINT(), nullable=True),
        sa.Column("requested_node", sa.BIGINT(), nullable=True),
        sa.Column("requested_billing", sa.BIGINT(), nullable=True),
        sa.Column("requested_gres_gpu", sa.BIGINT(), nullable=True),
        sa.Column(
            "requested_gpu_type", sqlmodel.sql.sqltypes.AutoString(), nullable=True
        ),
        sa.Column("allocated_cpu", sa.BIGINT(), nullable=True),
        sa.Column("allocated_mem", sa.BIGINT(), nullable=True),
        sa.Column("allocated_node", sa.BIGINT(), nullable=True),
        sa.Column("allocated_billing", sa.BIGINT(), nullable=True),
        sa.Column("allocated_gres_gpu", sa.BIGINT(), nullable=True),
        sa.Column(
            "allocated_gpu_type", sqlmodel.sql.sqltypes.AutoString(), nullable=True
        ),
        sa.Column(
            "harmonized_gpu_type", sqlmodel.sql.sqltypes.AutoString(), nullable=True
        ),
        sa.Column("sarc_user_id", sa.Integer(), nullable=False),
        sa.Column("display_name", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("email", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("cluster_name", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column("gpu_type_rgu", sa.Float(), nullable=True),
        sa.Column("gpu_type_rgu_drac", sa.Float(), nullable=True),
        sa.Column("requested_rgu", sa.Float(), nullable=True),
        sa.Column("requested_rgu_drac", sa.Float(), nullable=True),
        sa.Column("allocated_rgu", sa.Float(), nullable=True),
        sa.Column("allocated_rgu_drac", sa.Float(), nullable=True),
        sa.Column("requested_cpu_cost", sa.Float(), nullable=True),
        sa.Column("requested_cpu_waste", sa.Float(), nullable=True),
        sa.Column("allocated_cpu_cost", sa.Float(), nullable=True),
        sa.Column("allocated_cpu_waste", sa.Float(), nullable=True),
        sa.Column("cpu_overbilling_cost", sa.Float(), nullable=True),
        sa.Column("requested_gpu_cost", sa.Float(), nullable=True),
        sa.Column("requested_gpu_waste", sa.Float(), nullable=True),
        sa.Column("allocated_gpu_cost", sa.Float(), nullable=True),
        sa.Column("allocated_gpu_waste", sa.Float(), nullable=True),
        sa.Column("gpu_overbilling_cost", sa.Float(), nullable=True),
        sa.Column("gpu_sm_occupancy_mean", sa.Float(), nullable=True),
        sa.Column("gpu_sm_occupancy_max", sa.Float(), nullable=True),
        sa.Column("gpu_utilization_mean", sa.Float(), nullable=True),
        sa.Column("gpu_utilization_max", sa.Float(), nullable=True),
        sa.Column("gpu_utilization_fp16_mean", sa.Float(), nullable=True),
        sa.Column("gpu_utilization_fp16_max", sa.Float(), nullable=True),
        sa.Column("gpu_utilization_fp32_mean", sa.Float(), nullable=True),
        sa.Column("gpu_utilization_fp32_max", sa.Float(), nullable=True),
        sa.Column("gpu_utilization_fp64_mean", sa.Float(), nullable=True),
        sa.Column("gpu_utilization_fp64_max", sa.Float(), nullable=True),
        sa.Column("gpu_memory_mean", sa.Float(), nullable=True),
        sa.Column("gpu_memory_max", sa.Float(), nullable=True),
        sa.Column("system_memory_mean", sa.Float(), nullable=True),
        sa.Column("system_memory_max", sa.Float(), nullable=True),
        sa.Column("cpu_utilization_mean", sa.Float(), nullable=True),
        sa.ForeignKeyConstraint(["job_db_id"], ["slurm_jobs.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("job_db_id"),
    )
    _create_indexes()

    # The wrapper needs the table, so the view swap comes after it.
    op.drop_entity(
        PGView(
            schema="public",
            signature="job_series_view",
            definition=LEGACY_JOB_SERIES_VIEW,
        )
    )
    op.create_entity(
        PGView(
            schema="public",
            signature="job_series_view",
            definition=JOB_SERIES_WRAPPER_VIEW,
        )
    )
    _create_functions_and_triggers()


def downgrade() -> None:
    """Downgrade schema."""
    _drop_functions_and_triggers()
    op.drop_entity(
        PGView(
            schema="public",
            signature="job_series_view",
            definition=JOB_SERIES_WRAPPER_VIEW,
        )
    )
    op.create_entity(
        PGView(
            schema="public",
            signature="job_series_view",
            definition=LEGACY_JOB_SERIES_VIEW,
        )
    )
    for name in (
        "ix_job_series_email",
        "ix_job_series_sarc_user_id",
        "ix_job_series_job_id",
        "ix_job_series_end_time",
        "ix_job_series_submit_time",
        "ix_job_series_end_stats",
        "ix_job_series_submit",
        "ix_job_series_user",
        "ix_job_series_end",
    ):
        op.drop_index(name, table_name="job_series")
    op.drop_table("job_series")
