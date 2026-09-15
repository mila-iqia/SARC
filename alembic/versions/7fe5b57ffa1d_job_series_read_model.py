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

# The trigger functions and triggers, frozen at this migration's creation time.
# They embed the slurm_jobs/job_series column lists, so importing them from
# sarc.db.job_series would silently change this migration whenever the model
# evolves (a later column addition would make fresh-history upgrades create
# triggers over columns that do not exist yet). ba029bf00184 is the migration
# that swaps the slurm_jobs_job_series trigger and job_series_sync_job() for
# its own, column-list-updated versions.
JOB_SERIES_FUNCTIONS = {
    "job_series_sync_job()": """
returns trigger
language plpgsql
as $$
begin
    insert into job_series (job_db_id, cluster_id, account, job_id, array_job_id, task_id, name, cluster_user, "group", job_state, exit_code, signal, partition, nodes, work_dir, submit_line, constraints, priority, qos, "CLEAR_SCHEDULING", "STARTED_ON_SUBMIT", "STARTED_ON_SCHEDULE", "STARTED_ON_BACKFILL", time_limit, submit_time, start_time, end_time, elapsed_time, requested_cpu, requested_mem, requested_node, requested_billing, requested_gres_gpu, requested_gpu_type, allocated_cpu, allocated_mem, allocated_node, allocated_billing, allocated_gres_gpu, allocated_gpu_type, harmonized_gpu_type, sarc_user_id, display_name, email, cluster_name, gpu_type_rgu, gpu_type_rgu_drac, requested_rgu, requested_rgu_drac, allocated_rgu, allocated_rgu_drac, requested_cpu_cost, requested_cpu_waste, allocated_cpu_cost, allocated_cpu_waste, cpu_overbilling_cost, requested_gpu_cost, requested_gpu_waste, allocated_gpu_cost, allocated_gpu_waste, gpu_overbilling_cost, gpu_sm_occupancy_mean, gpu_sm_occupancy_max, gpu_utilization_mean, gpu_utilization_max, gpu_utilization_fp16_mean, gpu_utilization_fp16_max, gpu_utilization_fp32_mean, gpu_utilization_fp32_max, gpu_utilization_fp64_mean, gpu_utilization_fp64_max, gpu_memory_mean, gpu_memory_max, system_memory_mean, system_memory_max, cpu_utilization_mean)
    select new.id, new.cluster_id, new.account, new.job_id, new.array_job_id, new.task_id, new.name, new.cluster_user, new."group", new.job_state, new.exit_code, new.signal, new.partition, new.nodes, new.work_dir, new.submit_line, new.constraints, new.priority, new.qos, new."CLEAR_SCHEDULING", new."STARTED_ON_SUBMIT", new."STARTED_ON_SCHEDULE", new."STARTED_ON_BACKFILL", new.time_limit, new.submit_time, new.start_time, new.end_time, new.elapsed_time, new.requested_cpu, new.requested_mem, new.requested_node, new.requested_billing, new.requested_gres_gpu, new.requested_gpu_type, new.allocated_cpu, new.allocated_mem, new.allocated_node, new.allocated_billing, new.allocated_gres_gpu, new.allocated_gpu_type, new.harmonized_gpu_type, new.sarc_user_id,
           u.display_name, u.email,
           c.name,
           w.rgu, w.drac_rgu, coalesce(new.requested_gres_gpu, 0) * w.rgu, coalesce(new.requested_gres_gpu, 0) * w.drac_rgu, coalesce(new.allocated_gres_gpu, 0) * w.rgu, coalesce(new.allocated_gres_gpu, 0) * w.drac_rgu, new.elapsed_time * new.requested_cpu, (1 - s.cpu_utilization_mean) * (new.elapsed_time * new.requested_cpu), new.elapsed_time * new.allocated_cpu, (1 - s.cpu_utilization_mean) * (new.elapsed_time * new.allocated_cpu), new.elapsed_time * (new.allocated_cpu - new.requested_cpu), new.elapsed_time * new.requested_gres_gpu * w.drac_rgu, (1 - s.gpu_sm_occupancy_mean) * (new.elapsed_time * new.requested_gres_gpu * w.drac_rgu), new.elapsed_time * new.allocated_gres_gpu * w.drac_rgu, (1 - s.gpu_sm_occupancy_mean) * (new.elapsed_time * new.allocated_gres_gpu * w.drac_rgu), new.elapsed_time * (new.allocated_gres_gpu - new.requested_gres_gpu) * w.drac_rgu,
           s.gpu_sm_occupancy_mean, s.gpu_sm_occupancy_max, s.gpu_utilization_mean, s.gpu_utilization_max, s.gpu_utilization_fp16_mean, s.gpu_utilization_fp16_max, s.gpu_utilization_fp32_mean, s.gpu_utilization_fp32_max, s.gpu_utilization_fp64_mean, s.gpu_utilization_fp64_max, s.gpu_memory_mean, s.gpu_memory_max, s.system_memory_mean, s.system_memory_max, s.cpu_utilization_mean
      from (select 1) x
      left join users u on u.id = new.sarc_user_id
      left join clusters c on c.id = new.cluster_id
      left join gpurgudb w on w.name = new.harmonized_gpu_type
      left join lateral (select max(st.mean) FILTER (WHERE st.name = 'gpu_sm_occupancy') AS gpu_sm_occupancy_mean,
                max(st.max) FILTER (WHERE st.name = 'gpu_sm_occupancy') AS gpu_sm_occupancy_max,
                max(st.mean) FILTER (WHERE st.name = 'gpu_utilization') AS gpu_utilization_mean,
                max(st.max) FILTER (WHERE st.name = 'gpu_utilization') AS gpu_utilization_max,
                max(st.mean) FILTER (WHERE st.name = 'gpu_utilization_fp16') AS gpu_utilization_fp16_mean,
                max(st.max) FILTER (WHERE st.name = 'gpu_utilization_fp16') AS gpu_utilization_fp16_max,
                max(st.mean) FILTER (WHERE st.name = 'gpu_utilization_fp32') AS gpu_utilization_fp32_mean,
                max(st.max) FILTER (WHERE st.name = 'gpu_utilization_fp32') AS gpu_utilization_fp32_max,
                max(st.mean) FILTER (WHERE st.name = 'gpu_utilization_fp64') AS gpu_utilization_fp64_mean,
                max(st.max) FILTER (WHERE st.name = 'gpu_utilization_fp64') AS gpu_utilization_fp64_max,
                max(st.mean) FILTER (WHERE st.name = 'gpu_memory') AS gpu_memory_mean,
                max(st.max) FILTER (WHERE st.name = 'gpu_memory') AS gpu_memory_max,
                max(st.mean) FILTER (WHERE st.name = 'system_memory') AS system_memory_mean,
                max(st.max) FILTER (WHERE st.name = 'system_memory') AS system_memory_max,
                max(st.mean) FILTER (WHERE st.name = 'cpu_utilization') AS cpu_utilization_mean from jobstatisticdb st where st.job_id = new.id and st.name in ('gpu_sm_occupancy', 'gpu_utilization', 'gpu_utilization_fp16', 'gpu_utilization_fp32', 'gpu_utilization_fp64', 'gpu_memory', 'system_memory', 'cpu_utilization')) s on true
    on conflict (job_db_id) do update set
        cluster_id = excluded.cluster_id, account = excluded.account, job_id = excluded.job_id, array_job_id = excluded.array_job_id, task_id = excluded.task_id, name = excluded.name, cluster_user = excluded.cluster_user, "group" = excluded."group", job_state = excluded.job_state, exit_code = excluded.exit_code, signal = excluded.signal, partition = excluded.partition, nodes = excluded.nodes, work_dir = excluded.work_dir, submit_line = excluded.submit_line, constraints = excluded.constraints, priority = excluded.priority, qos = excluded.qos, "CLEAR_SCHEDULING" = excluded."CLEAR_SCHEDULING", "STARTED_ON_SUBMIT" = excluded."STARTED_ON_SUBMIT", "STARTED_ON_SCHEDULE" = excluded."STARTED_ON_SCHEDULE", "STARTED_ON_BACKFILL" = excluded."STARTED_ON_BACKFILL", time_limit = excluded.time_limit, submit_time = excluded.submit_time, start_time = excluded.start_time, end_time = excluded.end_time, elapsed_time = excluded.elapsed_time, requested_cpu = excluded.requested_cpu, requested_mem = excluded.requested_mem, requested_node = excluded.requested_node, requested_billing = excluded.requested_billing, requested_gres_gpu = excluded.requested_gres_gpu, requested_gpu_type = excluded.requested_gpu_type, allocated_cpu = excluded.allocated_cpu, allocated_mem = excluded.allocated_mem, allocated_node = excluded.allocated_node, allocated_billing = excluded.allocated_billing, allocated_gres_gpu = excluded.allocated_gres_gpu, allocated_gpu_type = excluded.allocated_gpu_type, harmonized_gpu_type = excluded.harmonized_gpu_type, sarc_user_id = excluded.sarc_user_id, display_name = excluded.display_name, email = excluded.email, cluster_name = excluded.cluster_name, gpu_type_rgu = excluded.gpu_type_rgu, gpu_type_rgu_drac = excluded.gpu_type_rgu_drac, requested_rgu = excluded.requested_rgu, requested_rgu_drac = excluded.requested_rgu_drac, allocated_rgu = excluded.allocated_rgu, allocated_rgu_drac = excluded.allocated_rgu_drac, requested_cpu_cost = excluded.requested_cpu_cost, requested_cpu_waste = excluded.requested_cpu_waste, allocated_cpu_cost = excluded.allocated_cpu_cost, allocated_cpu_waste = excluded.allocated_cpu_waste, cpu_overbilling_cost = excluded.cpu_overbilling_cost, requested_gpu_cost = excluded.requested_gpu_cost, requested_gpu_waste = excluded.requested_gpu_waste, allocated_gpu_cost = excluded.allocated_gpu_cost, allocated_gpu_waste = excluded.allocated_gpu_waste, gpu_overbilling_cost = excluded.gpu_overbilling_cost, gpu_sm_occupancy_mean = excluded.gpu_sm_occupancy_mean, gpu_sm_occupancy_max = excluded.gpu_sm_occupancy_max, gpu_utilization_mean = excluded.gpu_utilization_mean, gpu_utilization_max = excluded.gpu_utilization_max, gpu_utilization_fp16_mean = excluded.gpu_utilization_fp16_mean, gpu_utilization_fp16_max = excluded.gpu_utilization_fp16_max, gpu_utilization_fp32_mean = excluded.gpu_utilization_fp32_mean, gpu_utilization_fp32_max = excluded.gpu_utilization_fp32_max, gpu_utilization_fp64_mean = excluded.gpu_utilization_fp64_mean, gpu_utilization_fp64_max = excluded.gpu_utilization_fp64_max, gpu_memory_mean = excluded.gpu_memory_mean, gpu_memory_max = excluded.gpu_memory_max, system_memory_mean = excluded.system_memory_mean, system_memory_max = excluded.system_memory_max, cpu_utilization_mean = excluded.cpu_utilization_mean;
    return new;
end;
$$""",
    "job_series_sync_stat()": """
returns trigger
language plpgsql
as $$
begin
    if new.name not in ('gpu_sm_occupancy', 'gpu_utilization', 'gpu_utilization_fp16', 'gpu_utilization_fp32', 'gpu_utilization_fp64', 'gpu_memory', 'system_memory', 'cpu_utilization') then
        return new;
    end if;
    update job_series set
    gpu_sm_occupancy_mean = case when new.name = 'gpu_sm_occupancy' then new.mean
                   else job_series.gpu_sm_occupancy_mean end,
    gpu_sm_occupancy_max = case when new.name = 'gpu_sm_occupancy' then new.max
                   else job_series.gpu_sm_occupancy_max end,
    gpu_utilization_mean = case when new.name = 'gpu_utilization' then new.mean
                   else job_series.gpu_utilization_mean end,
    gpu_utilization_max = case when new.name = 'gpu_utilization' then new.max
                   else job_series.gpu_utilization_max end,
    gpu_utilization_fp16_mean = case when new.name = 'gpu_utilization_fp16' then new.mean
                   else job_series.gpu_utilization_fp16_mean end,
    gpu_utilization_fp16_max = case when new.name = 'gpu_utilization_fp16' then new.max
                   else job_series.gpu_utilization_fp16_max end,
    gpu_utilization_fp32_mean = case when new.name = 'gpu_utilization_fp32' then new.mean
                   else job_series.gpu_utilization_fp32_mean end,
    gpu_utilization_fp32_max = case when new.name = 'gpu_utilization_fp32' then new.max
                   else job_series.gpu_utilization_fp32_max end,
    gpu_utilization_fp64_mean = case when new.name = 'gpu_utilization_fp64' then new.mean
                   else job_series.gpu_utilization_fp64_mean end,
    gpu_utilization_fp64_max = case when new.name = 'gpu_utilization_fp64' then new.max
                   else job_series.gpu_utilization_fp64_max end,
    gpu_memory_mean = case when new.name = 'gpu_memory' then new.mean
                   else job_series.gpu_memory_mean end,
    gpu_memory_max = case when new.name = 'gpu_memory' then new.max
                   else job_series.gpu_memory_max end,
    system_memory_mean = case when new.name = 'system_memory' then new.mean
                   else job_series.system_memory_mean end,
    system_memory_max = case when new.name = 'system_memory' then new.max
                   else job_series.system_memory_max end,
    cpu_utilization_mean = case when new.name = 'cpu_utilization' then new.mean
                   else job_series.cpu_utilization_mean end
    where job_db_id = new.job_id;
    if new.name in ('cpu_utilization', 'gpu_sm_occupancy') then
        update job_series set
    requested_cpu_waste = (1 - cpu_utilization_mean) * requested_cpu_cost,
    allocated_cpu_waste = (1 - cpu_utilization_mean) * allocated_cpu_cost,
    requested_gpu_waste = (1 - gpu_sm_occupancy_mean) * requested_gpu_cost,
    allocated_gpu_waste = (1 - gpu_sm_occupancy_mean) * allocated_gpu_cost
        where job_db_id = new.job_id;
    end if;
    return new;
end;
$$""",
    "job_series_clear_stat()": """
returns trigger
language plpgsql
as $$
begin
    if old.name not in ('gpu_sm_occupancy', 'gpu_utilization', 'gpu_utilization_fp16', 'gpu_utilization_fp32', 'gpu_utilization_fp64', 'gpu_memory', 'system_memory', 'cpu_utilization') then
        return old;
    end if;
    update job_series set
    gpu_sm_occupancy_mean = case when old.name = 'gpu_sm_occupancy' then null
                   else job_series.gpu_sm_occupancy_mean end,
    gpu_sm_occupancy_max = case when old.name = 'gpu_sm_occupancy' then null
                   else job_series.gpu_sm_occupancy_max end,
    gpu_utilization_mean = case when old.name = 'gpu_utilization' then null
                   else job_series.gpu_utilization_mean end,
    gpu_utilization_max = case when old.name = 'gpu_utilization' then null
                   else job_series.gpu_utilization_max end,
    gpu_utilization_fp16_mean = case when old.name = 'gpu_utilization_fp16' then null
                   else job_series.gpu_utilization_fp16_mean end,
    gpu_utilization_fp16_max = case when old.name = 'gpu_utilization_fp16' then null
                   else job_series.gpu_utilization_fp16_max end,
    gpu_utilization_fp32_mean = case when old.name = 'gpu_utilization_fp32' then null
                   else job_series.gpu_utilization_fp32_mean end,
    gpu_utilization_fp32_max = case when old.name = 'gpu_utilization_fp32' then null
                   else job_series.gpu_utilization_fp32_max end,
    gpu_utilization_fp64_mean = case when old.name = 'gpu_utilization_fp64' then null
                   else job_series.gpu_utilization_fp64_mean end,
    gpu_utilization_fp64_max = case when old.name = 'gpu_utilization_fp64' then null
                   else job_series.gpu_utilization_fp64_max end,
    gpu_memory_mean = case when old.name = 'gpu_memory' then null
                   else job_series.gpu_memory_mean end,
    gpu_memory_max = case when old.name = 'gpu_memory' then null
                   else job_series.gpu_memory_max end,
    system_memory_mean = case when old.name = 'system_memory' then null
                   else job_series.system_memory_mean end,
    system_memory_max = case when old.name = 'system_memory' then null
                   else job_series.system_memory_max end,
    cpu_utilization_mean = case when old.name = 'cpu_utilization' then null
                   else job_series.cpu_utilization_mean end
    where job_db_id = old.job_id;
    if old.name in ('cpu_utilization', 'gpu_sm_occupancy') then
        update job_series set
    requested_cpu_waste = (1 - cpu_utilization_mean) * requested_cpu_cost,
    allocated_cpu_waste = (1 - cpu_utilization_mean) * allocated_cpu_cost,
    requested_gpu_waste = (1 - gpu_sm_occupancy_mean) * requested_gpu_cost,
    allocated_gpu_waste = (1 - gpu_sm_occupancy_mean) * allocated_gpu_cost
        where job_db_id = old.job_id;
    end if;
    return old;
end;
$$""",
    "job_series_sync_weights()": """
returns trigger
language plpgsql
as $$
begin
    update job_series set
    gpu_type_rgu = g.rgu,
    gpu_type_rgu_drac = g.drac_rgu,
    requested_rgu = coalesce(job_series.requested_gres_gpu, 0) * g.rgu,
    requested_rgu_drac = coalesce(job_series.requested_gres_gpu, 0) * g.drac_rgu,
    allocated_rgu = coalesce(job_series.allocated_gres_gpu, 0) * g.rgu,
    allocated_rgu_drac = coalesce(job_series.allocated_gres_gpu, 0) * g.drac_rgu,
    requested_cpu_cost = job_series.elapsed_time * job_series.requested_cpu,
    allocated_cpu_cost = job_series.elapsed_time * job_series.allocated_cpu,
    cpu_overbilling_cost = job_series.elapsed_time * (job_series.allocated_cpu - job_series.requested_cpu),
    requested_gpu_cost = job_series.elapsed_time * job_series.requested_gres_gpu * g.drac_rgu,
    requested_gpu_waste = (1 - job_series.gpu_sm_occupancy_mean) * (job_series.elapsed_time * job_series.requested_gres_gpu * g.drac_rgu),
    allocated_gpu_cost = job_series.elapsed_time * job_series.allocated_gres_gpu * g.drac_rgu,
    allocated_gpu_waste = (1 - job_series.gpu_sm_occupancy_mean) * (job_series.elapsed_time * job_series.allocated_gres_gpu * g.drac_rgu),
    gpu_overbilling_cost = job_series.elapsed_time * (job_series.allocated_gres_gpu - job_series.requested_gres_gpu) * g.drac_rgu
      from gpurgudb g
     where job_series.harmonized_gpu_type = g.name
       and (job_series.gpu_type_rgu_drac is distinct from g.drac_rgu
            or job_series.gpu_type_rgu is distinct from g.rgu);
    return null;
end;
$$""",
    "job_series_sync_user()": """
returns trigger
language plpgsql
as $$
begin
    if new.display_name is distinct from old.display_name
       or new.email is distinct from old.email then
        update job_series
           set display_name = new.display_name, email = new.email
         where sarc_user_id = new.id;
    end if;
    return new;
end;
$$""",
    "job_series_sync_cluster()": """
returns trigger
language plpgsql
as $$
begin
    if new.name is distinct from old.name then
        update job_series set cluster_name = new.name where cluster_id = new.id;
    end if;
    return null;
end;
$$""",
}

JOB_SERIES_TRIGGERS = {
    "slurm_jobs_job_series": (
        "slurm_jobs",
        """AFTER INSERT OR UPDATE OF cluster_id, account, job_id, array_job_id, task_id, name, cluster_user, "group", job_state, exit_code, signal, partition, nodes, work_dir, submit_line, constraints, priority, qos, "CLEAR_SCHEDULING", "STARTED_ON_SUBMIT", "STARTED_ON_SCHEDULE", "STARTED_ON_BACKFILL", time_limit, submit_time, start_time, end_time, elapsed_time, requested_cpu, requested_mem, requested_node, requested_billing, requested_gres_gpu, requested_gpu_type, allocated_cpu, allocated_mem, allocated_node, allocated_billing, allocated_gres_gpu, allocated_gpu_type, harmonized_gpu_type, sarc_user_id ON slurm_jobs FOR EACH ROW EXECUTE FUNCTION job_series_sync_job()""",
    ),
    "jobstatisticdb_job_series": (
        "jobstatisticdb",
        """AFTER INSERT OR UPDATE OF mean, max ON jobstatisticdb FOR EACH ROW EXECUTE FUNCTION job_series_sync_stat()""",
    ),
    "jobstatisticdb_job_series_del": (
        "jobstatisticdb",
        """AFTER DELETE ON jobstatisticdb FOR EACH ROW EXECUTE FUNCTION job_series_clear_stat()""",
    ),
    "gpurgudb_job_series": (
        "gpurgudb",
        """AFTER UPDATE OF rgu, drac_rgu ON gpurgudb FOR EACH STATEMENT EXECUTE FUNCTION job_series_sync_weights()""",
    ),
    "users_job_series": (
        "users",
        """AFTER UPDATE OF display_name, email ON users FOR EACH ROW EXECUTE FUNCTION job_series_sync_user()""",
    ),
    "clusters_job_series": (
        "clusters",
        """AFTER UPDATE OF name ON clusters FOR EACH ROW EXECUTE FUNCTION job_series_sync_cluster()""",
    ),
}


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
