"""Add the reservation field to slurm jobs

Revision ID: ba029bf00184
Revises: 3ec1f2451f71
Create Date: 2026-09-15 15:48:10.082871+00:00

Adds slurm_jobs.reservation (the reservation name a job ran within, from
sacct's reservation.name / fastsacct's resv_name) and mirrors it into the
job_series read model: the column, the sync_job trigger function and its
trigger's UPDATE OF column list, and the job_series_view wrapper. Existing
rows keep NULL (the reservation of past jobs was never recorded).

"""

from typing import Sequence, Union

import sqlalchemy as sa
import sqlmodel.sql.sqltypes
from alembic_utils.pg_function import PGFunction
from alembic_utils.pg_trigger import PGTrigger
from alembic_utils.pg_view import PGView

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "ba029bf00184"
down_revision: Union[str, Sequence[str], None] = "3ec1f2451f71"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# The job_series machinery touched by this migration, frozen at the pre- and
# post-change state (the definitions job_series.py generated then; see
# 7fe5b57ffa1d for their role).
_OLD_SYNC_JOB = """
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
$$"""
_NEW_SYNC_JOB = """
returns trigger
language plpgsql
as $$
begin
    insert into job_series (job_db_id, cluster_id, account, job_id, array_job_id, task_id, name, cluster_user, "group", job_state, exit_code, signal, partition, nodes, work_dir, submit_line, constraints, priority, qos, reservation, "CLEAR_SCHEDULING", "STARTED_ON_SUBMIT", "STARTED_ON_SCHEDULE", "STARTED_ON_BACKFILL", time_limit, submit_time, start_time, end_time, elapsed_time, requested_cpu, requested_mem, requested_node, requested_billing, requested_gres_gpu, requested_gpu_type, allocated_cpu, allocated_mem, allocated_node, allocated_billing, allocated_gres_gpu, allocated_gpu_type, harmonized_gpu_type, sarc_user_id, display_name, email, cluster_name, gpu_type_rgu, gpu_type_rgu_drac, requested_rgu, requested_rgu_drac, allocated_rgu, allocated_rgu_drac, requested_cpu_cost, requested_cpu_waste, allocated_cpu_cost, allocated_cpu_waste, cpu_overbilling_cost, requested_gpu_cost, requested_gpu_waste, allocated_gpu_cost, allocated_gpu_waste, gpu_overbilling_cost, gpu_sm_occupancy_mean, gpu_sm_occupancy_max, gpu_utilization_mean, gpu_utilization_max, gpu_utilization_fp16_mean, gpu_utilization_fp16_max, gpu_utilization_fp32_mean, gpu_utilization_fp32_max, gpu_utilization_fp64_mean, gpu_utilization_fp64_max, gpu_memory_mean, gpu_memory_max, system_memory_mean, system_memory_max, cpu_utilization_mean)
    select new.id, new.cluster_id, new.account, new.job_id, new.array_job_id, new.task_id, new.name, new.cluster_user, new."group", new.job_state, new.exit_code, new.signal, new.partition, new.nodes, new.work_dir, new.submit_line, new.constraints, new.priority, new.qos, new.reservation, new."CLEAR_SCHEDULING", new."STARTED_ON_SUBMIT", new."STARTED_ON_SCHEDULE", new."STARTED_ON_BACKFILL", new.time_limit, new.submit_time, new.start_time, new.end_time, new.elapsed_time, new.requested_cpu, new.requested_mem, new.requested_node, new.requested_billing, new.requested_gres_gpu, new.requested_gpu_type, new.allocated_cpu, new.allocated_mem, new.allocated_node, new.allocated_billing, new.allocated_gres_gpu, new.allocated_gpu_type, new.harmonized_gpu_type, new.sarc_user_id,
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
        cluster_id = excluded.cluster_id, account = excluded.account, job_id = excluded.job_id, array_job_id = excluded.array_job_id, task_id = excluded.task_id, name = excluded.name, cluster_user = excluded.cluster_user, "group" = excluded."group", job_state = excluded.job_state, exit_code = excluded.exit_code, signal = excluded.signal, partition = excluded.partition, nodes = excluded.nodes, work_dir = excluded.work_dir, submit_line = excluded.submit_line, constraints = excluded.constraints, priority = excluded.priority, qos = excluded.qos, reservation = excluded.reservation, "CLEAR_SCHEDULING" = excluded."CLEAR_SCHEDULING", "STARTED_ON_SUBMIT" = excluded."STARTED_ON_SUBMIT", "STARTED_ON_SCHEDULE" = excluded."STARTED_ON_SCHEDULE", "STARTED_ON_BACKFILL" = excluded."STARTED_ON_BACKFILL", time_limit = excluded.time_limit, submit_time = excluded.submit_time, start_time = excluded.start_time, end_time = excluded.end_time, elapsed_time = excluded.elapsed_time, requested_cpu = excluded.requested_cpu, requested_mem = excluded.requested_mem, requested_node = excluded.requested_node, requested_billing = excluded.requested_billing, requested_gres_gpu = excluded.requested_gres_gpu, requested_gpu_type = excluded.requested_gpu_type, allocated_cpu = excluded.allocated_cpu, allocated_mem = excluded.allocated_mem, allocated_node = excluded.allocated_node, allocated_billing = excluded.allocated_billing, allocated_gres_gpu = excluded.allocated_gres_gpu, allocated_gpu_type = excluded.allocated_gpu_type, harmonized_gpu_type = excluded.harmonized_gpu_type, sarc_user_id = excluded.sarc_user_id, display_name = excluded.display_name, email = excluded.email, cluster_name = excluded.cluster_name, gpu_type_rgu = excluded.gpu_type_rgu, gpu_type_rgu_drac = excluded.gpu_type_rgu_drac, requested_rgu = excluded.requested_rgu, requested_rgu_drac = excluded.requested_rgu_drac, allocated_rgu = excluded.allocated_rgu, allocated_rgu_drac = excluded.allocated_rgu_drac, requested_cpu_cost = excluded.requested_cpu_cost, requested_cpu_waste = excluded.requested_cpu_waste, allocated_cpu_cost = excluded.allocated_cpu_cost, allocated_cpu_waste = excluded.allocated_cpu_waste, cpu_overbilling_cost = excluded.cpu_overbilling_cost, requested_gpu_cost = excluded.requested_gpu_cost, requested_gpu_waste = excluded.requested_gpu_waste, allocated_gpu_cost = excluded.allocated_gpu_cost, allocated_gpu_waste = excluded.allocated_gpu_waste, gpu_overbilling_cost = excluded.gpu_overbilling_cost, gpu_sm_occupancy_mean = excluded.gpu_sm_occupancy_mean, gpu_sm_occupancy_max = excluded.gpu_sm_occupancy_max, gpu_utilization_mean = excluded.gpu_utilization_mean, gpu_utilization_max = excluded.gpu_utilization_max, gpu_utilization_fp16_mean = excluded.gpu_utilization_fp16_mean, gpu_utilization_fp16_max = excluded.gpu_utilization_fp16_max, gpu_utilization_fp32_mean = excluded.gpu_utilization_fp32_mean, gpu_utilization_fp32_max = excluded.gpu_utilization_fp32_max, gpu_utilization_fp64_mean = excluded.gpu_utilization_fp64_mean, gpu_utilization_fp64_max = excluded.gpu_utilization_fp64_max, gpu_memory_mean = excluded.gpu_memory_mean, gpu_memory_max = excluded.gpu_memory_max, system_memory_mean = excluded.system_memory_mean, system_memory_max = excluded.system_memory_max, cpu_utilization_mean = excluded.cpu_utilization_mean;
    return new;
end;
$$"""
_OLD_TRIGGER_DEF = """AFTER INSERT OR UPDATE OF cluster_id, account, job_id, array_job_id, task_id, name, cluster_user, "group", job_state, exit_code, signal, partition, nodes, work_dir, submit_line, constraints, priority, qos, "CLEAR_SCHEDULING", "STARTED_ON_SUBMIT", "STARTED_ON_SCHEDULE", "STARTED_ON_BACKFILL", time_limit, submit_time, start_time, end_time, elapsed_time, requested_cpu, requested_mem, requested_node, requested_billing, requested_gres_gpu, requested_gpu_type, allocated_cpu, allocated_mem, allocated_node, allocated_billing, allocated_gres_gpu, allocated_gpu_type, harmonized_gpu_type, sarc_user_id ON slurm_jobs FOR EACH ROW EXECUTE FUNCTION job_series_sync_job()"""
_NEW_TRIGGER_DEF = """AFTER INSERT OR UPDATE OF cluster_id, account, job_id, array_job_id, task_id, name, cluster_user, "group", job_state, exit_code, signal, partition, nodes, work_dir, submit_line, constraints, priority, qos, reservation, "CLEAR_SCHEDULING", "STARTED_ON_SUBMIT", "STARTED_ON_SCHEDULE", "STARTED_ON_BACKFILL", time_limit, submit_time, start_time, end_time, elapsed_time, requested_cpu, requested_mem, requested_node, requested_billing, requested_gres_gpu, requested_gpu_type, allocated_cpu, allocated_mem, allocated_node, allocated_billing, allocated_gres_gpu, allocated_gpu_type, harmonized_gpu_type, sarc_user_id ON slurm_jobs FOR EACH ROW EXECUTE FUNCTION job_series_sync_job()"""
_OLD_VIEW = """SELECT job_series.job_db_id, job_series.cluster_id, job_series.account, job_series.job_id, job_series.array_job_id, job_series.task_id, job_series.name, job_series.cluster_user, job_series."group", job_series.job_state, job_series.exit_code, job_series.signal, job_series.partition, job_series.nodes, job_series.work_dir, job_series.submit_line, job_series.constraints, job_series.priority, job_series.qos, job_series."CLEAR_SCHEDULING", job_series."STARTED_ON_SUBMIT", job_series."STARTED_ON_SCHEDULE", job_series."STARTED_ON_BACKFILL", job_series.time_limit, job_series.submit_time, job_series.start_time, job_series.end_time, job_series.elapsed_time, job_series.requested_cpu, job_series.requested_mem, job_series.requested_node, job_series.requested_billing, job_series.requested_gres_gpu, job_series.requested_gpu_type, job_series.allocated_cpu, job_series.allocated_mem, job_series.allocated_node, job_series.allocated_billing, job_series.allocated_gres_gpu, job_series.allocated_gpu_type, job_series.harmonized_gpu_type, job_series.sarc_user_id, job_series.display_name, job_series.email, job_series.cluster_name, job_series.gpu_type_rgu, job_series.gpu_type_rgu_drac, job_series.requested_rgu, job_series.requested_rgu_drac, job_series.allocated_rgu, job_series.allocated_rgu_drac, job_series.requested_cpu_cost, job_series.requested_cpu_waste, job_series.allocated_cpu_cost, job_series.allocated_cpu_waste, job_series.cpu_overbilling_cost, job_series.requested_gpu_cost, job_series.requested_gpu_waste, job_series.allocated_gpu_cost, job_series.allocated_gpu_waste, job_series.gpu_overbilling_cost, job_series.gpu_sm_occupancy_mean, job_series.gpu_sm_occupancy_max, job_series.gpu_utilization_mean, job_series.gpu_utilization_max, job_series.gpu_utilization_fp16_mean, job_series.gpu_utilization_fp16_max, job_series.gpu_utilization_fp32_mean, job_series.gpu_utilization_fp32_max, job_series.gpu_utilization_fp64_mean, job_series.gpu_utilization_fp64_max, job_series.gpu_memory_mean, job_series.gpu_memory_max, job_series.system_memory_mean, job_series.system_memory_max, job_series.cpu_utilization_mean, job_series.gpu_sm_occupancy_mean AS usage_metric, (SELECT membertypedb.member_type
FROM membertypedb
WHERE membertypedb.user_id = job_series.sarc_user_id AND membertypedb.valid @> job_series.submit_time) AS member_type, (SELECT json_agg(supervisorshelper.supervisor ORDER BY supervisorshelper.pos) AS json_agg_1
FROM user_supervisors JOIN supervisorshelper ON user_supervisors.id = supervisorshelper.list_id
WHERE user_supervisors.user_id = job_series.sarc_user_id AND user_supervisors.valid @> job_series.submit_time) AS supervisors
FROM job_series"""
_NEW_VIEW = """SELECT job_series.job_db_id, job_series.cluster_id, job_series.account, job_series.job_id, job_series.array_job_id, job_series.task_id, job_series.name, job_series.cluster_user, job_series."group", job_series.job_state, job_series.exit_code, job_series.signal, job_series.partition, job_series.nodes, job_series.work_dir, job_series.submit_line, job_series.constraints, job_series.priority, job_series.qos, job_series.reservation, job_series."CLEAR_SCHEDULING", job_series."STARTED_ON_SUBMIT", job_series."STARTED_ON_SCHEDULE", job_series."STARTED_ON_BACKFILL", job_series.time_limit, job_series.submit_time, job_series.start_time, job_series.end_time, job_series.elapsed_time, job_series.requested_cpu, job_series.requested_mem, job_series.requested_node, job_series.requested_billing, job_series.requested_gres_gpu, job_series.requested_gpu_type, job_series.allocated_cpu, job_series.allocated_mem, job_series.allocated_node, job_series.allocated_billing, job_series.allocated_gres_gpu, job_series.allocated_gpu_type, job_series.harmonized_gpu_type, job_series.sarc_user_id, job_series.display_name, job_series.email, job_series.cluster_name, job_series.gpu_type_rgu, job_series.gpu_type_rgu_drac, job_series.requested_rgu, job_series.requested_rgu_drac, job_series.allocated_rgu, job_series.allocated_rgu_drac, job_series.requested_cpu_cost, job_series.requested_cpu_waste, job_series.allocated_cpu_cost, job_series.allocated_cpu_waste, job_series.cpu_overbilling_cost, job_series.requested_gpu_cost, job_series.requested_gpu_waste, job_series.allocated_gpu_cost, job_series.allocated_gpu_waste, job_series.gpu_overbilling_cost, job_series.gpu_sm_occupancy_mean, job_series.gpu_sm_occupancy_max, job_series.gpu_utilization_mean, job_series.gpu_utilization_max, job_series.gpu_utilization_fp16_mean, job_series.gpu_utilization_fp16_max, job_series.gpu_utilization_fp32_mean, job_series.gpu_utilization_fp32_max, job_series.gpu_utilization_fp64_mean, job_series.gpu_utilization_fp64_max, job_series.gpu_memory_mean, job_series.gpu_memory_max, job_series.system_memory_mean, job_series.system_memory_max, job_series.cpu_utilization_mean, job_series.gpu_sm_occupancy_mean AS usage_metric, (SELECT membertypedb.member_type
FROM membertypedb
WHERE membertypedb.user_id = job_series.sarc_user_id AND membertypedb.valid @> job_series.submit_time) AS member_type, (SELECT json_agg(supervisorshelper.supervisor ORDER BY supervisorshelper.pos) AS json_agg_1
FROM user_supervisors JOIN supervisorshelper ON user_supervisors.id = supervisorshelper.list_id
WHERE user_supervisors.user_id = job_series.sarc_user_id AND user_supervisors.valid @> job_series.submit_time) AS supervisors
FROM job_series"""


def _swap_triggers_and_view(new: bool) -> None:
    """Swap sync_job(), its trigger and the wrapper view to the new (or old)
    definitions. Drops go through raw SQL (plain CREATE/DROP); the trigger is
    dropped first, then re-created by PGTrigger with the swapped column list.
    """
    op.execute("DROP TRIGGER slurm_jobs_job_series ON slurm_jobs")
    op.execute("DROP VIEW job_series_view")
    op.create_entity(
        PGView(
            schema="public",
            signature="job_series_view",
            definition=_NEW_VIEW if new else _OLD_VIEW,
        )
    )
    op.replace_entity(
        PGFunction(
            schema="public",
            signature="job_series_sync_job()",
            definition=_NEW_SYNC_JOB if new else _OLD_SYNC_JOB,
        )
    )
    op.create_entity(
        PGTrigger(
            schema="public",
            signature="slurm_jobs_job_series",
            definition=_NEW_TRIGGER_DEF if new else _OLD_TRIGGER_DEF,
            on_entity="public.slurm_jobs",
            is_constraint=False,
        )
    )


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        "slurm_jobs",
        sa.Column("reservation", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
    )
    op.add_column(
        "job_series",
        sa.Column("reservation", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
    )
    _swap_triggers_and_view(new=True)


def downgrade() -> None:
    """Downgrade schema."""
    _swap_triggers_and_view(new=False)
    op.drop_column("job_series", "reservation")
    op.drop_column("slurm_jobs", "reservation")
