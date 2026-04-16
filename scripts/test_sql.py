# ruff: noqa: T201
"""
Read all jobs from the SARC MongoDB jobs collection and upload them to a Cloud SQL
PostgreSQL database via the Cloud SQL Python Connector.

Usage:
    uv run scripts/test_sql.py \\
        --instance project:region:instance \\
        --db-user myuser \\
        --db-password mypassword \\
        --db-name mydb

The SARC config is loaded from the SARC_CONFIG environment variable as usual.
Use --limit to restrict the number of jobs loaded (useful for testing).

Progress is checkpointed after every --batch-size jobs. If the script crashes,
re-running it with the same arguments will resume from the last checkpoint.
Use --drop to start over from scratch.
"""

import argparse
import json
import math
import time
from urllib.parse import urlparse

import pg8000
from google.cloud.sql.connector import Connector

from sarc.client.job import SlurmJob, Statistics, _jobs_collection
from sarc.config import using_sarc_mode

RETRY_DELAY = 5  # seconds between MongoDB reconnection attempts

# nodes is stored as JSON text since pg8000 doesn't auto-cast Python lists to TEXT[]
CREATE_JOBS_TABLE = """
CREATE TABLE IF NOT EXISTS jobs (
    -- MongoDB ID
    mongo_id            TEXT,

    -- Job identification
    cluster_name        TEXT        NOT NULL,
    account             TEXT        NOT NULL,
    job_id              INTEGER     NOT NULL,
    array_job_id        INTEGER,
    task_id             INTEGER,
    name                TEXT        NOT NULL,
    "user"              TEXT        NOT NULL,
    "group"             TEXT        NOT NULL,

    -- Status
    job_state           TEXT        NOT NULL,
    exit_code           INTEGER,
    signal              INTEGER,

    -- Allocation information
    partition           TEXT        NOT NULL,
    nodes               TEXT        NOT NULL,
    work_dir            TEXT        NOT NULL,

    -- Miscellaneous
    constraints         TEXT,
    priority            INTEGER,
    qos                 TEXT,

    -- Flags
    clear_scheduling        BOOLEAN     NOT NULL DEFAULT FALSE,
    started_on_submit       BOOLEAN     NOT NULL DEFAULT FALSE,
    started_on_schedule     BOOLEAN     NOT NULL DEFAULT FALSE,
    started_on_backfill     BOOLEAN     NOT NULL DEFAULT FALSE,

    -- Temporal fields
    time_limit              INTEGER,
    submit_time             TIMESTAMPTZ NOT NULL,
    start_time              TIMESTAMPTZ,
    end_time                TIMESTAMPTZ,
    elapsed_time            DOUBLE PRECISION NOT NULL,

    -- Requested resources
    req_cpu             BIGINT,
    req_mem             BIGINT,
    req_node            BIGINT,
    req_billing         BIGINT,
    req_gres_gpu        BIGINT,
    req_gpu_type        TEXT,

    -- Allocated resources
    alloc_cpu           BIGINT,
    alloc_mem           BIGINT,
    alloc_node          BIGINT,
    alloc_billing       BIGINT,
    alloc_gres_gpu      BIGINT,
    alloc_gpu_type      TEXT,

    PRIMARY KEY (cluster_name, job_id, submit_time)
)
"""

CREATE_JOB_STATISTICS_TABLE = """
CREATE TABLE IF NOT EXISTS job_statistics (
    cluster_name    TEXT            NOT NULL,
    job_id          INTEGER         NOT NULL,
    submit_time     TIMESTAMPTZ     NOT NULL,

    -- Name of the statistic (e.g. gpu_utilization, cpu_utilization, ...)
    stat_name       TEXT            NOT NULL,

    mean            DOUBLE PRECISION,
    std             DOUBLE PRECISION,
    q05             DOUBLE PRECISION,
    q25             DOUBLE PRECISION,
    median          DOUBLE PRECISION,
    q75             DOUBLE PRECISION,
    max             DOUBLE PRECISION,

    PRIMARY KEY (cluster_name, job_id, submit_time, stat_name),
    FOREIGN KEY (cluster_name, job_id, submit_time)
        REFERENCES jobs (cluster_name, job_id, submit_time)
        ON DELETE CASCADE
)
"""

CREATE_CHECKPOINT_TABLE = """
CREATE TABLE IF NOT EXISTS _import_checkpoint (
    id               INTEGER     PRIMARY KEY DEFAULT 1,
    last_submit_time TIMESTAMPTZ,
    total_inserted   INTEGER     NOT NULL DEFAULT 0
)
"""

STAT_FIELDS = [
    "gpu_utilization",
    "gpu_utilization_fp16",
    "gpu_utilization_fp32",
    "gpu_utilization_fp64",
    "gpu_sm_occupancy",
    "gpu_memory",
    "gpu_power",
    "cpu_utilization",
    "system_memory",
]


def nan_to_none(v: float) -> float | None:
    """Convert NaN (used as sentinel in SARC) to None for SQL NULL."""
    if isinstance(v, float) and math.isnan(v):
        return None
    return v


def read_checkpoint(cur: pg8000.Cursor) -> tuple:
    cur.execute(
        "SELECT last_submit_time, total_inserted FROM _import_checkpoint WHERE id = 1"
    )
    row = cur.fetchone()
    if row is None:
        return None, 0
    return row[0], row[1]


def write_checkpoint(cur: pg8000.Cursor, last_submit_time, total: int) -> None:
    cur.execute(
        """
        INSERT INTO _import_checkpoint (id, last_submit_time, total_inserted)
        VALUES (1, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            last_submit_time = EXCLUDED.last_submit_time,
            total_inserted   = EXCLUDED.total_inserted
        """,
        (last_submit_time, total),
    )


def fetch_jobs_from(cluster_name: str | None, checkpoint_time, limit: int | None):
    """Query MongoDB sorted by submit_time, optionally resuming from checkpoint_time."""
    coll = _jobs_collection()
    query = {}
    if cluster_name:
        query["cluster_name"] = cluster_name
    if checkpoint_time is not None:
        query["submit_time"] = {"$gte": checkpoint_time}
    kwargs: dict = {"sort": [("submit_time", 1)]}
    if limit is not None:
        kwargs["limit"] = limit
    return coll.find_by(query, **kwargs)


def insert_job(cur: pg8000.Cursor, job: SlurmJob) -> None:
    cur.execute(
        """
        INSERT INTO jobs (
            mongo_id, cluster_name, account, job_id, array_job_id, task_id,
            name, "user", "group", job_state, exit_code, signal,
            partition, nodes, work_dir, constraints, priority, qos,
            clear_scheduling, started_on_submit, started_on_schedule, started_on_backfill,
            time_limit, submit_time, start_time, end_time, elapsed_time,
            req_cpu, req_mem, req_node, req_billing, req_gres_gpu, req_gpu_type,
            alloc_cpu, alloc_mem, alloc_node, alloc_billing, alloc_gres_gpu, alloc_gpu_type
        ) VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (cluster_name, job_id, submit_time) DO UPDATE SET
            mongo_id            = EXCLUDED.mongo_id,
            account             = EXCLUDED.account,
            array_job_id        = EXCLUDED.array_job_id,
            task_id             = EXCLUDED.task_id,
            name                = EXCLUDED.name,
            "user"              = EXCLUDED."user",
            "group"             = EXCLUDED."group",
            job_state           = EXCLUDED.job_state,
            exit_code           = EXCLUDED.exit_code,
            signal              = EXCLUDED.signal,
            partition           = EXCLUDED.partition,
            nodes               = EXCLUDED.nodes,
            work_dir            = EXCLUDED.work_dir,
            constraints         = EXCLUDED.constraints,
            priority            = EXCLUDED.priority,
            qos                 = EXCLUDED.qos,
            clear_scheduling    = EXCLUDED.clear_scheduling,
            started_on_submit   = EXCLUDED.started_on_submit,
            started_on_schedule = EXCLUDED.started_on_schedule,
            started_on_backfill = EXCLUDED.started_on_backfill,
            time_limit          = EXCLUDED.time_limit,
            start_time          = EXCLUDED.start_time,
            end_time            = EXCLUDED.end_time,
            elapsed_time        = EXCLUDED.elapsed_time,
            req_cpu             = EXCLUDED.req_cpu,
            req_mem             = EXCLUDED.req_mem,
            req_node            = EXCLUDED.req_node,
            req_billing         = EXCLUDED.req_billing,
            req_gres_gpu        = EXCLUDED.req_gres_gpu,
            req_gpu_type        = EXCLUDED.req_gpu_type,
            alloc_cpu           = EXCLUDED.alloc_cpu,
            alloc_mem           = EXCLUDED.alloc_mem,
            alloc_node          = EXCLUDED.alloc_node,
            alloc_billing       = EXCLUDED.alloc_billing,
            alloc_gres_gpu      = EXCLUDED.alloc_gres_gpu,
            alloc_gpu_type      = EXCLUDED.alloc_gpu_type
        """,
        (
            str(job.id) if job.id else None,
            job.cluster_name,
            job.account,
            job.job_id,
            job.array_job_id,
            job.task_id,
            job.name,
            job.user,
            job.group,
            job.job_state.value,
            job.exit_code,
            job.signal,
            job.partition,
            json.dumps(job.nodes),
            job.work_dir,
            job.constraints,
            job.priority,
            job.qos,
            job.CLEAR_SCHEDULING,
            job.STARTED_ON_SUBMIT,
            job.STARTED_ON_SCHEDULE,
            job.STARTED_ON_BACKFILL,
            job.time_limit,
            job.submit_time,
            job.start_time,
            job.end_time,
            job.elapsed_time,
            job.requested.cpu,
            job.requested.mem,
            job.requested.node,
            job.requested.billing,
            job.requested.gres_gpu,
            job.requested.gpu_type,
            job.allocated.cpu,
            job.allocated.mem,
            job.allocated.node,
            job.allocated.billing,
            job.allocated.gres_gpu,
            job.allocated.gpu_type,
        ),
    )


def insert_statistics(cur: pg8000.Cursor, job: SlurmJob) -> None:
    if job.stored_statistics is None:
        return

    for stat_name in STAT_FIELDS:
        stat: Statistics | None = getattr(job.stored_statistics, stat_name)
        if stat is None:
            continue
        cur.execute(
            """
            INSERT INTO job_statistics (
                cluster_name, job_id, submit_time, stat_name,
                mean, std, q05, q25, median, q75, max
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (cluster_name, job_id, submit_time, stat_name) DO UPDATE SET
                mean   = EXCLUDED.mean,
                std    = EXCLUDED.std,
                q05    = EXCLUDED.q05,
                q25    = EXCLUDED.q25,
                median = EXCLUDED.median,
                q75    = EXCLUDED.q75,
                max    = EXCLUDED.max
            """,
            (
                job.cluster_name,
                job.job_id,
                job.submit_time,
                stat_name,
                nan_to_none(stat.mean),
                nan_to_none(stat.std),
                nan_to_none(stat.q05),
                nan_to_none(stat.q25),
                nan_to_none(stat.median),
                nan_to_none(stat.q75),
                nan_to_none(stat.max),
            ),
        )


def connect_plain(sql_url: str) -> pg8000.Connection:
    """Open a direct pg8000 connection from a postgresql:// URL."""
    parsed = urlparse(sql_url)
    return pg8000.connect(
        host=parsed.hostname,
        port=parsed.port or 5432,
        user=parsed.username,
        password=parsed.password,
        database=parsed.path.lstrip("/"),
    )


def connect_cloud_sql(
    connector: Connector, instance: str, user: str, password: str, db: str
) -> pg8000.Connection:
    """Open a pg8000 connection via the Cloud SQL Python Connector."""
    return connector.connect(instance, "pg8000", user=user, password=password, db=db)


def run_import(conn: pg8000.Connection, args) -> int:
    """Create tables, resume from checkpoint if present, and transfer all jobs.

    Returns the total number of jobs transferred.
    """
    cur = conn.cursor()
    try:
        if args.drop:
            print("Dropping existing tables...", flush=True)
            cur.execute("DROP TABLE IF EXISTS job_statistics")
            cur.execute("DROP TABLE IF EXISTS jobs")
            cur.execute("DROP TABLE IF EXISTS _import_checkpoint")
            conn.commit()

        print("Creating tables...", flush=True)
        cur.execute(CREATE_JOBS_TABLE)
        cur.execute(CREATE_JOB_STATISTICS_TABLE)
        cur.execute(CREATE_CHECKPOINT_TABLE)
        conn.commit()

        # Read existing checkpoint so we can resume a previous run
        checkpoint_time, total = read_checkpoint(cur)
        if checkpoint_time is not None:
            print(
                f"Resuming from checkpoint: {checkpoint_time} ({total} jobs already done)",
                flush=True,
            )

        # Retry loop: restart the MongoDB cursor on failure
        while True:
            remaining = (args.limit - total) if args.limit is not None else None
            if remaining is not None and remaining <= 0:
                break

            try:
                jobs = fetch_jobs_from(args.cluster, checkpoint_time, remaining)
                batch = 0
                last_job = None

                for job in jobs:
                    insert_job(cur, job)
                    insert_statistics(cur, job)
                    total += 1
                    batch += 1
                    last_job = job

                    if batch >= args.batch_size:
                        conn.commit()
                        write_checkpoint(cur, job.submit_time, total)
                        conn.commit()
                        checkpoint_time = job.submit_time
                        batch = 0
                        print(
                            f"  Committed {total} jobs, checkpoint: {checkpoint_time}",
                            flush=True,
                        )

                # Commit any remaining jobs in the last partial batch
                if last_job is not None and batch > 0:
                    conn.commit()
                    write_checkpoint(cur, last_job.submit_time, total)
                    conn.commit()

                break  # All jobs processed successfully

            except Exception as e:
                print(f"\nMongoDB error: {e}", flush=True)
                print(f"Rolling back and retrying in {RETRY_DELAY}s...", flush=True)
                try:
                    conn.rollback()
                except Exception:
                    pass
                time.sleep(RETRY_DELAY)
                # Reload checkpoint from PostgreSQL — it reflects the last
                # successfully committed state regardless of the crash
                checkpoint_time, total = read_checkpoint(cur)
                print(
                    f"Resuming from checkpoint: {checkpoint_time} ({total} jobs done)",
                    flush=True,
                )
    finally:
        cur.close()

    return total


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )

    conn_group = parser.add_mutually_exclusive_group(required=True)
    conn_group.add_argument(
        "--sql-url",
        help="Plain PostgreSQL URL for local use, e.g. postgresql://user:pass@host:5432/dbname",
    )
    conn_group.add_argument(
        "--instance",
        help="Cloud SQL instance connection name, e.g. project:region:instance",
    )

    parser.add_argument("--db-user", help="Database user (Cloud SQL only)")
    parser.add_argument("--db-password", help="Database password (Cloud SQL only)")
    parser.add_argument("--db-name", help="Database name (Cloud SQL only)")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of jobs to transfer (default: all)",
    )
    parser.add_argument(
        "--cluster", default=None, help="Only transfer jobs from this cluster"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Number of jobs to commit per transaction (default: 1000)",
    )
    parser.add_argument(
        "--drop", action="store_true", help="Drop existing tables before creating them"
    )
    args = parser.parse_args()

    with using_sarc_mode("scraping"):
        if args.sql_url:
            print(f"Connecting to PostgreSQL at {args.sql_url}...", flush=True)
            conn = connect_plain(args.sql_url)
            try:
                total = run_import(conn, args)
            finally:
                conn.close()
        else:
            print(f"Connecting to Cloud SQL instance {args.instance}...", flush=True)
            with Connector() as connector:
                conn = connect_cloud_sql(
                    connector,
                    args.instance,
                    args.db_user,
                    args.db_password,
                    args.db_name,
                )
                try:
                    total = run_import(conn, args)
                finally:
                    conn.close()

    print(f"Done. Transferred {total} jobs to PostgreSQL.")


if __name__ == "__main__":
    main()
