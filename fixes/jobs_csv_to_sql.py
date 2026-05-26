"""Import a CSV produced by load_jobs_csv.py (fixes-only branch) into a SQL SARC DB.

The CSV is a flat dump of MongoDB-era jobs. We:
- wipe all jobs, statistics, and users in the target SQL DB (clusters/RGU kept);
- import GPU billings from a JSON file (produced by gpu_billing_from_mongodb.py)
  so RGU values can be computed for clusters where billing_is_gpu is False;
- assume clusters listed in the CSV are already declared in the SARC config so
  that db_upgrade() seeds them;
- resolve users by ``user_uuid`` (MongoDB-era UUID) when present, falling back
  to ``(cluster_name, cluster_user)`` otherwise — see ``get_or_create_user``;
- insert one SlurmJobDB per CSV row, plus one JobStatisticDB per non-empty
  metric. Each row's ``allocated_gpu_type`` is harmonised against the GPU
  names known to GpuRguDB (so jobs scraped with raw Slurm names like
  ``gpu:h100:4`` map to ``H100-SXM5-80GB`` and find their RGU/billing).

The target DB is assumed to be a throwaway test DB.
"""

import argparse
import ast
import csv
import json
import logging
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from psycopg.types.json import Jsonb
from sqlmodel import Session, select, text
from tqdm import tqdm

from sarc.config import ClusterConfig, config
from sarc.db.cluster import GPUBillingDB, SlurmClusterDB
from sarc.db.support import GpuRguDB
from sarc.db.users import UserDB
from sarc.models.job import SlurmState

logger = logging.getLogger(__name__)


STATISTIC_METRICS = (
    "gpu_utilization",
    "gpu_utilization_fp16",
    "gpu_utilization_fp32",
    "gpu_utilization_fp64",
    "gpu_sm_occupancy",
    "gpu_memory",
    "gpu_power",
    "cpu_utilization",
    "system_memory",
)
STATISTIC_FIELDS = ("mean", "std", "q05", "q25", "median", "q75", "max", "unused")

CSV_IMPORT_UUID_PLUGIN = "csv_import_uuid"


def csv_import_cu_plugin(cluster_name: str) -> str:
    """Plugin name for the (cluster, cluster_user) matching_id.

    Per-cluster so a single user can carry usernames on multiple clusters
    (UniqueConstraint(user_id, plugin_name) means at most one match_id per
    plugin per user).
    """
    return f"csv_import_cu_{cluster_name}"


_INT32_MIN = -(2**31)
_INT32_MAX = 2**31 - 1
# Mutable single-element lists so parsing helpers can bump counters without `global`.
_int_overflow_count = [0]
_time_check_skip_count = [0]


def _parse_int(v: str) -> int | None:
    """Parse a CSV cell into ``int | None``, clamping int32 overflow to None.

    The MongoDB dump contains a handful of corrupt rows with values up to ~80e9
    in ``requested.billing`` / ``requested.gres_gpu``. PostgreSQL's INTEGER is
    32-bit so those rows would abort the whole batch. Since these columns are
    nullable in the SQL schema, we drop the offending value and bump a counter
    that ``run()`` reports at the end.
    """
    if v == "":
        return None
    iv = int(v)
    if iv < _INT32_MIN or iv > _INT32_MAX:
        _int_overflow_count[0] += 1
        return None
    return iv


def _parse_bool(v: str) -> bool:
    if v == "True":
        return True
    if v == "False":
        return False
    raise ValueError(f"Invalid bool literal: {v!r}")


def _parse_dt(v: str) -> datetime | None:
    if v == "":
        return None
    dt = datetime.fromisoformat(v)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _parse_dt_required(v: str) -> datetime:
    dt = _parse_dt(v)
    if dt is None:
        raise ValueError("required datetime field is empty")
    return dt


def _parse_nodes(v: str) -> list[str]:
    if v in {"", "[]"}:
        return []
    # Stored as Python repr: ['cn-c031', 'cn-a005']. Slurm node names have no
    # quotes/special chars, so json.loads(...) after ' -> " is ~5x faster than
    # ast.literal_eval. Fall back if a name surprises us.
    try:
        parsed = json.loads(v.replace("'", '"'))
    except json.JSONDecodeError:
        parsed = ast.literal_eval(v)
    assert isinstance(parsed, list)
    return [str(x) for x in parsed]


def import_gpu_billings(
    sess: Session, json_path: Path, cluster_ids: dict[str, int]
) -> tuple[int, Counter]:
    """Load GPU billings from the JSON dump produced by gpu_billing_from_mongodb.py.

    Returns (inserted_count, skipped_per_cluster). Skipped entries correspond
    to cluster names absent from the SQL DB.
    """
    docs = json.loads(json_path.read_text())
    if not isinstance(docs, list):
        raise ValueError(f"Expected a JSON array in {json_path}.")
    inserted = 0
    skipped: Counter = Counter()
    for doc in docs:
        cluster_name = doc["cluster_name"]
        cluster_id = cluster_ids.get(cluster_name)
        if cluster_id is None:
            skipped[cluster_name] += 1
            continue
        since = datetime.fromisoformat(doc["since"])
        if since.tzinfo is None:
            since = since.replace(tzinfo=UTC)
        GPUBillingDB.get_or_create(
            sess,
            cluster_id=cluster_id,
            since=since,
            gpu_to_billing=doc["gpu_to_billing"],
        )
        inserted += 1
    sess.commit()
    return inserted, skipped


def harmonize_allocated_gpu(
    cluster_name: str,
    gpu_type: str | None,
    nodes: list[str],
    known_gpus: set[str],
    cluster_cfgs: dict[str, ClusterConfig],
    harmonized: Counter,
    unharmonisable: Counter,
) -> str | None:
    """Map a raw Slurm ``allocated_gpu_type`` to a name known to GpuRguDB.

    Jobs scraped before the harmonisation step (or on clusters where
    ``gpus_per_nodes`` is not declared) carry raw names like ``gpu:h100:4``.
    GpuRguDB / GPUBillingDB key on harmonised names (``H100-SXM5-80GB``), so
    the raw name needs to be translated for RGU and billing lookups to hit.

    Counters are mutated in place:
        * ``harmonised[cluster]`` += 1 when we replaced the name.
        * ``unharmonisable[cluster]`` += 1 when no mapping was found.
    Unharmonisable values are returned as-is, leaving rgu NULL downstream.
    """
    if gpu_type is None or gpu_type in known_gpus:
        return gpu_type
    cluster_cfg = cluster_cfgs.get(cluster_name)
    if cluster_cfg is None:
        unharmonisable[cluster_name] += 1
        return gpu_type

    full_name = None
    if " : " in gpu_type:
        full_name, gpu_type = gpu_type.split(" : ")

    harmonized_name = cluster_cfg.harmonize_gpu_from_nodes(nodes, gpu_type)
    if harmonized_name is not None and harmonized_name in known_gpus:
        harmonized[cluster_name] += 1

        if full_name is not None:
            assert harmonized_name.startswith(full_name + " : ")

        return harmonized_name

    unharmonisable[cluster_name] += 1
    return gpu_type


def wipe_data(sess: Session) -> None:
    """Truncate jobs, stats, users, and all user-related tables in O(1).

    Keeps clusters, gpu_billings, node_gpu_mapping, gpu_rgus, diskusage,
    allocation — none of them reference ``users.id`` so CASCADE leaves them
    alone. The two FK chains rooted at ``users.id`` are:
      - slurm_jobs (RESTRICT) -> job_statistics (CASCADE)
      - matching_ids / credentials / member_type / user_supervisors / supervisors_helper

    ``TRUNCATE ... CASCADE`` follows the entire FK graph regardless of
    ``ondelete`` clauses, so a single statement clears everything.
    """
    sess.exec(text(f'TRUNCATE TABLE "{UserDB.__tablename__}" RESTART IDENTITY CASCADE'))
    sess.commit()


def get_or_create_user(
    sess: Session,
    cluster_name: str,
    cluster_user: str,
    user_uuid: str | None,
    uuid_cache: dict[str, int],
    cu_cache: dict[tuple[str, str], int],
) -> int:
    """Resolve a UserDB id from a CSV row.

    Identity priority:
    1. ``user_uuid`` (MongoDB-era UUID) — stable across clusters when present.
    2. ``(cluster_name, cluster_user)`` — fallback when the row has no uuid.

    When a user is found via the cu fallback and the row also carries a uuid,
    the uuid is attached to that user (it's the first time we see it). When
    found via uuid, we don't attach a new ``csv_import_cu_<cluster>`` matching
    id (it would violate the global ``UniqueConstraint(plugin_name, match_id)``
    if a different stub already owns that cu pair); the cu_cache entry is
    in-memory only.
    """
    cu_key = (cluster_name, cluster_user)
    user_id: int | None = None

    if user_uuid and user_uuid in uuid_cache:
        user_id = uuid_cache[user_uuid]
    elif cu_key in cu_cache:
        user_id = cu_cache[cu_key]

    if user_id is not None:
        if cu_cache.get(cu_key) != user_id:
            cu_cache[cu_key] = user_id
        if user_uuid and user_uuid not in uuid_cache:
            user = sess.get(UserDB, user_id)
            assert user is not None
            user.matching_ids[CSV_IMPORT_UUID_PLUGIN] = user_uuid
            sess.flush()
            uuid_cache[user_uuid] = user_id
        return user_id

    user = UserDB(
        display_name=cluster_user, email=f"{cluster_user}@{cluster_name}.imported.csv"
    )
    sess.add(user)
    sess.flush()
    user.matching_ids[csv_import_cu_plugin(cluster_name)] = cluster_user
    if user_uuid:
        user.matching_ids[CSV_IMPORT_UUID_PLUGIN] = user_uuid
    sess.flush()
    assert user.id is not None
    cu_cache[cu_key] = user.id
    if user_uuid:
        uuid_cache[user_uuid] = user.id
    return user.id


def build_job_kwargs(
    row: dict[str, str],
    cluster_id: int,
    sarc_user_id: int,
    nodes: list[str],
    allocated_gpu_type: str | None,
) -> dict[str, Any]:
    return dict(
        cluster_id=cluster_id,
        account=row["account"],
        job_id=int(row["job_id"]),
        array_job_id=_parse_int(row["array_job_id"]),
        task_id=_parse_int(row["task_id"]),
        name=row["name"],
        cluster_user=row["user"],
        group=row["group"],
        job_state=SlurmState(row["job_state"]).value,
        exit_code=_parse_int(row["exit_code"]),
        signal=_parse_int(row["signal"]),
        partition=row["partition"],
        nodes=Jsonb(nodes),
        work_dir=row["work_dir"],
        submit_line=None,
        constraints=row["constraints"] or None,
        priority=_parse_int(row["priority"]),
        qos=row["qos"] or None,
        CLEAR_SCHEDULING=_parse_bool(row["CLEAR_SCHEDULING"]),
        STARTED_ON_SUBMIT=_parse_bool(row["STARTED_ON_SUBMIT"]),
        STARTED_ON_SCHEDULE=_parse_bool(row["STARTED_ON_SCHEDULE"]),
        STARTED_ON_BACKFILL=_parse_bool(row["STARTED_ON_BACKFILL"]),
        time_limit=_parse_int(row["time_limit"]),
        submit_time=_parse_dt_required(row["submit_time"]),
        start_time=_parse_dt(row["start_time"]),
        end_time=_parse_dt(row["end_time"]),
        elapsed_time=float(row["elapsed_time"]),
        latest_scraped_start=_parse_dt(row["latest_scraped_start"]),
        latest_scraped_end=_parse_dt(row["latest_scraped_end"]),
        requested_cpu=_parse_int(row["requested.cpu"]),
        requested_mem=_parse_int(row["requested.mem"]),
        requested_node=_parse_int(row["requested.node"]),
        requested_billing=_parse_int(row["requested.billing"]),
        requested_gres_gpu=_parse_int(row["requested.gres_gpu"]),
        requested_gpu_type=row["requested.gpu_type"] or None,
        allocated_cpu=_parse_int(row["allocated.cpu"]),
        allocated_mem=_parse_int(row["allocated.mem"]),
        allocated_node=_parse_int(row["allocated.node"]),
        allocated_billing=_parse_int(row["allocated.billing"]),
        allocated_gres_gpu=_parse_int(row["allocated.gres_gpu"]),
        allocated_gpu_type=allocated_gpu_type,
        sarc_user_id=sarc_user_id,
    )


def build_stats_kwargs(row: dict[str, str]) -> dict[str, dict[str, float | None]]:
    """Return ``{metric_name: {field: value}}`` for non-empty metrics only.

    The ``job_id`` column is filled in later, once ``flush_batch`` knows the
    job ids returned by the bulk insert.

    Fast path: most rows have no stats at all. We short-circuit on all-empty
    cells (string compare, no float parsing) before doing any float() work.
    """
    res: dict[str, dict[str, float | None]] = {}
    for metric in STATISTIC_METRICS:
        cells = [row[f"{metric}.{f}"] for f in STATISTIC_FIELDS]
        if not any(cells):
            continue
        res[metric] = {
            f: (float(c) if c else None) for f, c in zip(STATISTIC_FIELDS, cells)
        }
    return res


_STAT_COLS = ("job_id", "name", *STATISTIC_FIELDS)


def flush_batch(
    sess: Session,
    job_dicts: list[dict[str, Any]],
    stats_per_job: list[dict[str, dict[str, float | None]]],
) -> None:
    """Bulk-load one batch via PostgreSQL ``COPY ... FROM STDIN``.

    COPY streams typed values to the backend with much less per-row overhead
    than INSERT (no SQL parse, no per-row plan). For 6.8M jobs we trade the
    convenient ``INSERT ... RETURNING id`` for one extra round-trip per batch
    that reserves N ids from the slurm_jobs sequence; the stats stream then
    uses those ids directly as the FK.

    We must flush any pending ORM operations first (e.g. newly-created
    ``UserDB`` rows) so the FK ``sarc_user_id -> users.id`` resolves.
    """
    if not job_dicts:
        return

    sess.flush()
    raw_conn = sess.connection().connection  # psycopg3 connection

    job_cols = (*job_dicts[0].keys(), "id")
    job_cols_sql = ", ".join(f'"{c}"' for c in job_cols)
    stat_cols_sql = ", ".join(f'"{c}"' for c in _STAT_COLS)

    with raw_conn.cursor() as cur:
        cur.execute(
            "SELECT nextval('slurm_jobs_id_seq') FROM generate_series(1, %s)",
            (len(job_dicts),),
        )
        job_ids = [r[0] for r in cur.fetchall()]

        with cur.copy(f"COPY slurm_jobs ({job_cols_sql}) FROM STDIN") as cp:
            for jid, jd in zip(job_ids, job_dicts, strict=True):
                cp.write_row((*(jd[c] for c in job_cols[:-1]), jid))

        if any(stats_per_job):
            with cur.copy(f"COPY jobstatisticdb ({stat_cols_sql}) FROM STDIN") as cp:
                for jid, stats in zip(job_ids, stats_per_job, strict=True):
                    for name, values in stats.items():
                        cp.write_row(
                            (jid, name, *(values[f] for f in STATISTIC_FIELDS))
                        )


def run(csv_path: str, gpu_billing_path: Path, batch_size: int) -> None:
    cfg = config("scraping")
    # Triggers db_upgrade(): creates tables, seeds clusters from config + RGU.
    engine = cfg.db.engine

    with Session(engine) as sess:
        logger.info("Wiping jobs, stats, and users...")
        wipe_data(sess)

    # Snapshot cluster name -> id mapping and the set of GPU names known to
    # GpuRguDB (used to short-circuit the harmonisation lookup below).
    with Session(engine) as sess:
        cluster_ids: dict[str, int] = {
            c.name: c.id  # ty:ignore[invalid-key-type]
            for c in sess.exec(select(SlurmClusterDB)).all()
            if c.id is not None and c.name is not None
        }
        known_gpus: set[str] = set(sess.exec(select(GpuRguDB.name)).all())
    logger.info("Found %d clusters in DB: %s", len(cluster_ids), sorted(cluster_ids))
    logger.info("Found %d GPU names in GpuRguDB.", len(known_gpus))

    # Load GPU billings before importing jobs so that JobSeriesDB.rgu can be
    # computed for clusters where billing_is_gpu is False.
    with Session(engine) as sess:
        inserted, skipped_billings = import_gpu_billings(
            sess, gpu_billing_path, cluster_ids
        )
    logger.info("Imported %d gpu_billing entries.", inserted)
    if skipped_billings:
        logger.warning(
            "Skipped billings for unknown clusters: %s", dict(skipped_billings)
        )

    cluster_cfgs: dict[str, ClusterConfig] = cfg.clusters
    harmonised: Counter = Counter()
    unharmonisable: Counter = Counter()

    uuid_cache: dict[str, int] = {}
    cu_cache: dict[tuple[str, str], int] = {}
    count = 0
    job_dicts: list[dict[str, Any]] = []
    stats_per_job: list[dict[str, dict[str, float | None]]] = []

    with Session(engine) as sess, open(csv_path, newline="", encoding="utf-8") as f:
        # Throwaway test DB: drop fsync round-trips on every COMMIT.
        # We wipe before each run, so a crash mid-import just means rerunning.
        sess.exec(text("SET synchronous_commit = OFF"))
        reader = csv.DictReader(f)
        for row in tqdm(reader, desc="job(s)"):
            cluster_name = row["cluster_name"]
            cluster_id = cluster_ids.get(cluster_name)
            if cluster_id is None:
                raise ValueError(
                    f"Cluster {cluster_name!r} not in DB. "
                    "Add it to your SARC config and re-run."
                )

            # Skip rows that would violate the SQL CHECK constraints
            # (submit_time <= start_time <= end_time). Done before user
            # creation so we don't leave an orphan UserDB for a dropped row.
            submit_time = _parse_dt_required(row["submit_time"])
            start_time = _parse_dt(row["start_time"])
            end_time = _parse_dt(row["end_time"])
            if (start_time is not None and submit_time > start_time) or (
                start_time is not None
                and end_time is not None
                and start_time > end_time
            ):
                _time_check_skip_count[0] += 1
                continue

            user_uuid = row["user_uuid"].strip() or None
            sarc_user_id = get_or_create_user(
                sess, cluster_name, row["user"], user_uuid, uuid_cache, cu_cache
            )

            nodes = _parse_nodes(row["nodes"])
            allocated_gpu_type = harmonize_allocated_gpu(
                cluster_name,
                row["allocated.gpu_type"] or None,
                nodes,
                known_gpus,
                cluster_cfgs,
                harmonised,
                unharmonisable,
            )

            job_dicts.append(
                build_job_kwargs(
                    row, cluster_id, sarc_user_id, nodes, allocated_gpu_type
                )
            )
            stats_per_job.append(build_stats_kwargs(row))
            count += 1

            if len(job_dicts) >= batch_size:
                flush_batch(sess, job_dicts, stats_per_job)
                sess.commit()
                sess.expunge_all()
                job_dicts.clear()
                stats_per_job.clear()

        flush_batch(sess, job_dicts, stats_per_job)
        sess.commit()

    logger.info("Imported %d jobs.", count)
    if _int_overflow_count[0]:
        logger.warning(
            "Clamped %d int32-overflow values to None "
            "(likely corrupt requested.billing / requested.gres_gpu).",
            _int_overflow_count[0],
        )
    if _time_check_skip_count[0]:
        logger.warning(
            "Skipped %d row(s) violating submit_time <= start_time <= end_time.",
            _time_check_skip_count[0],
        )
    if harmonised:
        logger.info("Harmonised allocated_gpu_type per cluster: %s", dict(harmonised))
    if unharmonisable:
        logger.warning(
            "Could not harmonise allocated_gpu_type per cluster (kept raw, "
            "rgu will be NULL): %s",
            dict(unharmonisable),
        )


def main() -> None:
    logging.basicConfig(level=logging.INFO, force=True)

    parser = argparse.ArgumentParser(
        description="Rebuild a SARC SQL DB from a CSV produced by load_jobs_csv.py."
    )
    parser.add_argument("csv_path", help="Path to the CSV file.")
    parser.add_argument(
        "--gpu-billing",
        required=True,
        type=Path,
        help="JSON file produced by gpu_billing_from_mongodb.py "
        "(loaded into gpubillingdb before importing jobs).",
    )
    parser.add_argument(
        "--batch-size", type=int, default=20_000, help="Commit every N jobs."
    )
    args = parser.parse_args()

    run(args.csv_path, args.gpu_billing, args.batch_size)


if __name__ == "__main__":
    main()
