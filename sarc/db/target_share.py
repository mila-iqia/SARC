"""Target shares: the GPU budgets of the allocations CSV, per cluster and period.

`insert_target_shares()` deletes and re-inserts every row at each import, so
ids change: join on the natural key (cluster_id, start_date), never keep an
`id`. A table referencing `id` would need a sync on that key instead.
"""

import csv
import logging
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Annotated

from pydantic import BeforeValidator, ValidationError, ValidationInfo, field_validator
from sqlalchemy import CheckConstraint, and_, func, literal_column, or_
from sqlalchemy.dialects.postgresql import ExcludeConstraint
from sqlmodel import Field, Session, col, delete, select

from sarc.validators import datetime_utc

from .cluster import SlurmClusterDB
from .job_series import ELIGIBILITY_CLAUSES, JobSeriesTable, overlap_hours, ran_between
from .sqlmodel import SQLModel, datetime_utc_field

logger = logging.getLogger(__name__)


def utc_midnight(value: str | date) -> datetime:
    """A CSV day as the instant midnight UTC.

    This is our convention: the CSV gives bare days, and we do not know
    whether a budget starts at midnight UTC or at midnight in its cluster's
    time zone. Rather than look up each cluster's time zone, every bound is
    normalized to midnight UTC.
    """
    day = date.fromisoformat(value) if isinstance(value, str) else value
    return datetime.combine(day, time(), tzinfo=UTC)


def empty_allocation_as_none(value: object) -> object:
    """A blank CSV allocation field means the whole-cluster budget (NULL)."""
    if isinstance(value, str) and not value.strip():
        return None
    return value


class TargetShareDB(SQLModel, table=True):
    __tablename__ = "target_shares"
    __table_args__ = (
        # One target share per cluster at any instant, so a job counts in one
        # budget at most. Periods are [start_date, end_date): years may touch.
        ExcludeConstraint(
            ("cluster_id", "="),
            (
                func.tstzrange(
                    literal_column("start_date"), literal_column("end_date"), "[)"
                ),
                "&&",
            ),
            name="target_shares_no_overlap",
        ),
        CheckConstraint("end_date > start_date", name="target_shares_end_after_start"),
    )

    # Database ID
    id: int | None = Field(default=None, primary_key=True)
    cluster_id: int = Field(foreign_key="clusters.id")

    # The CSV's group_name (e.g. rrg-bengioy-ad). A budget takes the jobs whose
    # account is this name or this name + "_gpu"; "_cpu" accounts stay out, an
    # RGU budget covers GPUs only. NULL, for a budget negotiated for the whole
    # cluster, takes all its jobs.
    allocation: str | None
    # Budget period, end exclusive: the CSV's start and end days, read as
    # midnight UTC by utc_midnight.
    start_date: datetime_utc = datetime_utc_field()
    end_date: datetime_utc = datetime_utc_field()
    # A number of RGUs (RGU-years per year), whatever the period length:
    # RGU-hours = rgu_year x hours.
    rgu_year: float


class TargetShareRow(SQLModel):
    """One validated CSV row, before cluster-id resolution."""

    cluster_name: str
    allocation: Annotated[str | None, BeforeValidator(empty_allocation_as_none)]
    start_date: Annotated[datetime_utc, BeforeValidator(utc_midnight)]
    end_date: Annotated[datetime_utc, BeforeValidator(utc_midnight)]
    rgu_year: float

    @field_validator("rgu_year")
    @classmethod
    def positive_rate(cls, rgu_year: float) -> float:
        if rgu_year <= 0:
            raise ValueError(f"rgu_year must be positive, got {rgu_year}")
        return rgu_year

    @field_validator("end_date")
    @classmethod
    def end_after_start(cls, end_date: datetime, info: ValidationInfo) -> datetime:
        start_date = info.data.get("start_date")
        if start_date is not None and end_date <= start_date:
            raise ValueError(
                f"end {end_date.date()} is not after start {start_date.date()}"
            )
        return end_date


def read_target_shares_csv(path: Path) -> list[TargetShareRow]:
    """Parse and validate the allocations CSV, raising on any bad row."""
    required = ["start", "end", "cluster_name", "group_name", "rgu_year"]
    rows: list[TargetShareRow] = []
    with path.open(newline="") as csv_file:
        reader = csv.DictReader(csv_file, skipinitialspace=True)
        fieldnames = reader.fieldnames or []
        missing = [name for name in required if name not in fieldnames]
        if missing:
            raise ValueError(f"{path}: missing column(s): {', '.join(missing)}")
        for line_nb, row in enumerate(reader, start=2):  # header is line 1
            # values past the header's last column (trailing commas) land under the key None.
            row.pop(None, None)

            if all(not (value or "").strip() for value in row.values()):
                continue  # fully empty row
            rgu_year = (row.get("rgu_year") or "").strip()
            if not rgu_year:
                continue  # a cpu, storage or gpu_year row: no RGU budget
            values = {
                "cluster_name": row["cluster_name"],
                "allocation": row.get("group_name"),
                "start_date": row["start"],
                "end_date": row["end"],
                "rgu_year": rgu_year,
            }
            try:
                rows.append(TargetShareRow.model_validate(values))
            except ValidationError as e:
                raise ValueError(f"{path}:{line_nb}: invalid row {row}") from e
    return rows


def _resolve_cluster_ids(
    sess: Session, rows: list[TargetShareRow]
) -> list[TargetShareDB]:
    db_clusters = {
        cluster.name: cluster.id for cluster in sess.exec(select(SlurmClusterDB)).all()
    }
    shares: list[TargetShareDB] = []
    for row in rows:
        cluster_id = db_clusters.get(row.cluster_name)
        if cluster_id is None:
            # The CSV may also list clusters SARC does not scrape yet: skip them.
            logger.warning(
                f"skipping target share for unknown cluster {row.cluster_name!r}"
            )
            continue
        shares.append(
            TargetShareDB(
                cluster_id=cluster_id,
                allocation=row.allocation,
                start_date=row.start_date,
                end_date=row.end_date,
                rgu_year=row.rgu_year,
            )
        )
    return shares


def insert_target_shares() -> None:
    """Replace `target_shares` with the CSV rows, in one transaction. A missing
    file or a bad row raises: only null disables the import."""
    from sarc.config import config

    if config.target_shares is None:
        return

    rows = read_target_shares_csv(config.target_shares)

    with config.db.session() as sess:
        shares = _resolve_cluster_ids(sess, rows)
        sess.exec(delete(TargetShareDB))
        sess.add_all(shares)
        sess.commit()
    logger.info(f"Refreshed {len(shares)} target shares from {config.target_shares}")


# --------------------------------------------------------------------------- #
# Consumption vs. budget
# --------------------------------------------------------------------------- #


class BudgetConsumption(SQLModel):
    """One target share with its prorated target and in-budget consumption,
    both in RGU-hours over the report window."""

    cluster_name: str
    allocation: str | None
    start_date: datetime_utc
    end_date: datetime_utc
    rgu_year: float
    target_rgu_hours: float
    consumed_rgu_hours: float


class ClusterConsumption(SQLModel):
    """All the eligible GPU consumption of a budgeted cluster, in RGU-hours
    over the report window, inside its budgets or not."""

    cluster_name: str
    consumed_rgu_hours: float


def target_share_report(
    sess: Session, *, begin: datetime, end: datetime, cluster_name: str | None = None
) -> tuple[list[BudgetConsumption], list[ClusterConsumption]]:
    """Compare GPU consumption with the target shares over ``[begin, end)``.

    Budgets: each target share intersecting the window, with its target
    (rgu_year x hours of the window in its period) and the RGU-hours its jobs
    spent there (account rule on TargetShareDB.allocation). Totals: each
    budgeted cluster's consumption of the window, 0 included; minus its
    budgets, it gives what no budget covers. A cluster without a target share
    in the window is in neither list. ``begin``/``end`` must be timezone-aware.
    """
    if end <= begin:
        return [], []
    begin_s, end_s = begin.timestamp(), end.timestamp()

    budget_start = func.extract("epoch", col(TargetShareDB.start_date))
    budget_end = func.extract("epoch", col(TargetShareDB.end_date))
    in_window = and_(budget_start < end_s, budget_end > begin_s)

    # Eligible GPU jobs running in the window, with the columns overlap_hours reads.
    job_wheres = [*ELIGIBILITY_CLAUSES, ran_between(JobSeriesTable, begin_s, end_s)]
    if cluster_name is not None:
        job_wheres.append(
            col(JobSeriesTable.cluster_id).in_(
                select(SlurmClusterDB.id)
                .where(SlurmClusterDB.name == cluster_name)
                .scalar_subquery()
            )
        )
    jobs = (
        select(  # ty:ignore[no-matching-overload]
            col(JobSeriesTable.cluster_id).label("cluster_id"),
            col(JobSeriesTable.account).label("account"),
            col(JobSeriesTable.allocated_rgu_drac).label("rgu"),
            col(JobSeriesTable.start_time),
            col(JobSeriesTable.elapsed_time),
        )
        .where(*job_wheres)
        .subquery()
    )

    attribution = or_(
        col(TargetShareDB.allocation).is_(None),
        jobs.c.account == col(TargetShareDB.allocation),
        jobs.c.account == func.concat(col(TargetShareDB.allocation), "_gpu"),
    )
    hours_in_budget = overlap_hours(
        jobs.c, (begin_s, end_s), (budget_start, budget_end)
    )
    prorated_target = (
        col(TargetShareDB.rgu_year)
        * (func.least(budget_end, end_s) - func.greatest(budget_start, begin_s))
        / 3600.0
    )

    clusters_where = []
    if cluster_name is not None:
        clusters_where.append(SlurmClusterDB.name == cluster_name)
    budget_rows = sess.exec(
        select(  # ty:ignore[no-matching-overload]
            col(SlurmClusterDB.name).label("cluster_name"),
            col(TargetShareDB.allocation),
            col(TargetShareDB.start_date),
            col(TargetShareDB.end_date),
            col(TargetShareDB.rgu_year),
            prorated_target.label("target_rgu_hours"),
            func.coalesce(func.sum(jobs.c.rgu * hours_in_budget), 0.0).label(
                "consumed_rgu_hours"
            ),
        )
        .join(SlurmClusterDB, col(SlurmClusterDB.id) == col(TargetShareDB.cluster_id))
        .outerjoin(
            jobs,
            and_(
                jobs.c.cluster_id == col(TargetShareDB.cluster_id),
                attribution,
                hours_in_budget > 0,
            ),
        )
        .where(in_window, *clusters_where)
        .group_by(col(TargetShareDB.id), col(SlurmClusterDB.name))
        .order_by(
            col(SlurmClusterDB.name),
            col(TargetShareDB.start_date),
            col(TargetShareDB.allocation),
        )
    ).all()
    budgets = [
        BudgetConsumption.model_validate(dict(row._mapping)) for row in budget_rows
    ]

    # From the clusters, not the jobs, so a budgeted cluster without jobs
    # still gets its row, at 0.
    window_hours = overlap_hours(jobs.c, (begin_s, end_s))
    budgeted_clusters = select(col(TargetShareDB.cluster_id)).where(in_window)
    total_rows = sess.exec(
        select(
            col(SlurmClusterDB.name),
            func.coalesce(func.sum(jobs.c.rgu * window_hours), 0.0),
        )
        .select_from(SlurmClusterDB)
        .outerjoin(jobs, jobs.c.cluster_id == col(SlurmClusterDB.id))
        .where(col(SlurmClusterDB.id).in_(budgeted_clusters), *clusters_where)
        .group_by(col(SlurmClusterDB.name))
        .order_by(col(SlurmClusterDB.name))
    ).all()
    totals = [
        ClusterConsumption(cluster_name=name, consumed_rgu_hours=total)
        for name, total in total_rows
    ]
    return budgets, totals
