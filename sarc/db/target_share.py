"""Target shares: annual compute budgets (RGU-years) declared per cluster/account.

The `target_shares` table is the materialization of the allocations CSV named
by the `sarc.target_shares` config key (default `data/allocations.csv`, same
format as the code's data file). `insert_target_shares()` deletes and re-inserts
every row at each import, so surrogate ids churn: consumers must join on the
natural key (cluster_id, start_date) and never persist `id`. If a future
table ever references `id`, this full-refresh strategy must be replaced by a
sync on the natural key.
"""

import csv
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Annotated

from pydantic import BeforeValidator, ValidationError, ValidationInfo, field_validator
from sqlalchemy import TIMESTAMP, CheckConstraint, and_, cast, func, literal_column, or_
from sqlalchemy.dialects.postgresql import ExcludeConstraint
from sqlmodel import Field, Session, col, delete, select

from .cluster import SlurmClusterDB
from .job_series import ELIGIBILITY_CLAUSES, JobSeriesTable, overlap_hours, ran_between
from .sqlmodel import SQLModel

logger = logging.getLogger(__name__)


def validate_date(value: str | date | datetime) -> date:
    if isinstance(value, str):
        if "T" in value:
            return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S").date()

        return datetime.strptime(value, "%Y-%m-%d").date()

    if isinstance(value, datetime):
        return value.date()

    return value


def empty_allocation_as_none(value: object) -> object:
    """A blank CSV allocation field means the whole-cluster budget (NULL)."""
    if isinstance(value, str) and not value.strip():
        return None
    return value


class TargetShareDB(SQLModel, table=True):
    __tablename__ = "target_shares"
    __table_args__ = (
        # One target share per cluster at any instant, whatever the
        # allocation, so a job is attributed to one budget at most. The
        # period is half-open, [start_date, end_date), so consecutive budget
        # years touching at a boundary date are legal — same convention as
        # allocations.csv. Requires the btree_gist extension.
        ExcludeConstraint(
            ("cluster_id", "="),
            (
                func.daterange(
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

    # Budget target: the allocations CSV group_name (e.g. rrg-bengioy-ad or
    # mila). It attributes the slurm_jobs.account rows named exactly by it,
    # or by it plus the "_gpu" suffix (DRAC leaf accounts); see
    # target_share_report. "_cpu" accounts stay out: an RGU budget is GPU
    # capacity only (the CSV's cpu rows carry cores, never rgu_year), and a
    # GPU job running on a "_cpu" account is a data anomaly worth surfacing
    # as unbudgeted, not absorbing into the pool.
    # NULL covers the whole cluster: budgets negotiated at cluster level
    # have no single group to name.
    allocation: str | None
    # Budget period, end exclusive: the CSV's start and end columns.
    start_date: Annotated[date, BeforeValidator(validate_date)]
    end_date: Annotated[date, BeforeValidator(validate_date)]
    # The CSV's rgu_year, a count of RGUs (RGU-years per year), whatever the
    # period length. RGU-hours over a window = rgu_year x hours of overlap
    # with [start_date, end_date).
    rgu_year: float


class TargetShareRow(SQLModel):
    """One validated CSV row, before cluster-id resolution."""

    start_date: Annotated[date, BeforeValidator(validate_date)]
    end_date: Annotated[date, BeforeValidator(validate_date)]
    cluster_name: str
    allocation: Annotated[str | None, BeforeValidator(empty_allocation_as_none)]
    # Annual rate, as the allocations.csv rgu_year column.
    rgu_year: float

    @field_validator("rgu_year")
    @classmethod
    def positive_rate(cls, rgu_year: float) -> float:
        if rgu_year <= 0:
            raise ValueError(f"rgu_year must be positive, got {rgu_year}")
        return rgu_year

    @field_validator("end_date")
    @classmethod
    def end_after_start(cls, end_date: date, info: ValidationInfo) -> date:
        start_date = info.data.get("start_date")
        if start_date is not None and end_date <= start_date:
            raise ValueError(f"end {end_date} is not after start {start_date}")
        return end_date


def read_target_shares_csv(path: Path) -> list[TargetShareRow]:
    """Parse and validate the allocations CSV, raising on any bad row.

    Reads only the target-share columns; rows without an rgu_year (cpu,
    storage, and the pre-RGU gpu_year rows) describe no RGU budget and are
    skipped here.
    """
    required = ["start", "end", "cluster_name", "group_name", "rgu_year"]
    rows: list[TargetShareRow] = []
    with path.open(newline="") as csv_file:
        reader = csv.DictReader(csv_file, skipinitialspace=True)
        fieldnames = reader.fieldnames or []
        missing = [name for name in required if name not in fieldnames]
        if missing:
            raise ValueError(f"{path}: missing column(s): {', '.join(missing)}")
        for line_nb, row in enumerate(reader, start=2):  # header is line 1
            row.pop(None, None)  # fields past the header's last column
            if all(not (value or "").strip() for value in row.values()):
                continue  # fully empty row
            rgu_year = (row.get("rgu_year") or "").strip()
            if not rgu_year:
                continue  # a cpu, storage or gpu_year row: no RGU budget
            values = {
                "start_date": row["start"],
                "end_date": row["end"],
                "cluster_name": row["cluster_name"],
                "allocation": row.get("group_name"),
                "rgu_year": rgu_year,
            }
            try:
                rows.append(TargetShareRow.model_validate(values))
            except ValidationError as e:
                raise ValueError(f"{path}:{line_nb}: invalid row {row}") from e
    return rows


def _rows_conflict(first: TargetShareRow, second: TargetShareRow) -> bool:
    """True when two rows budget one cluster over overlapping periods."""
    return (
        first.cluster_name == second.cluster_name
        and first.start_date < second.end_date
        and second.start_date < first.end_date
    )


def _check_no_overlap(rows: list[TargetShareRow]) -> None:
    """Reject two rows of one cluster with overlapping periods. The DB
    constraint enforces the same rule; checking here names both rows, where a
    constraint violation only gives a cluster id."""
    for i, first in enumerate(rows):
        for second in rows[i + 1 :]:
            if _rows_conflict(first, second):
                raise ValueError(
                    f"overlapping target shares for {first.cluster_name}: "
                    f"{first.allocation or 'all accounts'} "
                    f"[{first.start_date}, {first.end_date}) and "
                    f"{second.allocation or 'all accounts'} "
                    f"[{second.start_date}, {second.end_date})"
                )


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
            # Budgets of not-yet-instrumented clusters stay in the CSV until
            # insert_clusters learns them; the import must not fail on them.
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
    """Replace the content of `target_shares` with the configured CSV, in one
    transaction. A missing file or any invalid row raises: only null disables
    the import, and a silently dropped budget would skew every report."""
    from sarc.config import config

    if config.target_shares is None:
        return

    rows = read_target_shares_csv(config.target_shares)
    _check_no_overlap(rows)

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
    start_date: date
    end_date: date
    rgu_year: float
    target_rgu_hours: float
    consumed_rgu_hours: float


class UnbudgetedConsumption(SQLModel):
    """GPU consumption of a budgeted cluster that no target share of that
    cluster attributes to (other accounts, or hours outside every period)."""

    cluster_name: str
    consumed_rgu_hours: float


def _utc_day_epoch(day):
    """A budget's DATE bound as the epoch seconds of its midnight UTC. The
    value must not go through timestamptz (a cast, or timezone()): that
    conversion places midnight in the session TimeZone."""
    return func.extract("epoch", cast(day, TIMESTAMP))


def target_share_report(
    sess: Session, *, begin: datetime, end: datetime, cluster_name: str | None = None
) -> tuple[list[BudgetConsumption], list[UnbudgetedConsumption]]:
    """Compare GPU consumption against the target shares over ``[begin, end)``.

    A job contributes ``allocated_rgu_drac x hours`` for the hours its run
    (the shared ``job_span`` one, ``[start_time, start_time + elapsed_time]``)
    spends inside the budget period *and* the window, on jobs of the
    budget's cluster attributed to it by the account rule documented on
    TargetShareDB.allocation. The target is prorated over the same
    intersection: ``rgu_year`` is a rate of RGUs, so the prorated target is
    ``rgu_year x hours`` of it. Budgets not intersecting the window are
    absent, budgets intersecting it with no matching jobs report 0 consumed.

    The second list is the complement per budgeted cluster: eligible GPU
    consumption of the window minus what any of that cluster's budgets
    attributed. A cluster with no target share intersecting the window is in
    neither list. ``begin``/``end`` must be timezone-aware.
    """
    if end <= begin:
        return [], []
    begin_s, end_s = begin.timestamp(), end.timestamp()

    budget_start = _utc_day_epoch(col(TargetShareDB.start_date))
    budget_end = _utc_day_epoch(col(TargetShareDB.end_date))
    in_window = and_(budget_start < end_s, budget_end > begin_s)

    # The shared GPU population, running within the window; the subquery
    # keeps start_time/elapsed_time so overlap_hours can be spelled over
    # jobs.c in both queries below.
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

    budgets_where = [in_window]
    if cluster_name is not None:
        budgets_where.append(SlurmClusterDB.name == cluster_name)
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
        .where(*budgets_where)
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

    # Unbudgeted = the window's whole eligible consumption of a budgeted
    # cluster minus the attributed part; the difference is arithmetically
    # what the attribution left out, whatever the reason (foreign account,
    # hours outside every budget period).
    attributed: dict[str, float] = {}
    for budget in budgets:
        attributed[budget.cluster_name] = (
            attributed.get(budget.cluster_name, 0.0) + budget.consumed_rgu_hours
        )
    window_hours = overlap_hours(jobs.c, (begin_s, end_s))
    totals = sess.exec(
        select(
            jobs.c.cluster_id,
            func.sum(jobs.c.rgu * window_hours).label("consumed_rgu_hours"),
        )
        .where(
            jobs.c.cluster_id.in_(
                select(col(TargetShareDB.cluster_id)).where(in_window).scalar_subquery()
            )
        )
        .group_by(jobs.c.cluster_id)
    ).all()
    names = dict(
        sess.exec(select(col(SlurmClusterDB.id), col(SlurmClusterDB.name))).all()
    )
    unbudgeted = []
    for cluster_id, total in totals:
        rest = float(total) - attributed.get(names[cluster_id], 0.0)
        if rest > 0:
            unbudgeted.append(
                UnbudgetedConsumption(
                    cluster_name=names[cluster_id], consumed_rgu_hours=rest
                )
            )
    unbudgeted.sort(key=lambda row: row.cluster_name)
    return budgets, unbudgeted
