"""Target shares: annual compute budgets (RGU-years) declared per cluster/account.

The `target_shares` table is the materialization of the allocations CSV named
by the `sarc.target_shares` config key (default `data/allocations.csv`, same
format as the code's data file). `insert_target_shares()` deletes and re-inserts
every row at each import, so surrogate ids churn: consumers must join on the
natural key (cluster_id, allocation, period) and never persist `id`. If a future
table ever references `id`, this full-refresh strategy must be replaced by a
sync on the natural key.
"""

import csv
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Annotated

from pydantic import BeforeValidator, ValidationInfo, field_validator
from sqlalchemy import CheckConstraint, and_, func, literal_column, or_
from sqlalchemy.dialects.postgresql import ExcludeConstraint
from sqlmodel import Field, Relationship, Session, col, delete, select

from .cluster import SlurmClusterDB
from .job_series import JobSeriesTable
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
        # Two target shares for the same cluster and allocation may not
        # overlap. The period is half-open, [start, end), so consecutive
        # budget years touching at a boundary date are legal — same
        # convention as allocations.csv. Requires the btree_gist extension.
        # coalesce() keeps NULL allocations (whole-cluster budgets) from
        # silently escaping the constraint: a gist exclusion predicate with
        # a NULL operand is never true. A NULL vs. concrete-allocation
        # overlap is not expressible here; the import check in
        # _check_no_overlap rejects it instead.
        ExcludeConstraint(
            ("cluster_id", "="),
            (func.coalesce(literal_column('"allocation"'), ""), "="),
            (
                func.daterange(
                    literal_column('"start"'), literal_column('"end"'), "[)"
                ),
                "&&",
            ),
            name="target_shares_no_overlap",
        ),
        # "end" is a reserved word: it must be quoted in the raw SQL.
        CheckConstraint('"end" > start', name="target_shares_end_after_start"),
    )

    # Database ID
    id: int | None = Field(default=None, primary_key=True)
    cluster_id: int = Field(foreign_key="clusters.id", index=True)
    cluster: SlurmClusterDB = Relationship(passive_deletes="all")

    # Budget target: the allocations CSV group_name (e.g. rrg-bengioy-ad or
    # mila). It attributes the slurm_jobs.account rows named exactly by it,
    # or by it plus the "_gpu" suffix (DRAC leaf accounts); see
    # target_share_report. "_cpu" accounts stay out: an RGU budget is GPU
    # capacity only (the CSV's cpu rows carry cores, never rgu_year), and a
    # GPU job running on a "_cpu" account is a data anomaly worth surfacing
    # as unbudgeted, not absorbing into the pool.
    # NULL covers the whole cluster: budgets negotiated at cluster level
    # (PAICE shares) have no single group to name.
    allocation: str | None
    # Budget period, end exclusive.
    start: Annotated[date, BeforeValidator(validate_date)]
    end: Annotated[date, BeforeValidator(validate_date)]
    # Allocated compute, the CSV's rgu_year column stored raw: RGU-years per
    # year, i.e. an average count of RGUs. RGU-hours over a window = rgu_year
    # x hours of overlap with [start, end). Open question for rows not
    # spanning a year: rgu_year may be the period's RGU-year total, not a rate.
    rgu_year: float


class TargetShareRow(SQLModel):
    """One validated CSV row, before cluster-id resolution."""

    start: Annotated[date, BeforeValidator(validate_date)]
    end: Annotated[date, BeforeValidator(validate_date)]
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

    @field_validator("end")
    @classmethod
    def end_after_start(cls, end: date, info: ValidationInfo) -> date:
        start = info.data.get("start")
        if start is not None and end <= start:
            raise ValueError(f"end {end} is not after start {start}")
        return end


def read_target_shares_csv(path: Path) -> list[TargetShareRow]:
    """Parse and validate the allocations CSV, raising on any bad row.

    Reads only the target-share columns; rows without an rgu_year (cpu,
    storage, and the pre-RGU gpu_year rows) describe no RGU budget and are
    skipped here.
    """
    from pydantic import ValidationError

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
                "start": row["start"],
                "end": row["end"],
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
    """True when two rows budget the same accounts over overlapping periods.

    A NULL-allocation row covers every account of its cluster, so it
    conflicts with any other row of that cluster, concrete allocations
    included.
    """
    if first.cluster_name != second.cluster_name:
        return False
    if first.start >= second.end or second.start >= first.end:
        return False
    if first.allocation is None or second.allocation is None:
        return True
    return first.allocation == second.allocation


def _check_no_overlap(rows: list[TargetShareRow]) -> None:
    """Reject rows whose covered accounts and periods overlap within the CSV.

    The DB exclusion constraint catches same-allocation (and NULL/NULL)
    overlaps at flush; checking here rejects NULL vs. concrete overlaps the
    constraint cannot express, and names the offending pair instead of
    raising a constraint violation traceback.
    """
    for i, first in enumerate(rows):
        for second in rows[i + 1 :]:
            if _rows_conflict(first, second):
                raise ValueError(
                    f"overlapping target shares for {first.cluster_name}: "
                    f"{first.allocation or 'all accounts'} "
                    f"[{first.start}, {first.end}) and "
                    f"{second.allocation or 'all accounts'} "
                    f"[{second.start}, {second.end})"
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
                start=row.start,
                end=row.end,
                rgu_year=row.rgu_year,
            )
        )
    return shares


def insert_target_shares() -> None:
    """Full-refresh `target_shares` from the allocations CSV of the config.

    No-op when the config key is null or the file is missing. All rows are
    validated first, then the table is emptied and refilled in a single
    transaction: readers see either the previous set or the new one, never a
    partial or empty table, and rows deleted from the CSV disappear here —
    merge-by-key semantics could not. Raises on any invalid row: a silently
    dropped budget would skew every report built on it, so a failed deploy is
    the wanted outcome. Rows whose cluster the database does not know are
    skipped instead, see _resolve_cluster_ids. Runs after insert_clusters,
    which seeds the clusters resolved here.
    """
    from sarc.config import config

    if config.target_shares is None:
        return
    if not config.target_shares.is_file():
        logger.warning(f"target shares file {config.target_shares} not found")
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
    start: date
    end: date
    rgu_year: float
    target_rgu_hours: float
    consumed_rgu_hours: float


class UnbudgetedConsumption(SQLModel):
    """GPU consumption of a budgeted cluster that no target share of that
    cluster attributes to (other accounts, or hours outside every period)."""

    cluster_name: str
    consumed_rgu_hours: float


def _utc_day_epoch(day):
    """A budget's DATE bound as epoch seconds: the calendar-day midnight in
    UTC, the instant sessions read timestamps at -- independent of the
    session TimeZone, unlike a bare ``start::timestamptz`` cast."""
    return func.extract("epoch", func.timezone("UTC", day))


def target_share_report(
    sess: Session, *, begin: datetime, end: datetime, cluster_name: str | None = None
) -> tuple[list[BudgetConsumption], list[UnbudgetedConsumption]]:
    """Compare GPU consumption against the target shares over ``[begin, end)``.

    A job contributes ``allocated_rgu_drac x hours`` for the hours its run
    (``[start_time, start_time + elapsed_time]``, the /dash span) spends
    inside the budget period *and* the window, on jobs of the budget's
    cluster attributed to it by the account rule documented on
    TargetShareDB.allocation. The target is prorated over the same
    intersection: ``rgu_year`` is a rate of RGUs, so the prorated target is
    ``rgu_year x hours`` of it. Budgets not intersecting the window are
    absent, budgets intersecting it with no matching jobs report 0 consumed.

    The second list is the complement per budgeted cluster: eligible GPU
    consumption of the window minus what any of that cluster's budgets
    attributed. Clusters without any target share carry no budget notion and
    stay out of both lists. ``begin``/``end`` must be timezone-aware.
    """
    if end <= begin:
        return [], []
    begin_s, end_s = begin.timestamp(), end.timestamp()

    budget_start = _utc_day_epoch(col(TargetShareDB.start))
    budget_end = _utc_day_epoch(col(TargetShareDB.end))
    in_window = and_(budget_start < end_s, budget_end > begin_s)

    # The /dash GPU population, running within the window: the eligibility
    # conjuncts are what the partial covering indexes are for, and
    # slurm_job_end(...) keeps the window predicate on the indexed form.
    run_start = func.extract("epoch", col(JobSeriesTable.start_time))
    run_end = run_start + col(JobSeriesTable.elapsed_time)
    job_wheres = [
        col(JobSeriesTable.allocated_gres_gpu) > 0,
        col(JobSeriesTable.gpu_type_rgu_drac).is_not(None),
        func.slurm_job_end(
            col(JobSeriesTable.start_time), col(JobSeriesTable.elapsed_time)
        )
        > func.to_timestamp(begin_s),
        col(JobSeriesTable.start_time) < func.to_timestamp(end_s),
    ]
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
            run_start.label("run_start"),
            run_end.label("run_end"),
        )
        .where(*job_wheres)
        .subquery()
    )

    attribution = or_(
        col(TargetShareDB.allocation).is_(None),
        jobs.c.account == col(TargetShareDB.allocation),
        jobs.c.account == func.concat(col(TargetShareDB.allocation), "_gpu"),
    )
    hours_in_budget = (
        func.least(jobs.c.run_end, budget_end, end_s)
        - func.greatest(jobs.c.run_start, budget_start, begin_s)
    ) / 3600.0
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
            col(TargetShareDB.start),
            col(TargetShareDB.end),
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
        .order_by(col(SlurmClusterDB.name), col(TargetShareDB.start))
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
    window_hours = (
        func.least(jobs.c.run_end, end_s) - func.greatest(jobs.c.run_start, begin_s)
    ) / 3600.0
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
