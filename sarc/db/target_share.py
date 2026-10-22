"""Target shares: annual compute budgets (RGU-hours) declared per cluster/account.

The `target_shares` table is the full materialization of a CSV declared in the
`sarc.target_shares` config key. `insert_target_shares()` deletes and re-inserts
every row at each import, so surrogate ids churn: consumers must join on the
natural key (cluster_id, allocation, period) and never persist `id`. If a future
table ever references `id`, this full-refresh strategy must be replaced by a
sync on the natural key.
"""

import csv
import logging
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Annotated

from pydantic import BeforeValidator, ValidationInfo, field_validator
from sqlalchemy import CheckConstraint, func, literal_column
from sqlalchemy.dialects.postgresql import ExcludeConstraint
from sqlmodel import Field, Relationship, Session, delete, select

from sarc.validators import datetime_utc

from .allocation import validate_date
from .cluster import SlurmClusterDB
from .sqlmodel import SQLModel, datetime_utc_field

logger = logging.getLogger(__name__)


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

    # Slurm accounting account the budget covers, spelled exactly as in
    # `slurm_jobs.account` (exact match, no prefix rule). NULL covers the
    # whole cluster: budgets negotiated at cluster level (PAICE shares)
    # have no single account — one per chair — to name.
    allocation: str | None
    # Budget period, end exclusive.
    start: Annotated[date, BeforeValidator(validate_date)]
    end: Annotated[date, BeforeValidator(validate_date)]
    # Allocated compute, in RGU-hours — same unit as the iguane job costs.
    rgu_hours: float
    # Time of the import that produced this row; identical for every row
    # since each import is a full refresh.
    timestamp: datetime_utc = datetime_utc_field()


class TargetShareRow(SQLModel):
    """One validated CSV row, before cluster-id resolution."""

    start: Annotated[date, BeforeValidator(validate_date)]
    end: Annotated[date, BeforeValidator(validate_date)]
    cluster_name: str
    allocation: Annotated[str | None, BeforeValidator(empty_allocation_as_none)]
    rgu_hours: float

    @field_validator("rgu_hours")
    @classmethod
    def positive_budget(cls, rgu_hours: float) -> float:
        if rgu_hours <= 0:
            raise ValueError(f"rgu_hours must be positive, got {rgu_hours}")
        return rgu_hours

    @field_validator("end")
    @classmethod
    def end_after_start(cls, end: date, info: ValidationInfo) -> date:
        start = info.data.get("start")
        if start is not None and end <= start:
            raise ValueError(f"end {end} is not after start {start}")
        return end


def read_target_shares_csv(path: Path) -> list[TargetShareRow]:
    """Parse and validate the target shares CSV, raising on any bad row."""
    from pydantic import ValidationError

    rows: list[TargetShareRow] = []
    with path.open(newline="") as csv_file:
        reader = csv.DictReader(csv_file, skipinitialspace=True, restkey="garbage")
        for line_nb, row in enumerate(reader, start=2):  # header is line 1
            if row.pop("garbage", None) is not None:
                raise ValueError(
                    f"{path}:{line_nb}: too many columns: {row} (expected "
                    "start, end, cluster_name, allocation, rgu_hours)"
                )
            if all(not value.strip() for value in row.values()):
                continue  # fully empty row
            try:
                rows.append(TargetShareRow.model_validate(row))
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
    timestamp = datetime.now(UTC)
    shares: list[TargetShareDB] = []
    for row in rows:
        cluster_id = db_clusters.get(row.cluster_name)
        if cluster_id is None:
            raise ValueError(
                f"target share cluster {row.cluster_name!r} not found in the "
                "database (seeded by insert_clusters)"
            )
        shares.append(
            TargetShareDB(
                cluster_id=cluster_id,
                allocation=row.allocation,
                start=row.start,
                end=row.end,
                rgu_hours=row.rgu_hours,
                timestamp=timestamp,
            )
        )
    return shares


def insert_target_shares() -> None:
    """Full-refresh `target_shares` from the CSV in the `sarc.target_shares` config.

    No-op when the config key is unset. All rows are validated first, then the
    table is emptied and refilled in a single transaction: readers see either
    the previous set or the new one, never a partial or empty table, and rows
    deleted from the CSV disappear here — merge-by-key semantics could not.
    Raises on any invalid row: a silently dropped budget would skew every
    report built on it, so a failed deploy is the wanted outcome. Runs after
    insert_clusters, which seeds the clusters resolved here.
    """
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
