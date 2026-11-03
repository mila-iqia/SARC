"""Target shares: the GPU budgets of the allocations CSV, per cluster and period."""

import csv
import logging
from datetime import date
from pathlib import Path
from typing import Annotated

from pydantic import BeforeValidator, ValidationError, ValidationInfo, field_validator
from sqlalchemy import TIMESTAMP, CheckConstraint, and_, cast, func, literal_column, or_
from sqlalchemy.dialects.postgresql import ExcludeConstraint
from sqlmodel import Field, Session, col, delete, select

from .cluster import SlurmClusterDB
from .sqlmodel import SQLModel

logger = logging.getLogger(__name__)


def utc_midnight(day):
    """SQL: midnight UTC of a DATE expression, as a timestamptz.

    This is our convention: the CSV gives bare days, and we do not know
    whether a budget starts at midnight UTC or at midnight in its cluster's
    time zone. Rather than look up each cluster's time zone, every bound is
    normalized to midnight UTC. Spelled out because a plain
    ``day::timestamptz`` cast takes midnight in the session's TimeZone.
    """
    return func.timezone("UTC", cast(day, TIMESTAMP))


class TargetShareDB(SQLModel, table=True):
    __tablename__ = "target_shares"
    __table_args__ = (
        # One target share per cluster on any day, so a job counts in one
        # budget at most. Periods are [start_date, end_date): years may touch.
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

    # Primary key (cluster_id, start_date): unique because the exclusion
    # constraint allows one share per cluster at a time.
    cluster_id: int = Field(foreign_key="clusters.id", primary_key=True)

    # The CSV's group_name (e.g. rrg-bengioy-ad). A budget takes the jobs whose
    # account is this name or this name + "_gpu"; "_cpu" accounts stay out, an
    # RGU budget covers GPUs only. NULL, for a budget negotiated for the whole
    # cluster, takes all its jobs.
    allocation: str | None
    # Budget period, end exclusive: the CSV's start and end days. Queries take
    # each day at midnight UTC, through utc_midnight.
    start_date: date = Field(primary_key=True)
    end_date: date
    # A number of RGUs (RGU-years per year), whatever the period length:
    # RGU-hours = rgu_year x hours.
    rgu_year: float


class TargetShareRow(SQLModel):
    """One validated CSV row, before cluster-id resolution."""

    cluster_name: str
    allocation: str | None
    start_date: Annotated[date, BeforeValidator(date.fromisoformat)]
    end_date: Annotated[date, BeforeValidator(date.fromisoformat)]
    rgu_year: float = Field(gt=0)

    @field_validator("allocation")
    @classmethod
    def empty_allocation_as_none(cls, allocation: str | None) -> str | None:
        """A blank CSV allocation field means the whole-cluster budget (NULL)."""
        if allocation is None or not allocation.strip():
            return None
        return allocation

    @field_validator("end_date")
    @classmethod
    def end_after_start(cls, end_date: date, info: ValidationInfo) -> date:
        start_date = info.data.get("start_date")
        if start_date is not None and end_date <= start_date:
            raise ValueError(f"end {end_date} is not after start {start_date}")
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
        for row in reader:
            # values past the header's last column (trailing commas) land under the key None.
            row.pop(None, None)

            if all(not (value or "").strip() for value in row.values()):
                continue  # fully empty row
            rgu_year = (row["rgu_year"] or "").strip()
            if not rgu_year:
                continue  # a cpu, storage or gpu_year row: no RGU budget
            values = {
                "cluster_name": row["cluster_name"],
                "allocation": row["group_name"],
                "start_date": row["start"],
                "end_date": row["end"],
                "rgu_year": rgu_year,
            }
            try:
                rows.append(TargetShareRow.model_validate(values))
            except ValidationError as e:
                raise ValueError(f"{path}:{reader.line_num}: invalid row {row}") from e
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


def budget_bounds():
    """SQL: the target share's period ``[start, end)`` as epoch seconds."""
    return (
        func.extract("epoch", utc_midnight(col(TargetShareDB.start_date))),
        func.extract("epoch", utc_midnight(col(TargetShareDB.end_date))),
    )


def budget_in_window(begin_s: float, end_s: float):
    """SQL predicate: the target share's period meets ``[begin_s, end_s)``,
    bounds in epoch seconds."""
    budget_start, budget_end = budget_bounds()
    return and_(budget_start < end_s, budget_end > begin_s)


def charged_to_budget(account):
    """SQL predicate: a job of this ``account`` counts against the target
    share (the account rule of ``TargetShareDB.allocation``)."""
    return or_(
        col(TargetShareDB.allocation).is_(None),
        account == col(TargetShareDB.allocation),
        account == func.concat(col(TargetShareDB.allocation), "_gpu"),
    )
