from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text
from sqlmodel import Session, select

from sarc.db.cluster import SlurmClusterDB
from sarc.db.job import SlurmJobDB
from sarc.db.support import GpuRguDB
from sarc.db.target_share import TargetShareDB, target_share_report
from sarc.db.users import UserDB
from sarc.models.job import SlurmState

# Window fully inside every budget below, so a budget's prorated target is
# rgu_year x 888 h (2026-03-25 -> 2026-05-01) unless the budget starts inside
# it (fromage: 720 h from 2026-04-01).
BEGIN = datetime(2026, 3, 25, tzinfo=UTC)
END = datetime(2026, 5, 1, tzinfo=UTC)


def cluster_id(sess: Session, name: str) -> int:
    return sess.exec(select(SlurmClusterDB.id).where(SlurmClusterDB.name == name)).one()


def add_budget(
    sess: Session,
    cluster: str,
    allocation: str | None,
    rgu_year: float,
    start: str,
    end: str,
) -> None:
    sess.add(
        TargetShareDB(
            cluster_id=cluster_id(sess, cluster),
            allocation=allocation,
            start_date=start,
            end_date=end,
            rgu_year=rgu_year,
        )
    )


def utc_day(instant: str) -> datetime:
    return datetime.fromisoformat(instant).replace(tzinfo=UTC)


def add_job(
    sess: Session,
    gpu: GpuRguDB,
    user: UserDB,
    *,
    cluster: str,
    account: str,
    start: datetime,
    hours: float,
    gpus: int,
    job_id: int,
) -> None:
    elapsed = hours * 3600
    sess.add(
        SlurmJobDB(
            cluster_id=cluster_id(sess, cluster),
            sarc_user_id=user.id,
            submit_time=start,
            start_time=start,
            end_time=start + timedelta(seconds=elapsed),
            elapsed_time=elapsed,
            allocated_gres_gpu=gpus,
            allocated_billing=gpus,
            allocated_gpu_type=gpu.name,
            harmonized_gpu_type=gpu.name,
            requested_gres_gpu=gpus,
            account=account,
            job_id=job_id,
            name="name",
            cluster_user="user",
            group="group",
            partition="partition",
            job_state=SlurmState.COMPLETED,
            nodes=[],
            work_dir="work_dir",
            submit_line=None,
        )
    )


@pytest.fixture
def scenario(jobless_read_write_db: Session):
    """One budgeted DRAC-shaped cluster (fromage), one whole-cluster budget
    (patate), one zero-consumption budget (hyrule) and one unbudgeted cluster
    (gerudo)."""
    sess = jobless_read_write_db
    gpu = sess.exec(select(GpuRguDB).order_by(GpuRguDB.name)).first()
    user = sess.exec(select(UserDB)).first()
    assert gpu is not None and user is not None

    add_budget(sess, "fromage", "rrg-fromage-ad", 10, "2026-04-01", "2027-04-01")
    add_budget(sess, "patate", None, 4, "2026-03-01", "2027-03-01")
    add_budget(sess, "hyrule", "rrg-hyrule-ad", 1, "2026-01-01", "2027-01-01")

    day = utc_day
    add_job(  # 24 h x 2 GPUs, fully in budget
        sess,
        gpu,
        user,
        cluster="fromage",
        account="rrg-fromage-ad_gpu",
        start=day("2026-04-01T00:00"),
        hours=24,
        gpus=2,
        job_id=1,
    )
    add_job(  # straddles the budget start: 12 h in, 12 h before it
        sess,
        gpu,
        user,
        cluster="fromage",
        account="rrg-fromage-ad_gpu",
        start=day("2026-03-31T12:00"),
        hours=24,
        gpus=1,
        job_id=2,
    )
    add_job(  # bare group account matches the budget too
        sess,
        gpu,
        user,
        cluster="fromage",
        account="rrg-fromage-ad",
        start=day("2026-04-02T00:00"),
        hours=10,
        gpus=1,
        job_id=3,
    )
    add_job(  # _cpu leaf: not part of an RGU budget
        sess,
        gpu,
        user,
        cluster="fromage",
        account="rrg-fromage-ad_cpu",
        start=day("2026-04-03T00:00"),
        hours=10,
        gpus=1,
        job_id=4,
    )
    add_job(  # def- account draws the institutional default, not the budget
        sess,
        gpu,
        user,
        cluster="fromage",
        account="def-fromage_gpu",
        start=day("2026-04-04T00:00"),
        hours=5,
        gpus=1,
        job_id=5,
    )
    add_job(  # budgeted account, but before the budget period opened
        sess,
        gpu,
        user,
        cluster="fromage",
        account="rrg-fromage-ad_gpu",
        start=day("2026-03-26T00:00"),
        hours=6,
        gpus=1,
        job_id=6,
    )
    add_job(  # patate's NULL allocation attributes every account
        sess,
        gpu,
        user,
        cluster="patate",
        account="rrg-patate_gpu",
        start=day("2026-04-05T00:00"),
        hours=3,
        gpus=2,
        job_id=7,
    )
    add_job(
        sess,
        gpu,
        user,
        cluster="patate",
        account="some-odd-account",
        start=day("2026-04-06T00:00"),
        hours=3,
        gpus=1,
        job_id=8,
    )
    add_job(  # gerudo carries no budget notion at all
        sess,
        gpu,
        user,
        cluster="gerudo",
        account="rrg-gerudo_gpu",
        start=day("2026-04-06T00:00"),
        hours=4,
        gpus=1,
        job_id=9,
    )
    sess.commit()
    return gpu.drac_rgu


@pytest.mark.parametrize("session_tz", ["UTC", "America/Montreal"])
def test_report(jobless_read_write_db: Session, scenario: float, session_tz: str):
    """Budget bounds are UTC midnights whatever the session TimeZone."""
    w = scenario
    jobless_read_write_db.connection().execute(
        text(f"SET LOCAL TimeZone = '{session_tz}'")
    )
    budgets, unbudgeted = target_share_report(
        jobless_read_write_db, begin=BEGIN, end=END
    )

    assert [(b.cluster_name, b.allocation) for b in budgets] == [
        ("fromage", "rrg-fromage-ad"),
        ("hyrule", "rrg-hyrule-ad"),
        ("patate", None),
    ]
    fromage, hyrule, patate = budgets
    assert fromage.target_rgu_hours == pytest.approx(10 * 720)
    assert fromage.consumed_rgu_hours == pytest.approx((24 * 2 + 12 + 10) * w), (
        "gpu-suffix and bare accounts count, clipped to the budget period"
    )
    assert hyrule.target_rgu_hours == pytest.approx(1 * 888)
    assert hyrule.consumed_rgu_hours == 0
    assert patate.target_rgu_hours == pytest.approx(4 * 888)
    assert patate.consumed_rgu_hours == pytest.approx((3 * 2 + 3) * w)

    assert [(u.cluster_name, u.consumed_rgu_hours) for u in unbudgeted] == [
        ("fromage", pytest.approx((12 + 10 + 5 + 6) * w))
    ], "_cpu leaf, def- and pre-budget hours only, on budgeted clusters"


def test_cluster_filter(jobless_read_write_db: Session, scenario: float):
    w = scenario
    budgets, unbudgeted = target_share_report(
        jobless_read_write_db, begin=BEGIN, end=END, cluster_name="fromage"
    )
    assert [b.cluster_name for b in budgets] == ["fromage"]
    assert budgets[0].consumed_rgu_hours == pytest.approx((24 * 2 + 12 + 10) * w)
    assert [u.cluster_name for u in unbudgeted] == ["fromage"]

    budgets, unbudgeted = target_share_report(
        jobless_read_write_db, begin=BEGIN, end=END, cluster_name="gerudo"
    )
    assert budgets == [] and unbudgeted == []


def test_window_outside_every_budget(jobless_read_write_db: Session, scenario: float):
    budgets, unbudgeted = target_share_report(
        jobless_read_write_db,
        begin=datetime(2024, 1, 1, tzinfo=UTC),
        end=datetime(2024, 2, 1, tzinfo=UTC),
    )
    assert budgets == [] and unbudgeted == []


def test_empty_window(jobless_read_write_db: Session, scenario: float):
    budgets, unbudgeted = target_share_report(jobless_read_write_db, begin=END, end=END)
    assert budgets == [] and unbudgeted == []
