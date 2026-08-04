"""Tests for classify_cycle() (sarc.notifications.usage)."""

from datetime import UTC, datetime, timedelta

import pytest
from sqlmodel import select

from sarc.db.cluster import SlurmClusterDB
from sarc.db.users import UserDB
from sarc.notifications.usage import classify_cycle, get_underusers_usage
from tests.unittests.notifications._factory import add_gpu_job

# A single 14-day cycle (matches _CYCLE_LENGTH_WEEKS=2), so with
# recurrence_active_cycles=1 the personalized-action window equals the cycle
# window exactly — no extra history needed to isolate PA-floor behavior.
_CYCLE_LENGTH_WEEKS = 2
_CYCLE_START = datetime(2024, 6, 16, tzinfo=UTC)
_CYCLE_END = datetime(2024, 6, 30, tzinfo=UTC)

_MIN_WASTE_RATIO = 0.50
_MIN_WASTE_RGU_HOURS = 100.0
_PA_MIN_WASTE_RGU_HOURS = 400.0

# mila: billing_is_gpu=True, gpu_type A100-SXM4-80GB → rgu = 4.8
_MILA_RGU = 4.8


def _job(
    session, *, user_id, cluster_id, elapsed_h, utilization, job_id, end_offset_h=0
):
    """Seed a job whose end_time is `_CYCLE_START + end_offset_h`, regardless of
    elapsed_h — only end_time (not submit_time) is windowed by classify_cycle."""
    submit_time = (
        _CYCLE_START - timedelta(hours=elapsed_h) + timedelta(hours=end_offset_h)
    )
    return add_gpu_job(
        session,
        user_id=user_id,
        cluster_id=cluster_id,
        elapsed_h=elapsed_h,
        submit_time=submit_time,
        job_id=job_id,
        utilization=utilization,
    )


@pytest.fixture
def classify_db(read_write_db):
    session = read_write_db
    users = {u.email.split("@")[0]: u for u in session.exec(select(UserDB)).all()}
    mila_id = next(
        c.id for c in session.exec(select(SlurmClusterDB)).all() if c.name == "mila"
    )

    petitbonhomme_id = users["petitbonhomme"].id
    beaubonhomme_id = users["beaubonhomme"].id
    bramin_id = users["bramin"].id

    # petitbonhomme: ratio=0.90 >= 0.50, wasted=4.8*100*0.90=432 >= both floors.
    # isunderuser=True, flagged=True.
    _job(
        session,
        user_id=petitbonhomme_id,
        cluster_id=mila_id,
        elapsed_h=100,
        utilization=0.10,
        job_id=90001,
    )
    # beaubonhomme: ratio=0.20 < 0.50 (isunderuser=False) despite
    # wasted=4.8*2000*0.20=1920 clearing BOTH the activity floor and the PA
    # floor -- isolates that flagged requires isunderuser, not just the PA
    # floor alone. end_offset_h keeps end_time inside the 336h cycle window
    # despite the long job duration (only end_time, not submit_time, is
    # windowed).
    _job(
        session,
        user_id=beaubonhomme_id,
        cluster_id=mila_id,
        elapsed_h=2000,
        utilization=0.80,
        job_id=90002,
        end_offset_h=1,
    )
    # bramin: ratio=0.70 >= 0.50 (isunderuser=True), wasted=4.8*100*0.70=336 —
    # clears the activity floor (100) but not the PA floor (400) -- isolates
    # that flagged requires the PA floor too, not just isunderuser alone.
    _job(
        session,
        user_id=bramin_id,
        cluster_id=mila_id,
        elapsed_h=100,
        utilization=0.30,
        job_id=90003,
    )

    session.commit()
    yield session


def _classify(*, recurrence_active_cycles=1, utilization_ceiling=1.0):
    return classify_cycle(
        _CYCLE_START,
        _CYCLE_END,
        min_waste_ratio=_MIN_WASTE_RATIO,
        min_waste_rgu_hours=_MIN_WASTE_RGU_HOURS,
        personalized_action_min_waste_rgu_hours=_PA_MIN_WASTE_RGU_HOURS,
        recurrence_active_cycles=recurrence_active_cycles,
        cycle_length_weeks=_CYCLE_LENGTH_WEEKS,
        utilization_ceiling=utilization_ceiling,
    )


def test_rgu_math(classify_db):
    stats = _classify()
    row = next(
        s for s in stats if s.cluster == "mila" and s.rgu_hours == pytest.approx(480.0)
    )
    assert row.rgu_hours == pytest.approx(_MILA_RGU * 100)
    assert row.wasted == pytest.approx(_MILA_RGU * 100 * 0.90)
    assert row.sm_occ_mean == pytest.approx(0.10)


def test_isunderuser_matches_get_underusers_usage_membership(classify_db):
    stats = _classify()
    classify_underusers = {s.user_id for s in stats if s.isunderuser}

    live_rows = get_underusers_usage(
        _CYCLE_START,
        _CYCLE_END,
        min_waste_ratio=_MIN_WASTE_RATIO,
        min_waste_rgu_hours=_MIN_WASTE_RGU_HOURS,
        top_jobs_per_user=0,
    )
    live_underusers = {r.user_id for r in live_rows}

    assert classify_underusers == live_underusers
    assert classify_underusers  # non-empty: sanity check the fixture is wired up


def test_flagged_requires_pa_floor_and_isunderuser(classify_db):
    stats = {s.user_id: s for s in _classify()}
    users = {
        u.email.split("@")[0]: u.id for u in classify_db.exec(select(UserDB)).all()
    }

    # Meets both isunderuser and the PA floor.
    assert stats[users["petitbonhomme"]].isunderuser is True
    assert stats[users["petitbonhomme"]].flagged is True

    # Meets the PA floor (wasted=1920 >= 400) but not isunderuser (ratio=0.20).
    assert stats[users["beaubonhomme"]].isunderuser is False
    assert stats[users["beaubonhomme"]].flagged is False

    # Meets isunderuser (ratio=0.70) but not the PA floor (wasted=336 < 400).
    assert stats[users["bramin"]].isunderuser is True
    assert stats[users["bramin"]].flagged is False


def test_utilization_ceiling_adjustment(classify_db):
    users = {
        u.email.split("@")[0]: u.id for u in classify_db.exec(select(UserDB)).all()
    }
    petitbonhomme_id = users["petitbonhomme"]

    # m=0.10. At T=1.0: wasted = 480*(1-0.10) = 432.
    full = next(
        s for s in _classify(utilization_ceiling=1.0) if s.user_id == petitbonhomme_id
    )
    assert full.wasted == pytest.approx(432.0)
    assert full.sm_occ_mean == pytest.approx(0.10)

    # At T=0.5: wasted = 480*(0.5-0.10) = 192. sm_occ_mean stays raw (0.10).
    half = next(
        s for s in _classify(utilization_ceiling=0.5) if s.user_id == petitbonhomme_id
    )
    assert half.wasted == pytest.approx(192.0)
    assert half.sm_occ_mean == pytest.approx(0.10)

    # At T=0.05 < m: wasted floors at 0 (max(0, T-m)).
    below = next(
        s for s in _classify(utilization_ceiling=0.05) if s.user_id == petitbonhomme_id
    )
    assert below.wasted == pytest.approx(0.0, abs=1e-6)
    assert below.sm_occ_mean == pytest.approx(0.10)
