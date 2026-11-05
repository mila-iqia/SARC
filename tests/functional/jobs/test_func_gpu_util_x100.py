"""Tests for scripts/fix_gpu_util_x100.py (imported directly, scripts having
no package to live in).

One cache entry holds the raw series of a contaminated job (x100 values,
before the 0.5.1 release), of an ambiguous one (samples at 100 only, one at
or after the release) and of a clean saturated one (a sample at 57 rules out
exporter 0.5.0). The contaminated and ambiguous rows are seeded with the
inflated statistics the corrupted parse stored. Two more candidates have no
raw series to find: their key in the cache entry, and the cache entry itself.

On `jobless_read_write_db`: the seeded jobs of `read_write_db` would widen
the candidate set the tests count.
"""

import importlib.util
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from sqlmodel import select

from sarc.cache import Cache
from sarc.db.cluster import SlurmClusterDB
from sarc.db.job import JobStatisticDB, JobStatisticsFetchDateDB, SlurmJobDB
from sarc.db.users import UserDB
from sarc.scraping.gpu_util_x100 import FIXED_SINCE, X100_SINCE
from tests.db.factory import JobFactory


def _load_script(name: str):
    path = Path(__file__).resolve().parents[3] / "scripts" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


fixer = _load_script("fix_gpu_util_x100")

# One fetch run inside the x100 window, with the microseconds fetch_date
# keeps but the cache filename truncates.
FETCH = datetime(2026, 10, 1, 3, 4, 5, 678901, tzinfo=UTC)
ENTRY = FETCH.replace(microsecond=678000)
# A fetch_date no cache entry was created at.
FETCH_NO_ENTRY = datetime(2026, 10, 1, 9, 9, 9, 0, tzinfo=UTC)

START = datetime.fromtimestamp(X100_SINCE, tz=UTC) + timedelta(days=1)


def _seed(sess, job_ids: list[int]) -> dict[int, SlurmJobDB]:
    """One completed raisin job per id, started inside the x100 window."""
    factory = JobFactory(
        clusters=sess.exec(select(SlurmClusterDB)).all(),
        users=sess.exec(select(UserDB)).all(),
    )
    for job_id in job_ids:
        factory.add_job(
            job_id=job_id,
            job_state="COMPLETED",
            submit_time=START,
            start_time=START,
            elapsed_time=3600,
        )
    for job in factory.jobs:
        sess.add(job)
    sess.commit()
    return {job.job_id: job for job in factory.jobs}


def _seed_stat_and_fetch_date(
    sess, job: SlurmJobDB, mean: float, max_: float, fetch_date: datetime
) -> None:
    sess.add(
        JobStatisticDB(
            job_id=job.id,
            name="gpu_utilization",
            mean=mean,
            std=None,
            q05=None,
            q25=None,
            median=None,
            q75=None,
            max=max_,
        )
    )
    sess.add(JobStatisticsFetchDateDB(job_id=job.id, fetch_date=fetch_date))
    sess.commit()


def _series(job_id: int, name: str, samples: list[tuple[float, float]]) -> dict:
    return {
        "metric": {"__name__": name, "slurmjobid": str(job_id)},
        "values": [[t, str(v)] for t, v in samples],
    }


def _dcgm_series(job_id: int, samples: list[tuple[float, float]]) -> list[dict]:
    """The utilization series plus the PROF metrics ruling out NVML mode
    (sarc.scraping.gpu_util_x100._nvml), the PROF ones undivided."""
    return [
        _series(job_id, "slurm_job_utilization_gpu", samples),
        _series(job_id, "slurm_job_sm_active_gpu", samples),
        _series(job_id, "slurm_job_sm_occupancy_gpu", samples),
    ]


def _key(job: SlurmJobDB) -> str:
    return f"raisin${job.job_id}${job.submit_time.isoformat(timespec='seconds')}"


def _label(job: SlurmJobDB) -> str:
    """Same shape as the script's Candidate.label, for jobs the test holds raw."""
    return f"raisin:{job.job_id}@{job.submit_time.isoformat(timespec='seconds')}"


def _util(sess, job: SlurmJobDB) -> JobStatisticDB | None:
    return sess.exec(
        select(JobStatisticDB).where(
            JobStatisticDB.job_id == job.id, JobStatisticDB.name == "gpu_utilization"
        )
    ).one_or_none()


def _process(sess, cache: Cache, entry_time: datetime, dry_run: bool) -> dict[str, str]:
    candidates = fixer.load_candidates(sess)[entry_time]
    results = fixer.process_entry(sess, cache, entry_time, candidates, dry_run=dry_run)
    return {_label(c.job): status for c, status in results}


def test_fix_rescales_drops_and_leaves(jobless_read_write_db, enabled_cache):
    sess = jobless_read_write_db
    jobs = _seed(sess, [4001, 4002, 4003, 4004, 4005])
    contaminated, ambiguous, saturated = jobs[4001], jobs[4002], jobs[4003]
    missing_key, missing_entry = jobs[4004], jobs[4005]

    _seed_stat_and_fetch_date(sess, contaminated, 50.0, 100.0, FETCH)
    _seed_stat_and_fetch_date(sess, ambiguous, 0.5, 1.0, FETCH)
    _seed_stat_and_fetch_date(sess, saturated, 0.785, 1.0, FETCH)
    _seed_stat_and_fetch_date(sess, missing_key, 0.5, 1.0, FETCH)
    _seed_stat_and_fetch_date(sess, missing_entry, 1.5, 3.0, FETCH_NO_ENTRY)

    t0 = X100_SINCE + 3600
    cache = Cache("prometheus")
    with cache.create_entry(FETCH) as ce:
        ce.add_value(
            _key(contaminated),
            json.dumps(
                _dcgm_series(4001, [(t0, 5000), (t0 + 30, 10000), (t0 + 60, 0)])
            ).encode(),
        )
        ce.add_value(
            _key(ambiguous),
            json.dumps(
                _dcgm_series(
                    4002,
                    [
                        (t0, 0),
                        (t0 + 30, 100),
                        (FIXED_SINCE + 30, 0),
                        (FIXED_SINCE + 60, 100),
                    ],
                )
            ).encode(),
        )
        ce.add_value(
            _key(saturated),
            json.dumps(_dcgm_series(4003, [(t0, 57), (t0 + 30, 100)])).encode(),
        )
    # missing_key's job has a fetch_date at this entry but no key in it.

    assert sorted(fixer.load_candidates(sess)) == [ENTRY, FETCH_NO_ENTRY]
    assert len(fixer.load_candidates(sess)[ENTRY]) == 4

    statuses = _process(sess, cache, ENTRY, dry_run=True)
    assert statuses[_label(contaminated)] == fixer.STATUS_RESCALED
    assert statuses[_label(ambiguous)] == fixer.STATUS_AMBIGUOUS
    assert statuses[_label(saturated)] == fixer.STATUS_UNCHANGED
    assert statuses[_label(missing_key)] == fixer.STATUS_MISSING_KEY
    assert (
        _process(sess, cache, FETCH_NO_ENTRY, dry_run=True)[_label(missing_entry)]
        == fixer.STATUS_MISSING_ENTRY
    )

    # The dry runs rolled back: every stored row is where it was.
    sess.expire_all()
    assert (_util(sess, contaminated).mean, _util(sess, contaminated).max) == (
        50.0,
        100.0,
    )
    assert _util(sess, ambiguous) is not None

    statuses = _process(sess, cache, ENTRY, dry_run=False)
    _process(sess, cache, FETCH_NO_ENTRY, dry_run=False)
    assert statuses[_label(missing_key)] == fixer.STATUS_MISSING_KEY

    # The contaminated row comes back divided by 100, the ambiguous row is
    # gone, the rows the parse judges clean -- or cannot reach -- are untouched.
    sess.expire_all()
    assert (_util(sess, contaminated).mean, _util(sess, contaminated).max) == (0.5, 1.0)
    assert _util(sess, ambiguous) is None
    assert (_util(sess, saturated).mean, _util(sess, saturated).max) == (0.785, 1.0)
    assert (_util(sess, missing_key).mean, _util(sess, missing_key).max) == (0.5, 1.0)
    assert (_util(sess, missing_entry).mean, _util(sess, missing_entry).max) == (
        1.5,
        3.0,
    )

    # Re-running converges: the rescaled row is still a candidate (its divided
    # max is exactly 1.0) but lands on the same statistics.
    _process(sess, cache, ENTRY, dry_run=False)
    sess.expire_all()
    assert (_util(sess, contaminated).mean, _util(sess, contaminated).max) == (0.5, 1.0)
    assert _util(sess, ambiguous) is None
    assert (_util(sess, saturated).mean, _util(sess, saturated).max) == (0.785, 1.0)
