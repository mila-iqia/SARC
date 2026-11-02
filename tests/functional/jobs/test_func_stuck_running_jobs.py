"""
Tests for scripts/estimate_stuck_running_jobs.py and
scripts/fix_stuck_running_jobs.py (imported directly, scripts having no
package to live in).

On `jobless_read_write_db`: the seeded jobs of `read_write_db` include
RUNNING rows of their own, and the tests want the rows they seed to be the
only ones the scripts see.

The registry tests install their own `HEURISTICS` and check the mechanics of
that registry: the order it is asked in, what makes a heuristic fall through,
and how the per-job state override reaches the fix file. The prometheus
heuristic is asked through a fake `config`, its cluster answering range
queries with hand-made series.
"""

import importlib.util
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlmodel import select

from sarc.db.cluster import SlurmClusterDB
from sarc.db.job import SlurmJobDB
from sarc.db.users import UserDB
from sarc.models.job import SlurmState
from tests.db.factory import JobFactory


def _load_script(name: str):
    path = Path(__file__).resolve().parents[3] / "scripts" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


estimator = _load_script("estimate_stuck_running_jobs")
fixer = _load_script("fix_stuck_running_jobs")

NOW = datetime(2026, 10, 15, 12, 0, tzinfo=UTC)
# The start of the runs the tests corrupt: well past --grace hours ago.
START = NOW - timedelta(days=10)
DURATION = 7200.0

# Ten days of RUNNING past a one-hour limit: stuck.
STUCK = dict(
    job_state="RUNNING", start_time=START, time_limit=3600, elapsed_time=10 * 86400
)


def _seed(sess, **kwargs) -> SlurmJobDB:
    """Add one job (raisin, petitbonhomme, by default) to the database."""
    factory = JobFactory(
        clusters=sess.exec(select(SlurmClusterDB)).all(),
        users=sess.exec(select(UserDB)).all(),
    )
    factory.add_job(**kwargs)
    job = factory.jobs[-1]
    sess.add(job)
    sess.commit()
    return job


def _stuck_job(job_id: int = 42, **overrides) -> "estimator.StuckJob":
    """A StuckJob that never saw a database, for the registry tests."""
    fields = dict(
        cluster_name="raisin",
        job_id=job_id,
        submit_time=START - timedelta(seconds=60),
        start_time=START,
        time_limit=3600,
        elapsed_time=10 * 86400,
        nodes=("cn-c021",),
    )
    return estimator.StuckJob(**(fields | overrides))


def _entry(module, job_id: int, **overrides) -> "fixer.FixEntry":
    fields = dict(
        cluster_name="raisin",
        job_id=job_id,
        submit_time=START,
        duration=DURATION,
        state=SlurmState.NODE_FAIL,
    )
    return module.FixEntry(**(fields | overrides))


# Finding the stuck rows


@pytest.mark.time_machine(NOW, tick=False)
def test_find_stuck_rows(jobless_read_write_db):
    sess = jobless_read_write_db
    _seed(sess, job_id=1001, submit_time=START, **STUCK)
    # A resubmit of the same id that started at the very same instant: two
    # rows, two keys, since the fix file identifies by submit_time.
    _seed(sess, job_id=1001, submit_time=START - timedelta(hours=1), **STUCK)
    # A run that hit its limit only an hour ago is within the grace period.
    _seed(
        sess,
        job_id=1003,
        job_state="RUNNING",
        start_time=NOW - timedelta(hours=2),
        submit_time=NOW - timedelta(hours=3),
        time_limit=3600,
    )
    # A run with no time limit cannot be told, a closed run is not stuck.
    _seed(sess, job_id=1004, job_state="RUNNING", start_time=START, time_limit=None)
    _seed(
        sess,
        job_id=1005,
        job_state="COMPLETED",
        start_time=START,
        time_limit=3600,
        elapsed_time=3600,
    )

    grace = timedelta(hours=24)
    jobs = estimator.find_stuck_jobs(sess, grace=grace)
    assert [(j.cluster_name, j.job_id, j.submit_time) for j in jobs] == [
        ("raisin", 1001, START - timedelta(hours=1)),
        ("raisin", 1001, START),
    ]
    assert len({j.key for j in jobs}) == 2  # one key per stuck row
    assert jobs[0].time_limit == 3600
    assert jobs[0].nodes == ("cn-c021",)

    # The filters narrow what is considered.
    assert estimator.find_stuck_jobs(sess, cluster_names=["fromage"], grace=grace) == []
    assert (
        estimator.find_stuck_jobs(sess, since=NOW - timedelta(days=1), grace=grace)
        == []
    )
    # A tighter grace also catches the run that just passed its limit.
    assert [j.job_id for j in estimator.find_stuck_jobs(sess, grace=timedelta(0))] == [
        1001,
        1001,
        1003,
    ]


# The heuristic registry


def test_heuristics_are_asked_in_order_until_one_answers(monkeypatch):
    asked: list[str] = []

    def boom(job):
        asked.append("boom")
        raise RuntimeError("a heuristic may crash")

    def shrug(job):
        asked.append("shrug")

    def zero(job):
        asked.append("zero")
        return estimator.Estimate(duration=0)

    def answer(job):
        asked.append("answer")
        return estimator.Estimate(duration=DURATION, note="because")

    monkeypatch.setattr(estimator, "HEURISTICS", [boom, shrug, zero, answer])
    job = _stuck_job()
    entries, no_estimate = estimator.estimate_durations([job], SlurmState.NODE_FAIL)
    assert asked == ["boom", "shrug", "zero", "answer"]
    assert no_estimate == []
    assert entries == [
        {
            "cluster_name": "raisin",
            "job_id": 42,
            "submit_time": (START - timedelta(seconds=60)).isoformat(),
            "duration": DURATION,
            "state": "NODE_FAIL",
            "source": "answer",
            "note": "because",
        }
    ]


def test_job_no_heuristics_answer_is_left_out(monkeypatch):
    monkeypatch.setattr(estimator, "HEURISTICS", [lambda job: None, lambda job: None])
    job = _stuck_job()
    entries, no_estimate = estimator.estimate_durations([job], SlurmState.NODE_FAIL)
    assert entries == []
    assert no_estimate == [job]


def test_a_heuristic_can_override_the_state_per_job(monkeypatch):
    def answer(job):
        if job.job_id == 43:
            return estimator.Estimate(duration=DURATION, state=SlurmState.TIMEOUT)
        return estimator.Estimate(duration=DURATION)

    monkeypatch.setattr(estimator, "HEURISTICS", [answer])
    entries, _ = estimator.estimate_durations(
        [_stuck_job(42), _stuck_job(43)], SlurmState.NODE_FAIL
    )
    assert [e["state"] for e in entries] == ["NODE_FAIL", "TIMEOUT"]


def test_fix_file_roundtrip(tmp_path, monkeypatch):
    def answer(job):
        if job.job_id == 43:
            return estimator.Estimate(duration=DURATION, state=SlurmState.TIMEOUT)
        return estimator.Estimate(duration=DURATION)

    monkeypatch.setattr(estimator, "HEURISTICS", [answer])
    entries, _ = estimator.estimate_durations(
        [_stuck_job(43), _stuck_job(42)], SlurmState.NODE_FAIL
    )
    path = tmp_path / "fix.json"
    estimator.write_fix_file(path, entries, SlurmState.NODE_FAIL)

    document = json.loads(path.read_text())
    assert document["version"] == 1
    assert document["default_state"] == "NODE_FAIL"
    # Sorted by (cluster, submit_time, job_id), whatever order came in.
    assert [job["job_id"] for job in document["jobs"]] == [42, 43]

    loaded = fixer.load_fix_file(path)
    assert [(e.job_id, e.state) for e in loaded] == [
        (42, SlurmState.NODE_FAIL),
        (43, SlurmState.TIMEOUT),
    ]
    assert loaded[0].duration == DURATION
    assert loaded[0].submit_time == START - timedelta(seconds=60)


@pytest.mark.parametrize(
    "document",
    [
        {"version": 2, "jobs": []},
        {"version": 1, "jobs": [{"cluster_name": "raisin"}]},
        {
            "version": 1,
            "jobs": [{**_entry(fixer, 42).model_dump(mode="json"), "duration": 0}],
        },
        {
            "version": 1,
            "jobs": [
                {
                    **_entry(fixer, 42).model_dump(mode="json"),
                    "submit_time": "2026-10-05T00:00:00",
                }
            ],
        },
    ],
)
def test_load_fix_file_refuses_bad_files(tmp_path, document):
    path = tmp_path / "fix.json"
    path.write_text(json.dumps(document))
    with pytest.raises(SystemExit):
        fixer.load_fix_file(path)


def test_load_fix_file_refuses_a_state_of_running(tmp_path):
    entry = {**_entry(fixer, 42).model_dump(mode="json"), "state": "RUNNING"}
    path = tmp_path / "fix.json"
    path.write_text(json.dumps({"version": 1, "jobs": [entry]}))
    with pytest.raises(SystemExit):
        fixer.load_fix_file(path)


# The prometheus heuristic


def _series(node: str, times: list[float], job_id: int = 42) -> dict:
    """One raw series as `custom_query_range` returns it, instance and port."""
    return {
        "metric": {
            "__name__": "slurm_job_core_usage",
            "slurmjobid": str(job_id),
            "instance": f"{node}:9250",
        },
        "values": [[t, "1.0"] for t in times],
    }


def _from_start(seconds: int, *, first: int = 0) -> list[float]:
    """One sample every 30s over `seconds`, as absolute timestamps."""
    return [START.timestamp() + first + s for s in range(0, seconds + 1, 30)]


class _FakeProm:
    """The one method of PrometheusConnect the heuristic calls."""

    def __init__(self, series: list[dict]):
        self.series = series
        self.queries: list[tuple] = []

    def custom_query_range(self, query, start_time, end_time, step, **kw):
        self.queries.append((query, start_time, end_time, step))
        return self.series


def _prometheus(monkeypatch, series: list[dict]) -> _FakeProm:
    """Give the raisin cluster a fake Prometheus answering with these series."""
    prom = _FakeProm(series)
    monkeypatch.setattr(
        estimator,
        "config",
        SimpleNamespace(
            clusters={
                "raisin": SimpleNamespace(
                    prometheus_url="http://prom.example", prometheus=prom
                )
            }
        ),
    )
    monkeypatch.setattr(estimator, "PROM_FAILED", set())
    return prom


def test_heuristics_are_the_cache_then_prometheus():
    assert estimator.HEURISTICS == [
        estimator.closed_state_in_cache,
        estimator.stopped_data_in_prometheus,
    ]


@pytest.mark.time_machine(NOW, tick=False)
def test_prometheus_end_is_where_the_samples_stop(monkeypatch):
    prom = _prometheus(monkeypatch, [_series("cn-c021", _from_start(3600))])
    estimate = estimator.stopped_data_in_prometheus(_stuck_job())
    assert estimate is not None
    assert estimate.duration == 3600.0
    assert estimate.state is None  # when it ended is told, never how
    assert "cn-c021" in estimate.note
    # One hour limit plus one day of lookahead: the window fits one query.
    assert len(prom.queries) == 1
    query = prom.queries[0][0]
    assert 'slurmjobid=~"^42$"' in query
    assert "slurm_job_core_usage" in query


@pytest.mark.time_machine(NOW, tick=False)
def test_prometheus_long_windows_are_cut_into_queries(monkeypatch):
    prom = _prometheus(monkeypatch, [_series("cn-c021", _from_start(3600))])
    # A week's window is more than one query's worth of 30s evaluations.
    estimate = estimator.stopped_data_in_prometheus(_stuck_job(time_limit=6 * 86400))
    assert estimate is not None
    assert estimate.duration == 3600.0
    assert len(prom.queries) == 2


@pytest.mark.time_machine(NOW, tick=False)
def test_prometheus_multi_node_runs_until_the_last_node_falls_silent(monkeypatch):
    _prometheus(
        monkeypatch,
        [_series("cn-c021", _from_start(3600)), _series("cn-c022", _from_start(5400))],
    )
    job = _stuck_job(nodes=("cn-c021", "cn-c022"))
    estimate = estimator.stopped_data_in_prometheus(job)
    assert estimate is not None
    assert estimate.duration == 5400.0


@pytest.mark.time_machine(NOW, tick=False)
def test_prometheus_a_requeue_on_other_nodes_ends_the_run(monkeypatch):
    # The requeue starts 30s after the run stopped, a gap too small for the
    # resolution to see -- but it landed on other nodes, and that switch is
    # where this run's data ends.
    _prometheus(
        monkeypatch,
        [
            _series("cn-c021", _from_start(3600)),
            _series("cn-l007", _from_start(3600, first=3630)),
        ],
    )
    estimate = estimator.stopped_data_in_prometheus(_stuck_job())
    assert estimate is not None
    assert estimate.duration == 3600.0
    assert "cn-l007" in estimate.note


@pytest.mark.time_machine(NOW, tick=False)
def test_prometheus_a_requeue_on_the_same_nodes_breaks_on_the_gap(monkeypatch):
    # Ten minutes of nothing between the samples: no scrape explains that, and
    # the id reporting again there is a requeued run.
    _prometheus(
        monkeypatch,
        [_series("cn-c021", _from_start(3600) + _from_start(3000, first=4200))],
    )
    estimate = estimator.stopped_data_in_prometheus(_stuck_job())
    assert estimate is not None
    assert estimate.duration == 3600.0
    assert "same nodes" in estimate.note


@pytest.mark.time_machine(NOW, tick=False)
def test_prometheus_still_reporting_says_nothing(monkeypatch):
    # Samples until the very end of the window: no stopping to read an end
    # out of, wherever the run really went.
    _prometheus(monkeypatch, [_series("cn-c021", _from_start(93_540, first=-3600))])
    assert estimator.stopped_data_in_prometheus(_stuck_job()) is None


@pytest.mark.time_machine(NOW, tick=False)
def test_prometheus_no_samples_say_nothing(monkeypatch):
    _prometheus(monkeypatch, [])
    assert estimator.stopped_data_in_prometheus(_stuck_job()) is None


@pytest.mark.time_machine(NOW, tick=False)
def test_prometheus_data_missing_the_start_says_nothing(monkeypatch):
    # Only a later run of the id is left in the data: nothing ties it to the
    # start this row claims.
    _prometheus(monkeypatch, [_series("cn-c021", _from_start(3600, first=3660))])
    assert estimator.stopped_data_in_prometheus(_stuck_job()) is None


@pytest.mark.time_machine(NOW, tick=False)
def test_prometheus_data_on_other_nodes_is_another_run(monkeypatch):
    # The id reported right at this row's start, but from nodes the row never
    # had: that is another run's samples, not this one's.
    _prometheus(monkeypatch, [_series("cn-l007", _from_start(3600))])
    assert estimator.stopped_data_in_prometheus(_stuck_job()) is None


@pytest.mark.time_machine(NOW, tick=False)
def test_prometheus_unreadable_is_complained_about_once(monkeypatch):
    prom = _prometheus(monkeypatch, [])

    def boom(*args, **kwargs):
        raise RuntimeError("prometheus is down")

    monkeypatch.setattr(prom, "custom_query_range", boom)
    assert estimator.stopped_data_in_prometheus(_stuck_job()) is None
    assert estimator.PROM_FAILED == {"raisin"}
    assert estimator.stopped_data_in_prometheus(_stuck_job()) is None


def test_prometheus_a_cluster_without_it_says_nothing(monkeypatch):
    monkeypatch.setattr(estimator, "config", SimpleNamespace(clusters={}))
    assert estimator.stopped_data_in_prometheus(_stuck_job()) is None


# Applying the fix file


def test_fix_closes_only_the_stuck_rows(jobless_read_write_db):
    sess = jobless_read_write_db
    stuck = _seed(sess, job_id=1001, submit_time=START, **STUCK)
    # The twin started at its own instant: the end time the fixer writes
    # comes from the row's start_time, the entry only carries submit_time.
    twin = _seed(
        sess,
        job_id=1001,
        submit_time=START - timedelta(hours=1),
        **{**STUCK, "start_time": START - timedelta(minutes=30)},
    )
    closed = _seed(
        sess,
        job_id=1005,
        submit_time=START - timedelta(days=1),
        job_state="COMPLETED",
        start_time=START,
        time_limit=3600,
        elapsed_time=3600,
    )
    entries = [
        _entry(fixer, 1001),  # the stuck run
        _entry(fixer, 1001, submit_time=START - timedelta(hours=1)),  # and its twin
        _entry(fixer, 999999),  # no row for this job
        _entry(fixer, 1005, submit_time=START - timedelta(days=1)),  # closed properly
        _entry(fixer, 1001, cluster_name="nowhere"),  # not a cluster
    ]
    statuses = [
        fixer.STATUS_FIXED,
        fixer.STATUS_FIXED,
        fixer.STATUS_NO_ROW,
        fixer.STATUS_NOT_RUNNING,
        fixer.STATUS_UNKNOWN_CLUSTER,
    ]

    assert [
        r.status for r in fixer.fix_stuck_running_jobs(sess, entries, dry_run=True)
    ] == statuses
    for row in (stuck, twin):
        sess.refresh(row)
        assert row.job_state == SlurmState.RUNNING
        assert row.end_time is None

    results = fixer.fix_stuck_running_jobs(sess, entries, dry_run=False)
    assert [r.status for r in results] == statuses
    assert (results[0].row_id, results[1].row_id) == (stuck.id, twin.id)
    for row in (stuck, twin):
        sess.refresh(row)
        assert row.job_state == SlurmState.NODE_FAIL
        assert row.end_time == row.start_time + timedelta(seconds=DURATION)
        assert row.elapsed_time == DURATION
    sess.refresh(closed)
    assert closed.job_state == SlurmState.COMPLETED
    assert closed.elapsed_time == 3600

    # Re-applying the fix file touches nothing: the rows are no longer RUNNING.
    results = fixer.fix_stuck_running_jobs(sess, entries, dry_run=False)
    assert [r.status for r in results] == [
        fixer.STATUS_NOT_RUNNING,
        fixer.STATUS_NOT_RUNNING,
        fixer.STATUS_NO_ROW,
        fixer.STATUS_NOT_RUNNING,
        fixer.STATUS_UNKNOWN_CLUSTER,
    ]
    assert results[0].state == "NODE_FAIL"
