import copy
import json
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlmodel import select

from sarc.cache import Cache
from sarc.config import config
from sarc.db.cluster import SlurmClusterDB
from sarc.db.job import JobStatisticDB, SlurmJobDB
from sarc.scraping import gpu_accounting
from sarc.scraping.jobs import STEPS_SUFFIX, fetch_jobs, parse_jobs
from sarc.scraping.prometheus import fetch_prometheus

MEMORY = "sacct_gpu_memory_task_peak_bytes"
SM = "sacct_gpu_sm_task_peak_percent"
SINCE = datetime(2020, 1, 1, tzinfo=UTC)
INTERVAL = ["2023-02-15T00:00-2023-02-16T00:00"]


@pytest.fixture
def accounting_payload(create_sacct_json):
    path = Path(__file__).parents[2] / "data/sacct/fir_gpu_accounting_26_05_4.json"
    recorded = json.loads(path.read_text())["jobs"][0]
    payload = json.loads(
        create_sacct_json(
            [
                {"job_id": i, "tres": {"allocated": recorded["tres"]["allocated"]}}
                for i in (1, 2)
            ]
        )
    )
    for job in payload["jobs"]:
        job["steps"] = copy.deepcopy(recorded["steps"])
    return payload


def _snapshot(db):
    db.expire_all()
    return {
        (s.job_id, s.name): s.model_dump()
        for s in db.exec(select(JobStatisticDB)).all()
    }


@pytest.fixture
def accounting_state(
    test_config, enabled_cache, jobless_read_write_db, accounting_payload, get_jobs
):
    cluster = config.clusters["raisin"]
    cluster.sacct_gpu_accounting = True
    with Cache("jobs").create_entry(datetime(2023, 2, 17, tzinfo=UTC)) as entry:
        key = "raisin_2023-02-15T00:00_2023-02-16T00:00"
        entry.add_value(key + STEPS_SUFFIX, json.dumps(accounting_payload).encode())
    parse_jobs(config.clusters, SINCE, False)
    db = jobless_read_write_db
    first, second = sorted(get_jobs(), key=lambda job: job.job_id)
    statistic = (
        db.exec(select(JobStatisticDB))
        .first()
        .model_dump(exclude={"id", "job_id", "name"})
    )
    # Actual same-ID collisions, both across clusters and across submissions.
    other_cluster = SlurmClusterDB.by_name(db, "patate")
    for changes in (
        {"cluster_id": other_cluster.id},
        {"submit_time": first.submit_time - timedelta(seconds=100)},
    ):
        collision = SlurmJobDB(**(first.model_dump(exclude={"id"}) | changes))
        db.add(collision)
        db.flush()
        db.add(
            JobStatisticDB(
                job_id=collision.id, name=MEMORY, **(statistic | {"max": 999})
            )
        )
    for name in ("gpu_utilization", "sacct_gpu_future_statistic"):
        db.add(
            JobStatisticDB(
                job_id=first.id, name=name, **(statistic | {"max": 0.7, "mean": 0.4})
            )
        )
    db.commit()
    return db, cluster, accounting_payload, first, second


@pytest.mark.parametrize("value", [75, 0, None, "missing"])
def test_refresh_replaces_only_matched_peaks(accounting_state, value):
    db, cluster, payload, first, _ = accounting_state
    # A surviving owned row must lose old distribution fields and retain its ID.
    owned = db.exec(
        select(JobStatisticDB).where(
            JobStatisticDB.job_id == first.id, JobStatisticDB.name == MEMORY
        )
    ).one()
    for name in ("mean", "std", "q05", "q25", "median", "q75"):
        setattr(owned, name, 0.9)
    db.add(owned)
    db.commit()
    before = _snapshot(db)
    latest = copy.deepcopy(payload)
    latest["jobs"] = latest["jobs"][:1]
    latest["jobs"][0]["steps"] = (
        []
        if value == "missing"
        else [
            {
                "tres": {
                    "requested": {
                        "max": [{"type": "gres", "name": "gpumem", "count": value}]
                    }
                }
            }
        ]
    )
    raw = json.dumps(latest).encode()
    gpu_accounting.parse_gpu_accounting(db, raw, first.cluster_id, cluster)
    after = _snapshot(db)
    for key, previous in before.items():
        if key[0] != first.id or key[1] not in (MEMORY, SM):
            assert after[key] == previous
    assert (first.id, SM) not in after
    if value is None or value == "missing":
        assert (first.id, MEMORY) not in after
    else:
        current = after[(first.id, MEMORY)]
        assert (current["max"], current["id"]) == (value, owned.id)
        assert all(
            current[name] is None
            for name in ("mean", "std", "q05", "q25", "median", "q75")
        )
    gpu_accounting.parse_gpu_accounting(db, raw, first.cluster_id, cluster)
    assert _snapshot(db) == after


@pytest.mark.parametrize("rejected", ["submission", "running", "no_end", "no_gpu"])
def test_rejected_refresh_is_unchanged(accounting_state, rejected):
    db, cluster, payload, first, _ = accounting_state
    latest = copy.deepcopy(payload)
    latest["jobs"] = latest["jobs"][:1]
    latest["jobs"][0]["steps"] = []
    if rejected == "submission":
        latest["jobs"][0]["time"]["submission"] -= 200
    else:
        field, value = {
            "running": ("job_state", "RUNNING"),
            "no_end": ("end_time", None),
            "no_gpu": ("allocated_gres_gpu", 0),
        }[rejected]
        setattr(first, field, value)
        db.add(first)
        db.commit()
    before = _snapshot(db)
    gpu_accounting.parse_gpu_accounting(
        db, json.dumps(latest).encode(), first.cluster_id, cluster
    )
    assert _snapshot(db) == before


def test_malformed_later_batch_rolls_back(accounting_state, monkeypatch):
    db, cluster, payload, first, _ = accounting_state
    before = _snapshot(db)
    original_name = first.name
    payload["jobs"][0]["name"] = "uncommitted allocation edit"
    payload["jobs"][0]["steps"] = []
    payload["jobs"][1]["steps"] = [None]
    monkeypatch.setattr(gpu_accounting, "BATCH_SIZE", 1)
    with Cache("jobs").create_entry(datetime(2023, 2, 18, tzinfo=UTC)) as entry:
        entry.add_value(
            "raisin_2023-02-15T00:00_2023-02-16T00:00" + STEPS_SUFFIX,
            json.dumps(payload).encode(),
        )
    with pytest.raises(AttributeError):
        parse_jobs(config.clusters, SINCE, False)
    assert _snapshot(db) == before
    assert db.get(SlurmJobDB, first.id).name == original_name


@pytest.mark.parametrize("enabled", [True, False])
def test_fetch_cache_parse_and_dcgm_independence(
    enabled,
    test_config,
    enabled_cache,
    jobless_read_write_db,
    accounting_payload,
    monkeypatch,
):
    db = jobless_read_write_db
    cluster = config.clusters["raisin"]
    cluster.sacct_gpu_accounting = enabled
    cluster.host = "localhost"
    response = b"notice\n" + json.dumps(accounting_payload).encode()
    calls = []

    def run(command, **kwargs):
        assert kwargs["check"] == enabled
        assert (" -X " in command) != enabled
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, response.decode())

    monkeypatch.setattr("sarc.scraping.jobs_utils.subprocess.run", run)
    fetch_jobs(["raisin"], config.clusters, INTERVAL, None)
    entries = list(Cache("jobs").read_from(SINCE))
    values = list(entries[0].items())
    assert len(calls) == len(values) == 1
    assert values[0][0].endswith(STEPS_SUFFIX) == enabled
    assert values[0][1] == response
    parse_jobs(config.clusters, SINCE, False)
    stats = _snapshot(db)
    assert {name for _, name in stats} == ({MEMORY, SM} if enabled else set())
    assert all(
        s["max"] == (10924064768 if s["name"] == MEMORY else 100)
        for s in stats.values()
    )
    assert all(
        s[name] is None
        for s in stats.values()
        for name in ("mean", "std", "q05", "q25", "median", "q75")
    )
    parse_jobs(config.clusters, SINCE, False)
    assert _snapshot(db) == stats
    if enabled:
        selected = []

        def series(jobs, metric):
            selected.extend(job.job_id for job in jobs)
            return [[] for _ in jobs]

        monkeypatch.setattr("sarc.scraping.series.get_job_time_series_batched", series)
        fetch_prometheus(db, cluster, None, None)
        assert sorted(selected) == [1, 2]

        def fail(*args, **kwargs):
            raise subprocess.CalledProcessError(1, args[0])

        monkeypatch.setattr("sarc.scraping.jobs_utils.subprocess.run", fail)
        fetch_jobs(["raisin"], config.clusters, INTERVAL, None)
        parse_jobs(config.clusters, SINCE, False)
        assert _snapshot(db) == stats


@pytest.mark.parametrize("enabled,marked", [(True, False), (False, True)])
def test_non_accounting_replay_preserves_peaks(accounting_state, enabled, marked):
    db, cluster, payload, _, _ = accounting_state
    before = _snapshot(db)
    cluster.sacct_gpu_accounting = enabled
    for job in payload["jobs"]:
        job["steps"] = []
    with Cache("jobs").create_entry(datetime(2023, 2, 18, tzinfo=UTC)) as entry:
        key = "raisin_2023-02-15T00:00_2023-02-16T00:00" + (
            STEPS_SUFFIX if marked else ""
        )
        entry.add_value(key, json.dumps(payload).encode())
    parse_jobs(config.clusters, SINCE, False)
    assert _snapshot(db) == before
