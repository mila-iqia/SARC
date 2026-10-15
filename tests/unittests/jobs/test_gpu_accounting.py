import json
import subprocess
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest

from sarc.config import config
from sarc.scraping import gpu_accounting, jobs_utils

MEMORY = "sacct_gpu_memory_task_peak_bytes"
SM = "sacct_gpu_sm_task_peak_percent"


def _tres(name, count):
    return {"type": "gres", "name": name, "count": count}


def test_recorded_fir_and_step_peak_semantics():
    path = Path(__file__).parents[2] / "data/sacct/fir_gpu_accounting_26_05_4.json"
    assert gpu_accounting.compute_gpu_accounting(
        json.loads(path.read_text())["jobs"][0]
    ) == {MEMORY: 10924064768, SM: 100}
    # Synthetic steps: peaks are maxima, never sums or last-sample averages.
    steps = [
        {
            "step": {"name": name},
            "tres": {"requested": {"max": [_tres("gpuutil", peak)]}},
        }
        for name, peak in (("batch", 0), ("0", 180), ("1", 190), ("extern", None))
    ]
    steps.append(
        {
            "tres": {
                "requested": {
                    field: [_tres("gpumem", 999)] for field in ("total", "average")
                }
            }
        }
    )
    assert gpu_accounting.compute_gpu_accounting({"steps": steps}) == {SM: 190}
    assert gpu_accounting.compute_gpu_accounting({}) == {}


@pytest.mark.parametrize(
    "value,expected",
    [
        (0, 0),
        (42, 42),
        (None, None),
        (-1, None),
        (-2, None),
        (2**64 - 1, None),
        (2**64 - 2, None),
        (float("nan"), None),
        (float("inf"), None),
        ("1G", None),
        (True, None),
        ({"set": False, "number": 0}, None),
        ({"infinite": True, "number": 0}, None),
        ({"set": True, "number": 0}, 0),
    ],
)
def test_peak_values(value, expected):
    raw = {"steps": [{"tres": {"requested": {"max": [_tres("gpumem", value)]}}}]}
    assert gpu_accounting.compute_gpu_accounting(raw) == (
        {} if expected is None else {MEMORY: expected}
    )
    raw["steps"][0]["tres"]["requested"]["max"] *= 2
    assert gpu_accounting.compute_gpu_accounting(raw) == {}


@pytest.mark.parametrize(
    "raw",
    [
        b'{"jobs":null}',
        b'{"errors":[{}],"jobs":[]}',
        b"not JSON",
        b"[]",
        b'{"meta":{"schema_version":"fastsacct-flat-v1"},"jobs":[]}',
        b'{"meta":{"schema_version":"fastsacct-jsonl-v1"}}',
        b'{"meta":{"schema_version":"fastsacct-jsonl-v1"}}\n{"job_id":1}',
    ],
)
def test_opt_in_rejects_unsupported_capture(raw, monkeypatch):
    cluster = replace(
        config.clusters["raisin"], host="localhost", sacct_gpu_accounting=True
    )
    monkeypatch.setattr(
        jobs_utils.subprocess,
        "run",
        lambda *_, **__: subprocess.CompletedProcess([], 0, raw.decode()),
    )
    start = datetime(2026, 10, 1, tzinfo=UTC)
    with pytest.raises(ValueError):
        jobs_utils.fetch_raw(cluster, start, start)
    cluster.sacct_gpu_accounting = False
    assert jobs_utils.fetch_raw(cluster, start, start) == raw


def test_local_scheduler_failure_propagates():
    cluster = replace(
        config.clusters["raisin"],
        host="localhost",
        sacct_bin="/usr/bin/false",
        sacct_gpu_accounting=True,
    )
    start = datetime(2026, 10, 1, tzinfo=UTC)
    with pytest.raises(subprocess.CalledProcessError):
        jobs_utils.fetch_raw(cluster, start, start)
