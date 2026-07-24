from datetime import datetime, timedelta

import pandas
import pytest
from pandas import DataFrame

from sarc.scraping.dcgm import (
    DCGM_FP64_BLANK,
    DCGM_FP64_NOT_FOUND,
    DCGM_FP64_NOT_PERMISSIONED,
    DCGM_FP64_NOT_SUPPORTED,
)
from sarc.scraping.series import compute_job_statistics_from_dataframe


def _generate_df(rows, delta=30):
    t0 = datetime(2023, 1, 1)
    df = DataFrame(
        {"timestamp": t0 + timedelta(seconds=i * delta), **row}
        for i, row in enumerate(rows)
    )
    return df.set_index("timestamp")


def test_compute_job_statistics_from_dataframe(captrace):
    rows = [{"instance": "cn-c002", "value": i} for i in range(100)]
    df = _generate_df(rows)
    stats = compute_job_statistics_from_dataframe(
        df, {"mean": lambda self: self.mean()}
    )
    assert stats == {"mean": 99 / 2}

    # Check trace
    spans = captrace.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].name == "compute_job_statistics_from_dataframe"


def test_compute_job_statistics_from_dataframe_normalization():
    rows = [{"instance": "cn-c002", "value": i} for i in range(100)]
    df = _generate_df(rows)
    stats = compute_job_statistics_from_dataframe(
        df, {"mean": lambda self: self.mean()}, normalization=lambda x: x * 10
    )
    assert stats == {"mean": 10 * 99 / 2}


@pytest.mark.parametrize(["delta"], [[30], [60]])
def test_compute_job_statistics_from_dataframe_time_counter(delta):
    rows1 = [{"instance": "cn-c002", "value": 75e9 * i} for i in range(100)]
    rows2 = [{"instance": "cn-c007", "value": 15e9 * i} for i in range(100)]
    df1 = _generate_df(rows1, delta=delta)
    df2 = _generate_df(rows2, delta=delta)

    # The two series are interleaved in the data, but the function will
    # have to group by instance before taking the mean
    df = pandas.concat([df1, df2])
    df = df.sort_values(by="timestamp")

    stats = compute_job_statistics_from_dataframe(
        df, {"mean": lambda self: self.mean()}, is_time_counter=True
    )
    assert stats == {"mean": (75 / delta + 15 / delta) / 2}


def test_compute_job_statistics_from_dataframe_filters_dcgm_blank():
    # Mix of valid values and DCGM sentinels (BLANK + the three error
    # variants). All sentinels must be discarded so that stats reflect only
    # the valid samples.
    sentinels = [
        DCGM_FP64_BLANK,
        DCGM_FP64_NOT_FOUND,
        DCGM_FP64_NOT_SUPPORTED,
        DCGM_FP64_NOT_PERMISSIONED,
    ]
    rows = [{"instance": "cn-c002", "value": v} for v in [1.0, 2.0, 3.0, *sentinels]]
    df = _generate_df(rows)
    stats = compute_job_statistics_from_dataframe(
        df, {"mean": lambda self: self.mean(), "max": lambda self: self.max()}
    )
    assert stats == {"mean": 2.0, "max": 3.0}


def test_compute_job_statistics_from_dataframe_all_blank_returns_none():
    rows = [{"instance": "cn-c002", "value": DCGM_FP64_BLANK} for _ in range(5)]
    df = _generate_df(rows)
    stats = compute_job_statistics_from_dataframe(
        df, {"mean": lambda self: self.mean()}
    )
    assert stats is None
