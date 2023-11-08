import itertools
import math
import time
from datetime import datetime, timedelta

import pandas
import pytest
from pandas import DataFrame

from sarc.jobs.series import compute_job_statistics_from_dataframe


def _generate_df(rows, delta=30):
    t0 = datetime(2023, 1, 1)
    df = DataFrame(
        {"timestamp": t0 + timedelta(seconds=i * delta), **row}
        for i, row in enumerate(rows)
    )
    return df.set_index("timestamp")


def test_compute_job_statistics_from_dataframe():
    rows = [{"instance": "cn-c002", "value": i} for i in range(100)]
    df = _generate_df(rows)
    stats = compute_job_statistics_from_dataframe(
        df,
        {"mean": DataFrame.mean},
    )
    assert stats == {"mean": 99 / 2, "unused": 0}


def test_compute_job_statistics_from_dataframe_normalization():
    rows = [{"instance": "cn-c002", "value": i} for i in range(100)]
    df = _generate_df(rows)
    stats = compute_job_statistics_from_dataframe(
        df,
        {"mean": DataFrame.mean},
        normalization=lambda x: x * 10,
    )
    assert stats == {"mean": 10 * 99 / 2, "unused": 0}


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
        df,
        {"mean": DataFrame.mean},
        is_time_counter=True,
    )
    assert stats == {"mean": (75 / delta + 15 / delta) / 2, "unused": 0}


@pytest.mark.parametrize(
    ["nprops", "threshold"], [[1, 0.2], [2, 0.2], [2, 2.0], [2, None]]
)
def test_compute_job_statistics_from_dataframe_unused_count(nprops, threshold):
    values = [0.5, 0.1, 1.0]
    assert nprops in (1, 2)
    if nprops == 2:
        # Two properties differentiate the series: instance and core
        rows1 = [
            {"instance": "cn-c002", "core": 1, "value": values[0]} for _ in range(10)
        ]
        rows2 = [
            {"instance": "cn-c007", "core": 1, "value": values[1]} for _ in range(10)
        ]
        rows3 = [
            {"instance": "cn-c007", "core": 2, "value": values[2]} for _ in range(10)
        ]
    else:
        # One property differentiates the series: instance
        rows1 = [{"instance": "cn-c002", "value": values[0]} for _ in range(10)]
        rows2 = [{"instance": "cn-c007", "value": values[1]} for _ in range(10)]
        rows3 = [{"instance": "cn-c009", "value": values[2]} for _ in range(10)]

    df1 = _generate_df(rows1)
    df2 = _generate_df(rows2)
    df3 = _generate_df(rows3)

    df = pandas.concat([df1, df2, df3])
    df = df.sort_values(by="timestamp")

    stats = compute_job_statistics_from_dataframe(
        df,
        {"mean": DataFrame.mean},
        unused_threshold=threshold,
    )
    valid = [v for v in values if threshold is None or v >= threshold]
    nvalid = len(valid)
    nunused = len(values) - len(valid)
    if nvalid:
        assert stats == {"mean": sum(valid) / nvalid, "unused": nunused}
    else:
        assert math.isnan(stats["mean"]) and stats["unused"] == nunused

@pytest.mark.parametrize(["nprops", "threshold"], [[1, 0.0], [2, 0.2], [2, 2.0]])
@pytest.mark.timeout(2)
def test_compute_job_statistics_from_dataframe_unused_count_timing(nprops, threshold):
    values = [0.5, 0.1, 1.0]
    assert nprops in (1, 2)

    n_data_points = 1000
    n_instances = 10
    n_cores = 10
    rows = []
    if nprops == 2:
        n_cores = 10
    else:
        n_cores = 1

    timestamps = pandas.date_range(
        datetime(2023, 1, 1), periods=n_data_points, freq="30s"
    )

    for node, core, value in itertools.product(
        range(n_instances), range(n_cores), range(n_data_points)
    ):
        value_index = (node * core + node) % len(values)
        rows.append(
            {
                "instance": "cn-c{node:03}",
                "core": core,
                "value": values[value_index],
                "timestamp": timestamps[value],
            }
        )

    df = pandas.DataFrame(rows)
    df = df.sort_values(by="timestamp")

    start = time.perf_counter()
    compute_job_statistics_from_dataframe(
        df,
        {"mean": DataFrame.mean},
        unused_threshold=threshold,
    )
    end = time.perf_counter()
    print(timedelta(seconds=end - start))
