import pandas
import pytest

from sarc.client.job import get_jobs
from sarc.client.series import compute_cost_and_waste, load_job_series

from .test_func_job_statistics import generate_fake_timeseries
from .test_func_load_job_series import MOCK_TIME

# columns used to compute cost and waste
USED_FIELDS = [
    "elapsed_time",
    "requested.cpu",
    "requested.gres_gpu",
    "allocated.cpu",
    "allocated.gres_gpu",
    "cpu_utilization",
    "gpu_utilization",
]
# computed columns
COST_WASTE_FIELDS = [
    "cpu_cost",
    "cpu_waste",
    "cpu_equivalent_cost",
    "cpu_equivalent_waste",
    "cpu_overbilling_cost",
    "gpu_cost",
    "gpu_waste",
    "gpu_equivalent_cost",
    "gpu_equivalent_waste",
    "gpu_overbilling_cost",
]


def _df_to_pretty_str(df: pandas.DataFrame) -> str:
    return df[USED_FIELDS + COST_WASTE_FIELDS].to_markdown()


@pytest.mark.freeze_time(MOCK_TIME)
@pytest.mark.usefixtures("read_only_db", "client_mode", "tzlocal_is_mtl")
def test_compute_cost_and_waste(file_regression):
    frame = load_job_series()
    assert all(column not in frame.columns for column in COST_WASTE_FIELDS)
    assert compute_cost_and_waste(frame) is frame
    assert all(column in frame.columns for column in COST_WASTE_FIELDS)

    # Jobs do not have statistics, so, (gpu/cpu)_utilization is nan, and waste is nan too.
    assert frame["cpu_utilization"].isnull().all()
    assert frame["cpu_waste"].isnull().all()
    assert frame["cpu_equivalent_waste"].isnull().all()
    assert frame["gpu_utilization"].isnull().all()
    assert frame["gpu_waste"].isnull().all()
    assert frame["gpu_equivalent_waste"].isnull().all()

    file_regression.check(
        f"Compute cost and waste for {frame.shape[0]} job(s):\n\n{_df_to_pretty_str(frame)}"
    )


@pytest.mark.usefixtures("read_write_db", "tzlocal_is_mtl")
def test_compute_cost_and_waste_with_stored_statistics(file_regression, monkeypatch):
    # List of job indices with no stored statistics initially,
    # then with stored statistic after call to job.statistics().
    job_indices = [
        1,
        2,
        3,
        4,
        5,
        6,
        9,
        10,
        11,
        12,
        13,
        14,
        15,
        16,
        17,
        18,
        19,
        20,
        23,
        1000000,
        999_999_999,
    ]
    params = {"job_id": job_indices}

    jobs = list(get_jobs(**params))

    # Utilization fields are nan, so waste fields are nan too.
    frame = load_job_series(**params)
    assert frame["cpu_utilization"].isnull().all()
    assert frame["gpu_utilization"].isnull().all()
    frame = compute_cost_and_waste(frame)
    assert frame["cpu_waste"].isnull().all()
    assert frame["cpu_equivalent_waste"].isnull().all()
    assert frame["gpu_waste"].isnull().all()
    assert frame["gpu_equivalent_waste"].isnull().all()

    # Save job statistics.
    monkeypatch.setattr(
        "sarc.jobs.series.get_job_time_series", generate_fake_timeseries
    )
    for job in jobs:
        assert not job.stored_statistics
        job.statistics(save=True)
        assert job.stored_statistics

    # With statistics computed, utilization fields are not nan, so waste fields are not nan neither.
    frame = load_job_series(**params)
    assert frame["cpu_utilization"].notnull().all()
    assert frame["gpu_utilization"].notnull().all()
    frame = compute_cost_and_waste(frame)
    assert frame["cpu_waste"].notnull().all()
    assert frame["cpu_equivalent_waste"].notnull().all()
    assert frame["gpu_waste"].notnull().all()
    assert frame["gpu_equivalent_waste"].notnull().all()

    file_regression.check(
        f"Compute cost and waste for {frame.shape[0]} job(s):\n\n{_df_to_pretty_str(frame)}"
    )
