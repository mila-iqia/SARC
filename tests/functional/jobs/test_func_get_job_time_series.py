import itertools
from datetime import datetime, timedelta

import pytest

from sarc.config import MTL, UTC
from sarc.jobs import SlurmJob
from sarc.jobs.series import get_job_time_series

from .factory import JobFactory

mtl_test_time = datetime(2023, 3, 5, 6, 0, tzinfo=MTL)
utc_test_time = mtl_test_time.astimezone(UTC)

test_time_str = utc_test_time.strftime("%Y-%m-%dT%H:%M %Z")


@pytest.mark.usefixtures("empty_read_write_db", "empty_read_write_prometheus")
@pytest.fixture
def job(request):
    params = getattr(request, "param", {})

    print(utc_test_time)
    print(test_time_str)
    params.setdefault("submit_time", utc_test_time - timedelta(hours=3, minutes=30))
    params.setdefault("start_time", params["submit_time"] + timedelta(minutes=30))
    # TODO: Add a JobTimeSeriesFactory when we have a prometheus setup where we can send fake data.
    # job_factory = JobTimeSeriesFactory()
    job_factory = JobFactory()
    job_factory.add_job(**params)
    # Will need a save method to push data to prometheus
    # job_factory.save()
    job = SlurmJob(**job_factory.jobs[0])
    # pytest.set_trace()

    return job


@pytest.mark.skip("Need to mock prometheus")
def test_no_job_with_this_id(job):
    assert get_job_time_series(job, "slurm_job_core_usage", dataframe=True) is None


def test_non_existing_metric(job):
    with pytest.raises(ValueError, match="Unknown metric name: non_existing_metric"):
        get_job_time_series(job, "non_existing_metric", dataframe=True)


parameters = {
    "job_id": {"job_id": 10},
    "no_end_time": {
        "job_state": "RUNNING",
        "end_time": None,
    },
    "end_time_in_past": {
        "job_state": "COMPLETED",
        "end_time": utc_test_time - timedelta(hours=1),
    },
    "end_time_in_future": {  # This will lead to offset < 0
        "job_state": "COMPLETED",
        "end_time": utc_test_time + timedelta(hours=1),
    },
}


@pytest.mark.freeze_time(test_time_str)
@pytest.mark.parametrize(
    "job", parameters.values(), ids=parameters.keys(), indirect=True
)
def test_get_job_time_series(job, custom_query_mock, file_regression):
    assert (
        get_job_time_series(job, "slurm_job_core_usage", dataframe=False) == []
    ), "custom_query was not mocked properly"

    file_regression.check(custom_query_mock.call_args[0][0])


no_duration_parameters = {
    "end_time_before_start_time": {
        "start_time": datetime(2023, 3, 4, tzinfo=MTL).astimezone(UTC),
        "end_time": datetime(2023, 3, 1, tzinfo=MTL).astimezone(UTC),
    },
    # is it possible? Otherwise look for jobs that did start and should hae duration == 0
    "not_started": {
        "job_state": "PENDING",
        "start_time": utc_test_time - timedelta(hours=3, minutes=30),
        "submit_time": utc_test_time - timedelta(hours=3, minutes=30),
        "end_time": None,
        "elapsed_time": 0,
    },
}


@pytest.mark.freeze_time(test_time_str)
@pytest.mark.parametrize(
    "job",
    no_duration_parameters.values(),
    ids=no_duration_parameters.keys(),
    indirect=True,
)
def test_jobs_with_no_duration(job):
    assert get_job_time_series(job, "slurm_job_core_usage", dataframe=False) == []


@pytest.mark.freeze_time(test_time_str)
@pytest.mark.parametrize(
    "measure,aggregation",
    itertools.product(
        ["avg_over_time", "quantile_over_time", None], ["total", "interval"]
    ),
)
def test_measure_and_aggregation(
    job, measure, aggregation, custom_query_mock, file_regression
):
    assert not get_job_time_series(
        job,
        metric="slurm_job_fp16_gpu",
        measure=measure,
        aggregation=aggregation,
        dataframe=True,
    ), "custom_query was not mocked properly"

    file_regression.check(custom_query_mock.call_args[0][0])


def test_invalid_aggregation(job):
    with pytest.raises(
        ValueError,
        match="Aggregation must be one of \['total', 'interval', None\]: invalid",
    ):
        get_job_time_series(
            job, metric="slurm_job_fp16_gpu", aggregation="invalid", dataframe=True
        )


@pytest.mark.freeze_time(test_time_str)
@pytest.mark.parametrize(
    "min_interval,max_points",
    [
        (0, 1),  # min_interval smaller than duration / max_points
        (1080.0, 20),  # Should give 10 points
        (10, 20),  # Should give 20 points
    ],
)
def test_intervals(job, min_interval, max_points, custom_query_mock, file_regression):
    assert not get_job_time_series(
        job,
        "slurm_job_fp16_gpu",
        dataframe=True,
        measure="avg_over_time",
        aggregation="interval",
        min_interval=min_interval,
        max_points=max_points,
    ), "custom_query was not mocked properly"

    file_regression.check(custom_query_mock.call_args[0][0])

    # TODO: Test when prometheus is mocked
    # assert isinstance(df, pd.DataFrame)
    # assert df["time"].diff().min() >= min_interval
    # assert df.shape[0] <= max_points


@pytest.mark.parametrize(
    "job",
    [{"job_id": -1}, {}],
    ids=["no_results", "with_results"],
    indirect=True,
)
@pytest.mark.parametrize("dataframe", [True, False])
def test_to_be_or_not_to_be_a_dataframe(job, custom_query_mock, dataframe):
    rval = get_job_time_series(
        job,
        metric="slurm_job_fp16_gpu",
        dataframe=dataframe,
    )

    if dataframe:
        assert rval is None
    else:
        assert rval == []


@pytest.mark.skip("TODO: Test when prometheus is mocked")
def test_preempted_and_resumed():
    job_factory = JobFactory()
    preempted_job = job_factory.add_job(job_id=1, status="PREEMPTED")
    resumed_job = job_factory.add_job(job_id=1, status="COMPLETED")
    job_factory.save()

    assert preempted_job.end_time < resumed_job.start_time
    assert preempted_job.series["time"].max() < resumed_job.series["time"].min()
    assert preempted_job.job_id == resumed_job.job_id
