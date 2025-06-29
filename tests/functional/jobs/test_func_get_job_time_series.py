import itertools
import json
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from sarc.client.job import SlurmJob
from sarc.config import MTL, UTC
from sarc.jobs.series import _get_job_time_series_data_cache_key, get_job_time_series

from .factory import JobFactory

mtl_test_time = datetime(2023, 3, 5, 6, 0, tzinfo=MTL)
utc_test_time = mtl_test_time.astimezone(UTC)

test_time_str = utc_test_time.strftime("%Y-%m-%dT%H:%M %Z")


@pytest.mark.usefixtures("empty_read_write_db", "empty_read_write_prometheus")
@pytest.fixture
def job(request):
    params = getattr(request, "param", {})

    params.setdefault("submit_time", utc_test_time - timedelta(hours=3, minutes=30))
    params.setdefault("start_time", params["submit_time"] + timedelta(minutes=30))
    params.setdefault("end_time", utc_test_time)
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


@pytest.mark.usefixtures("base_config")
def test_non_existing_metric(job):
    with pytest.raises(ValueError, match="^Unknown metric name: non_existing_metric"):
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
def test_get_job_time_series(job, prom_custom_query_mock, file_regression):
    assert get_job_time_series(job, "slurm_job_core_usage", dataframe=False) == [], (
        "custom_query was not mocked properly"
    )

    file_regression.check(prom_custom_query_mock.call_args[0][0])


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
@pytest.mark.usefixtures("base_config")
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
    job, measure, aggregation, prom_custom_query_mock, file_regression
):
    assert not get_job_time_series(
        job,
        metric="slurm_job_fp16_gpu",
        measure=measure,
        aggregation=aggregation,
        dataframe=True,
    ), "custom_query was not mocked properly"

    file_regression.check(prom_custom_query_mock.call_args[0][0])


@pytest.mark.usefixtures("base_config")
def test_invalid_aggregation(job):
    with pytest.raises(
        ValueError,
        match="^Aggregation must be one of ",
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
def test_intervals(
    job, min_interval, max_points, prom_custom_query_mock, file_regression
):
    assert not get_job_time_series(
        job,
        "slurm_job_fp16_gpu",
        dataframe=True,
        measure="avg_over_time",
        aggregation="interval",
        min_interval=min_interval,
        max_points=max_points,
    ), "custom_query was not mocked properly"

    file_regression.check(prom_custom_query_mock.call_args[0][0])

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
def test_to_be_or_not_to_be_a_dataframe(job, prom_custom_query_mock, dataframe):
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


@pytest.mark.usefixtures("enabled_cache")
def test_get_job_time_series_cache(job, test_config, monkeypatch, capsys):
    """Test cache for get_job_time_series"""

    fake_series_data = [1234, 5678]

    # Mock for job time series results.
    # Print a message, so that we can check
    # if this function is called (i.e. cache was not used)
    # or not (i.e. cache was used)
    def _fake_job_time_series_data(*args, **kwargs):
        print("live_fake_series")
        return fake_series_data

    monkeypatch.setattr(
        "sarc.jobs.series._get_job_time_series_data", _fake_job_time_series_data
    )

    params = {
        "job": job,
        "metric": "slurm_job_core_usage",
    }
    key = _get_job_time_series_data_cache_key(**params)
    assert key is not None

    prometheus_cache_dir: Path = test_config.cache / "prometheus"
    cache_path = prometheus_cache_dir / key

    # Cache folder should not exist
    assert not prometheus_cache_dir.exists()

    # Call the function and check returned value
    assert get_job_time_series(dataframe=False, **params) == fake_series_data

    # Cache should have NOT been used
    captured = capsys.readouterr()
    assert captured.out.strip() == "live_fake_series"

    # Cache should now exist
    assert prometheus_cache_dir.is_dir()
    assert cache_path.is_file()

    # Cache should have expected data
    with open(cache_path) as file:
        cached_data = json.load(file)
    assert cached_data == fake_series_data

    # Call the function again
    assert get_job_time_series(dataframe=False, **params) == fake_series_data
    # Cache should have been used
    captured = capsys.readouterr()
    assert captured.out.strip() == ""


@pytest.mark.usefixtures("enabled_cache")
def test_get_job_time_series_cache_check(job, test_config, monkeypatch):
    """Test cache checking for get_job_time_series using SARC_CACHE=check"""

    # Manually reset cache_policy_var
    from sarc.cache import CacheException, cache_policy_var

    token = cache_policy_var.set(None)
    try:
        monkeypatch.setenv("SARC_CACHE", "check")

        fake_series_data_orig = [1234, 5678]
        fake_series_data = [1234, 5678]

        def _fake_job_time_series_data(*args, **kwargs):
            # Change returned data on each call
            ret = list(fake_series_data)
            fake_series_data[0] += 1
            return ret

        monkeypatch.setattr(
            "sarc.jobs.series._get_job_time_series_data", _fake_job_time_series_data
        )

        params = {
            "job": job,
            "metric": "slurm_job_core_usage",
        }

        # Call the function and check returned value
        assert get_job_time_series(dataframe=False, **params) == fake_series_data_orig

        # Call the function again
        # Should fail because newly returned data
        # will be compared to previous returned data
        # which is stored in the cache
        with pytest.raises(CacheException) as exc_info:
            get_job_time_series(dataframe=False, **params)
        assert (
            str(exc_info.value)
            == """
Cached result != live result:
Key: raisin.1.2023-03-05T03h00m00s_to_2023-03-05T06h00m00s.cu.min-itv-30s.max-pts-100.no_measure.json

--- cached
+++ value
@@ -1,4 +1,4 @@
 [
- 1234,
+ 1235,
  5678
 ]
"""
        )

    finally:
        cache_policy_var.reset(token)
