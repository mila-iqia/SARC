from datetime import datetime, timedelta

import pytest

from sarc.config import UTC
from sarc.scraping.series import get_job_time_series_batched
from tests.common.dateutils import MTL
from tests.db.factory import JobFactory

mtl_test_time = datetime(2023, 3, 5, 6, 0, tzinfo=MTL)
utc_test_time = mtl_test_time.astimezone(UTC)


@pytest.fixture
def job(request, read_write_db):
    params = getattr(request, "param", {})

    params.setdefault("submit_time", utc_test_time - timedelta(hours=3, minutes=30))
    params.setdefault("start_time", params["submit_time"] + timedelta(minutes=30))
    params.setdefault("end_time", utc_test_time)
    params.setdefault("cluster_id", 1)
    params.setdefault("sarc_user_id", 1)
    # TODO: Add a JobTimeSeriesFactory when we have a prometheus setup where we can send fake data.
    # job_factory = JobTimeSeriesFactory()
    job_factory = JobFactory(clusters=[], users=[])
    job_factory.add_job(**params)
    # Will need a save method to push data to prometheus
    # job_factory.save()
    job = job_factory.jobs[0]
    read_write_db.add(job)
    read_write_db.flush()
    # pytest.set_trace()

    return job


@pytest.mark.usefixtures("base_config")
def test_non_existing_metric(job):
    with pytest.raises(ValueError, match="^Unknown metric name: non_existing_metric"):
        get_job_time_series_batched([job], "non_existing_metric")


parameters = {
    "job_id": {"job_id": 10},
    "end_time_in_past": {
        "job_state": "COMPLETED",
        "end_time": utc_test_time - timedelta(hours=1),
    },
    "end_time_in_future": {  # This will lead to offset < 0
        "job_state": "COMPLETED",
        "end_time": utc_test_time + timedelta(hours=1),
    },
}


@pytest.mark.time_machine(utc_test_time, tick=False)
@pytest.mark.parametrize(
    "job", parameters.values(), ids=parameters.keys(), indirect=True
)
def test_get_job_time_series_data(job, prom_custom_query_mock, file_regression):
    assert get_job_time_series_batched([job], "slurm_job_core_usage") == [[]], (
        "custom_query was not mocked properly"
    )

    file_regression.check(prom_custom_query_mock.call_args[0][0])


bucket_parameters = [
    # raw_cut is max_points * min_interval = 3000s: 600 < raw_cut, then the
    # two longer jobs are one bucket each (3_000_000 > 4 * 7200).
    {"job_id": 11, "duration": 600},
    {"job_id": 12, "duration": 7200},
    {"job_id": 13, "duration": 3_000_000},
]


@pytest.mark.time_machine(utc_test_time, tick=False)
def test_duration_buckets(prom_custom_query_mock, read_write_db):
    """Mixed runtimes are fetched one query per duration bucket, each query
    subquery-sampled at its shortest job's duration / max_points."""
    job_factory = JobFactory(clusters=[], users=[])
    jobs = []
    for params in bucket_parameters:
        start_time = utc_test_time - timedelta(seconds=params["duration"])
        job = job_factory.create_job(
            job_id=params["job_id"],
            submit_time=start_time,
            start_time=start_time,
            end_time=utc_test_time,
            cluster_id=1,
            sarc_user_id=1,
        )
        read_write_db.add(job)
        jobs.append(job)
    read_write_db.flush()

    assert get_job_time_series_batched(jobs, "slurm_job_core_usage") == [[], [], []], (
        "custom_query was not mocked properly"
    )

    queries = [call.args[0] for call in prom_custom_query_mock.call_args_list]
    # The short job keeps raw resolution, the others are downsampled to
    # ceil(bucket_min_duration / max_points) steps.
    assert queries == [
        '(slurm_job_core_usage{slurmjobid=~"^11$"}[600s])',
        '(slurm_job_core_usage{slurmjobid=~"^12$"}[7200s:72s])',
        '(slurm_job_core_usage{slurmjobid=~"^13$"}[3000000s:30000s])',
    ]


span_parameters = [
    # Raw bucket (duration <= raw_cut = 3000s, no explicit step): the two
    # short jobs ran ~10 days apart, and their shared window would request
    # far more than max_query_points at the raw 30s resolution, so the
    # bucket is split in two time windows.
    {"job_id": 41, "duration": 600, "end_ago": 899_400},
    {"job_id": 42, "duration": 600, "end_ago": 0},
    # Downsampled bucket (step = ceil(4000 / 100) = 40s, window cap
    # 300 * 40 = 12_000s): jobs 32 and 33 overlap and keep one window,
    # job 31 ran long before them and gets its own.
    {"job_id": 31, "duration": 4_000, "end_ago": 200_000},
    {"job_id": 32, "duration": 5_000, "end_ago": 100_000},
    {"job_id": 33, "duration": 4_500, "end_ago": 98_500},
]


@pytest.mark.time_machine(utc_test_time, tick=False)
def test_split_overspanned_buckets(prom_custom_query_mock, read_write_db):
    """A bucket whose jobs span more than max_query_points * step is split
    into narrower time windows instead of getting a 422 from Prometheus."""
    job_factory = JobFactory(clusters=[], users=[])
    jobs = []
    for params in span_parameters:
        end_time = utc_test_time - timedelta(seconds=params["end_ago"])
        start_time = end_time - timedelta(seconds=params["duration"])
        job = job_factory.create_job(
            job_id=params["job_id"],
            submit_time=start_time,
            start_time=start_time,
            end_time=end_time,
            cluster_id=1,
            sarc_user_id=1,
        )
        read_write_db.add(job)
        jobs.append(job)
    read_write_db.flush()

    assert get_job_time_series_batched(
        jobs, "slurm_job_core_usage", max_query_points=300
    ) == [[]] * len(jobs), "custom_query was not mocked properly"

    queries = [call.args[0] for call in prom_custom_query_mock.call_args_list]
    # Each window still carries its bucket's step, only the span shrinks.
    assert queries == [
        '(slurm_job_core_usage{slurmjobid=~"^41$"}[600s] offset 899400s)',
        '(slurm_job_core_usage{slurmjobid=~"^42$"}[600s])',
        '(slurm_job_core_usage{slurmjobid=~"^31$"}[4000s:40s] offset 200000s)',
        '(slurm_job_core_usage{slurmjobid=~"^32|33$"}[6500s:40s] offset 98500s)',
    ]


no_duration_parameters = {
    "end_time_with_start_none": {
        "job_state": "CANCELLED",
        "start_time": None,
        "end_time": datetime(2023, 3, 1, tzinfo=MTL).astimezone(UTC),
        "elapsed_time": 0,
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


@pytest.mark.time_machine(utc_test_time, tick=False)
@pytest.mark.usefixtures("base_config")
@pytest.mark.parametrize(
    "job",
    no_duration_parameters.values(),
    ids=no_duration_parameters.keys(),
    indirect=True,
)
def test_jobs_with_no_duration(job):
    assert get_job_time_series_batched([job], "slurm_job_core_usage") == [[]]


@pytest.mark.skip("TODO: Test when prometheus is mocked")
def test_preempted_and_resumed():
    job_factory = JobFactory()
    preempted_job = job_factory.add_job(job_id=1, status="PREEMPTED")
    resumed_job = job_factory.add_job(job_id=1, status="COMPLETED")
    job_factory.save()

    assert preempted_job.end_time < resumed_job.start_time
    assert preempted_job.series["time"].max() < resumed_job.series["time"].min()
    assert preempted_job.job_id == resumed_job.job_id
