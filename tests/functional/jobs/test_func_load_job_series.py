import math
from datetime import datetime

import pandas
import pytest

from sarc.config import MTL
from sarc.jobs.job import get_jobs
from sarc.jobs.series import load_job_series
from sarc.ldap.api import get_users

from .test_func_job_statistics import generate_fake_timeseries

parameters = {
    "no_cluster": {},
    "cluster_str": {"cluster": "patate"},
    "job_state": {"job_state": "COMPLETED"},
    "one_job": {"job_id": 10},
    "one_job_wrong_cluster": {"job_id": 10, "cluster": "patate"},
    "many_jobs": {"job_id": [8, 9]},
    "no_jobs": {"job_id": []},
    "start_only": {"start": datetime(2023, 2, 19, tzinfo=MTL)},
    "end_only": {"end": datetime(2023, 2, 16, tzinfo=MTL)},
    "start_str_only": {"start": "2023-02-19"},
    "end_str_only": {"end": "2023-02-16"},
    "start_and_end": {
        "start": datetime(2023, 2, 15, tzinfo=MTL),
        "end": datetime(2023, 2, 18, tzinfo=MTL),
    },
    "user": {"user": "beaubonhomme"},
    "resubmitted": {"job_id": 1_000_000},
}


few_parameters = {key: parameters[key] for key in ("no_cluster", "start_and_end")}
param_start_end = {"start_and_end": parameters["start_and_end"]}
params_no_start_or_end = {
    key: parameters[key] for key in ("no_cluster", "start_only", "end_only")
}

ALL_COLUMNS = [
    "CLEAR_SCHEDULING",
    "STARTED_ON_BACKFILL",
    "STARTED_ON_SCHEDULE",
    "STARTED_ON_SUBMIT",
    "account",
    "allocated.billing",
    "allocated.cpu",
    "allocated.gpu_type",
    "allocated.gres_gpu",
    "allocated.mem",
    "allocated.node",
    "array_job_id",
    "cluster_name",
    "constraints",
    "cpu_utilization",
    "elapsed_time",
    "end_time",
    "exit_code",
    "gpu_memory",
    "gpu_power",
    "gpu_utilization",
    "group",
    "id",
    "job_id",
    "job_state",
    "name",
    "nodes",
    "partition",
    "priority",
    "qos",
    "requested.billing",
    "requested.cpu",
    "requested.gpu_type",
    "requested.gres_gpu",
    "requested.mem",
    "requested.node",
    "signal",
    "start_time",
    "stored_statistics",
    "submit_time",
    "system_memory",
    "task_id",
    "time_limit",
    "user",
    "work_dir",
]

# For testing, we still check expected columns.
# If attributes in User class change, we may need to update this list.
USER_COLUMNS = [
    "user.primary_email",
    "user.name",
    "user.record_start",
    "user.record_end",
    "user.mila.username",
    "user.mila.email",
    "user.mila.active",
    "user.drac.username",
    "user.drac.email",
    "user.drac.active",
    "user.mila_ldap.co_supervisor",
    "user.mila_ldap.display_name",
    "user.mila_ldap.mila_cluster_gid",
    "user.mila_ldap.mila_cluster_uid",
    "user.mila_ldap.mila_cluster_username",
    "user.mila_ldap.mila_email_username",
    "user.mila_ldap.status",
    "user.mila_ldap.supervisor",
    "user.drac_members.activation_status",
    "user.drac_members.email",
    "user.drac_members.name",
    "user.drac_members.permission",
    "user.drac_members.sponsor",
    "user.drac_members.username",
    "user.drac_roles.email",
    "user.drac_roles.nom",
    "user.drac_roles.status",
    "user.drac_roles.username",
    "user.drac_roles.état du compte",
]

# For file regression tests, we will save data frame into a CSV.
# We won't include job `id` because it changes from a call to another
# (note that `job_id`, on the contrary, does not change).
CSV_COLUMNS = [col for col in ALL_COLUMNS if col not in ["id"]]


MOCK_TIME = "2023-11-22"


@pytest.mark.freeze_time(MOCK_TIME)
@pytest.mark.usefixtures("read_only_db_with_users", "tzlocal_is_mtl")
def test_load_job_series_with_users(file_regression):
    assert len(get_users()) == 4
    data_frame = load_job_series()
    expected_columns = sorted(ALL_COLUMNS + USER_COLUMNS)
    assert sorted(data_frame.keys().tolist()) == expected_columns

    str_view = data_frame[
        ["job_id", "cluster_name", "user"] + USER_COLUMNS
    ].to_markdown()
    file_regression.check(
        f"Found 4 users and {data_frame.shape[0]} job(s):\n\n{str_view}"
    )


@pytest.mark.freeze_time(MOCK_TIME)
@pytest.mark.usefixtures("read_only_db", "tzlocal_is_mtl")
@pytest.mark.parametrize("params", parameters.values(), ids=parameters.keys())
def test_load_job_series(params, file_regression, captrace):
    data_frame = load_job_series(**params)
    assert isinstance(data_frame, pandas.DataFrame)
    # Check columns
    if data_frame.shape[0]:
        assert sorted(data_frame.keys().tolist()) == ALL_COLUMNS
        file_regression.check(
            f"Found {data_frame.shape[0]} job(s):\n"
            f"\n{data_frame.to_csv(columns=CSV_COLUMNS)}"
        )
    else:
        file_regression.check(
            f"Found {data_frame.shape[0]} job(s):\n\n{data_frame.to_csv()}"
        )

    # Check trace
    spans = captrace.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].name == "load_job_series"


@pytest.mark.usefixtures("read_only_db", "tzlocal_is_mtl")
@pytest.mark.parametrize("params", [parameters["no_cluster"]], ids=["no_cluster"])
def test_load_job_series_check_end_times(params):
    # Get jobs
    jobs = list(get_jobs(**params))
    # Get a data frame
    frame_1 = load_job_series(**params)
    # Get a data frame again
    frame_2 = load_job_series(**params)
    frame_1_end_times = []
    frame_2_end_times = []
    for i, job in enumerate(jobs):
        if job.end_time is None:
            frame_1_end_times.append(frame_1["end_time"][i])
            frame_2_end_times.append(frame_2["end_time"][i])
            # End time won't be None in data frames, because
            # load_job_series() will have set it to current time.
            assert frame_1["end_time"][i]
            assert frame_2["end_time"][i]
            # As frame_2 is generated after frame_1,
            # end times in frame 2 will be set to a current time more recent
            # than in frame 1.
            assert frame_2["end_time"][i] > frame_1["end_time"][i]
        else:
            # End time won't be changed.
            assert job.end_time == frame_1["end_time"][i]
            assert job.end_time == frame_2["end_time"][i]
    # Check we really got many raw jobs with no end time.
    assert len(frame_1_end_times) > 1
    assert len(frame_2_end_times) > 1
    # All missing end times set by a call to load_job_series() must have same value.
    assert len(set(frame_1_end_times)) == 1
    assert len(set(frame_2_end_times)) == 1


@pytest.mark.usefixtures("read_write_db", "tzlocal_is_mtl")
def test_load_job_series_with_stored_statistics(monkeypatch):
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
    ]
    params = {"job_id": job_indices}

    jobs = list(get_jobs(**params))
    frame = load_job_series(**params)
    assert jobs
    for job in jobs:
        assert not job.stored_statistics
    for label in [
        "gpu_utilization",
        "cpu_utilization",
        "gpu_memory",
        "gpu_power",
        "system_memory",
    ]:
        assert all(math.isnan(value) for value in frame[label])

    monkeypatch.setattr(
        "sarc.jobs.series.get_job_time_series", generate_fake_timeseries
    )

    # Save job statistics.
    for job in jobs:
        job.statistics(save=True)
        assert job.stored_statistics

    # Generate new data frame. Relevant fields must not contain nan anymore.
    re_jobs = list(get_jobs(**params))
    re_frame = load_job_series(**params)
    assert re_jobs
    for i, re_job in enumerate(re_jobs):
        stats = re_job.stored_statistics.dict()
        assert re_frame["system_memory"][i] == stats["system_memory"]["max"]
        assert re_frame["gpu_memory"][i] == stats["gpu_memory"]["max"]
        assert re_frame["gpu_utilization"][i] == stats["gpu_utilization"]["median"]
        assert re_frame["cpu_utilization"][i] == stats["cpu_utilization"]["median"]
        assert re_frame["gpu_power"][i] == stats["gpu_power"]["median"]

    for label in [
        "gpu_utilization",
        "cpu_utilization",
        "gpu_memory",
        "gpu_power",
        "system_memory",
    ]:
        assert all(not math.isnan(value) for value in re_frame[label])


@pytest.mark.usefixtures("read_only_db", "tzlocal_is_mtl")
@pytest.mark.parametrize("params", few_parameters.values(), ids=few_parameters.keys())
def test_load_job_series_fields_list(params, file_regression):
    fields = ["gpu_memory", "allocated.mem", "requested.mem", "user", "work_dir"]
    data_frame = load_job_series(fields=fields, **params)
    assert isinstance(data_frame, pandas.DataFrame)
    assert sorted(data_frame.keys().tolist()) == sorted(fields)
    file_regression.check(
        f"Found {data_frame.shape[0]} job(s):\n\n{data_frame.to_csv()}"
    )


@pytest.mark.usefixtures("read_only_db", "tzlocal_is_mtl")
@pytest.mark.parametrize("params", few_parameters.values(), ids=few_parameters.keys())
def test_load_job_series_fields_dict(params, file_regression):
    fields = {
        "gpu_memory": "gpu_footprint",
        "allocated.mem": "memory",
        "user": "username",
        "work_dir": "the_user_folder",
    }
    expected_fields = ["gpu_footprint", "memory", "username", "the_user_folder"]
    data_frame = load_job_series(fields=fields, **params)
    assert isinstance(data_frame, pandas.DataFrame)
    assert sorted(data_frame.keys().tolist()) == sorted(expected_fields)
    file_regression.check(
        f"Found {data_frame.shape[0]} job(s):\n\n{data_frame.to_csv()}"
    )


@pytest.mark.freeze_time(MOCK_TIME)
@pytest.mark.usefixtures("read_only_db", "tzlocal_is_mtl")
@pytest.mark.parametrize("params", param_start_end.values(), ids=param_start_end.keys())
def test_load_job_series_clip_time_true(params, file_regression):
    assert "start" in params
    assert "end" in params
    data_frame = load_job_series(clip_time=True, **params)
    assert isinstance(data_frame, pandas.DataFrame)
    assert sorted(data_frame.keys().tolist()) == sorted(
        ALL_COLUMNS + ["unclipped_start", "unclipped_end"]
    )
    file_regression.check(
        f"Found {data_frame.shape[0]} job(s):\n\n{data_frame.to_csv(columns=CSV_COLUMNS)}"
    )


@pytest.mark.freeze_time(MOCK_TIME)
@pytest.mark.usefixtures("read_only_db", "tzlocal_is_mtl")
@pytest.mark.parametrize("params", param_start_end.values(), ids=param_start_end.keys())
def test_load_job_series_clip_time_false(params, file_regression):
    assert "start" in params
    assert "end" in params
    data_frame = load_job_series(clip_time=False, **params)
    assert isinstance(data_frame, pandas.DataFrame)
    assert sorted(data_frame.keys().tolist()) == sorted(ALL_COLUMNS)
    file_regression.check(
        f"Found {data_frame.shape[0]} job(s):\n\n{data_frame.to_csv(columns=CSV_COLUMNS)}"
    )


@pytest.mark.usefixtures("read_only_db", "tzlocal_is_mtl")
@pytest.mark.parametrize(
    "params", params_no_start_or_end.values(), ids=params_no_start_or_end.keys()
)
def test_load_job_series_clip_time_true_no_start_or_end(params, file_regression):
    with pytest.raises(ValueError, match="Clip time\: missing (start|end)"):
        load_job_series(clip_time=True, **params)


@pytest.mark.freeze_time(MOCK_TIME)
@pytest.mark.usefixtures("read_only_db", "tzlocal_is_mtl")
@pytest.mark.parametrize("params", few_parameters.values(), ids=few_parameters.keys())
def test_load_job_series_callback(params, file_regression):
    def callback(rows):
        rows[-1]["another_column"] = 1234

    data_frame = load_job_series(callback=callback, **params)
    assert isinstance(data_frame, pandas.DataFrame)
    assert sorted(data_frame.keys().tolist()) == sorted(
        ALL_COLUMNS + ["another_column"]
    )
    assert data_frame["another_column"].sum() == 1234 * data_frame.shape[0]
    file_regression.check(
        f"Found {data_frame.shape[0]} job(s):\n"
        f"\n{data_frame.to_csv(columns=CSV_COLUMNS + ['another_column'])}"
    )


@pytest.mark.usefixtures("read_only_db", "tzlocal_is_mtl")
@pytest.mark.parametrize("params", param_start_end.values(), ids=param_start_end.keys())
def test_load_job_series_all_args(params, file_regression):
    def callback(rows):
        rows[-1]["another_column"] = 1234

    fields = {
        "gpu_memory": "gpu_footprint",
        "allocated.mem": "memory",
        "user": "username",
        "work_dir": "the_user_folder",
    }
    expected_fields = [
        "gpu_footprint",
        "memory",
        "username",
        "the_user_folder",
        "another_column",
    ]
    data_frame = load_job_series(
        fields=fields, clip_time=True, callback=callback, **params
    )
    assert isinstance(data_frame, pandas.DataFrame)
    assert sorted(data_frame.keys().tolist()) == sorted(expected_fields)
    assert data_frame["another_column"].sum() == 1234 * data_frame.shape[0]
    file_regression.check(
        f"Found {data_frame.shape[0]} job(s):\n\n{data_frame.to_csv(columns=expected_fields)}"
    )
