import httpx
import pytest

from sarc.api.v0 import _EXTRA_FIELDS, _SERIES_OPTIONAL_COLS
from tests.common.dateutils import _iso_mtl_dt


@pytest.mark.usefixtures("read_only_db")
def test_get_jobs_no_filters(sarc_client):
    jobs = list(sarc_client.get_jobs())
    assert len(jobs) == 22


@pytest.mark.usefixtures("read_only_db")
def test_get_jobs_by_cluster(sarc_client):
    jobs = list(sarc_client.get_jobs(cluster_name="raisin"))
    assert len(jobs) == 19
    assert all(
        j.cluster_name == "raisin" for j in jobs
    )  # auto-populated when filtering by cluster


@pytest.mark.usefixtures("read_only_db")
def test_get_jobs_by_cluster_user(sarc_client):
    jobs = list(sarc_client.get_jobs(cluster_user="beaubonhomme"))
    assert len(jobs) == 1
    assert jobs[0].cluster_user == "beaubonhomme"


@pytest.mark.usefixtures("read_only_db")
def test_get_jobs_by_email(sarc_client):
    jobs = list(sarc_client.get_jobs(email="petitbonhomme@mila.quebec"))
    assert len(jobs) == 21


@pytest.mark.usefixtures("read_only_db")
def test_get_jobs_by_sarc_user_id(sarc_client):
    jobs = list(sarc_client.get_jobs(sarc_user_id=9))
    assert len(jobs) == 21


@pytest.mark.usefixtures("read_only_db")
def test_get_jobs_by_job_id(sarc_client):
    jobs = list(sarc_client.get_jobs(job_id=[10]))
    assert len(jobs) == 1
    assert jobs[0].job_id == 10


@pytest.mark.usefixtures("read_only_db")
def test_get_jobs_by_multiple_job_ids(sarc_client):
    jobs = list(sarc_client.get_jobs(job_id=[10, 15]))
    assert len(jobs) == 2


@pytest.mark.usefixtures("read_only_db")
def test_get_jobs_empty_job_id_list(sarc_client):
    jobs = list(sarc_client.get_jobs(job_id=[]))
    assert len(jobs) == 0


@pytest.mark.usefixtures("read_only_db")
def test_get_jobs_by_state(sarc_client):
    jobs = list(sarc_client.get_jobs(job_state="COMPLETED"))
    assert len(jobs) == 1
    assert jobs[0].job_state == "COMPLETED"


@pytest.mark.usefixtures("read_only_db")
def test_get_jobs_with_datetime_filters(sarc_client):
    jobs = list(
        sarc_client.get_jobs(
            start=_iso_mtl_dt("2023-01-01T00:00:00"),
            end=_iso_mtl_dt("2023-12-31T23:59:59"),
        )
    )
    assert len(jobs) > 0


@pytest.mark.usefixtures("read_only_db")
def test_get_jobs_invalid_cluster(sarc_client):
    with pytest.raises(httpx.HTTPStatusError) as exc:
        list(sarc_client.get_jobs(cluster_name="invalid_cluster"))
    assert exc.value.response.status_code == 404


@pytest.mark.usefixtures("read_only_db")
def test_get_jobs_pagination(sarc_client):
    """Verify pagination works: block_size=5 should still return all 22 jobs."""
    sarc_client.block_size = 5
    jobs = list(sarc_client.get_jobs())
    assert len(jobs) == 22


@pytest.mark.usefixtures("read_only_db")
def test_get_jobs_extra_fields(sarc_client):
    jobs = list(
        sarc_client.get_jobs(
            cluster_user="beaubonhomme",
            extra_fields=["cluster_name", "sarc_user", "statistics"],
        )
    )
    assert len(jobs) == 1
    assert jobs[0].cluster_name == "raisin"
    assert jobs[0].sarc_user is not None
    assert jobs[0].sarc_user.email == "beaubonhomme@mila.quebec"
    assert isinstance(jobs[0].statistics, dict)


@pytest.mark.usefixtures("read_only_db")
def test_count_jobs_no_filters(sarc_client):
    assert sarc_client.count_jobs() == 22


@pytest.mark.usefixtures("read_only_db")
def test_count_jobs_by_cluster(sarc_client):
    assert sarc_client.count_jobs(cluster_name="raisin") == 19


@pytest.mark.usefixtures("read_only_db")
def test_count_jobs_by_cluster_user(sarc_client):
    assert sarc_client.count_jobs(cluster_user="petitbonhomme") == 21


@pytest.mark.usefixtures("read_only_db")
def test_get_job_by_id(sarc_client):
    job = sarc_client.get_job(1)
    assert job.id == 1


@pytest.mark.usefixtures("read_only_db")
def test_get_job_by_id_not_found(sarc_client):
    with pytest.raises(httpx.HTTPStatusError) as exc:
        sarc_client.get_job(999_999)
    assert exc.value.response.status_code == 404


@pytest.mark.usefixtures("read_only_db")
def test_get_job_with_extra_fields(sarc_client):
    job = sarc_client.get_job(1, extra_fields=["cluster_name", "sarc_user"])
    assert job.cluster_name is not None
    assert job.sarc_user is not None


@pytest.mark.usefixtures("read_only_db")
def test_job_extra_fields_default(sarc_client):
    """job_extra_fields on the client are applied to every get_jobs/get_job call."""
    sarc_client.job_extra_fields = ["cluster_name", "sarc_user"]
    (job,) = list(sarc_client.get_jobs(cluster_user="beaubonhomme"))
    assert job.cluster_name == "raisin"
    assert job.sarc_user is not None

    job2 = sarc_client.get_job(job.id)
    assert job2.cluster_name == "raisin"
    assert job2.sarc_user is not None


@pytest.mark.usefixtures("read_only_db")
def test_job_extra_fields_merged(sarc_client):
    """Per-call extra_fields are merged with the instance default."""
    sarc_client.job_extra_fields = ["cluster_name"]
    (job,) = list(
        sarc_client.get_jobs(cluster_user="beaubonhomme", extra_fields=["sarc_user"])
    )
    assert job.cluster_name == "raisin"
    assert job.sarc_user is not None


@pytest.mark.usefixtures("read_only_db")
def test_job_series_no_extra_fields_by_default(sarc_client):
    jobs = list(sarc_client.get_job_series())
    assert len(jobs) > 0
    for job in jobs:
        for col in _SERIES_OPTIONAL_COLS:
            assert getattr(job, col) is None, f"{col} should be None by default"


@pytest.mark.usefixtures("read_only_db")
@pytest.mark.parametrize("extra_field", sorted(_EXTRA_FIELDS.keys()))
def test_job_series_extra_field(sarc_client, extra_field):
    jobs = list(sarc_client.get_job_series(extra_fields=[extra_field]))
    assert len(jobs) > 0

    sibling_cols = _SERIES_OPTIONAL_COLS - _EXTRA_FIELDS[extra_field]
    for job in jobs:
        for col in sibling_cols:
            assert getattr(job, col) is None, f"{col} should stay None"

    # There's no data in statistics and supervisors fields
    if extra_field not in ["statistics", "supervisors"]:
        assert any(
            getattr(job, col) for job in jobs for col in _EXTRA_FIELDS[extra_field]
        ), (
            f"At least on field from {extra_field}:{_EXTRA_FIELDS[extra_field]} should not be None"
        )


@pytest.mark.usefixtures("read_only_db")
def test_get_users_no_filters(sarc_client):
    users = list(sarc_client.get_users())
    assert len(users) == 11


@pytest.mark.usefixtures("read_only_db")
def test_get_users_by_display_name(sarc_client):
    users = list(sarc_client.get_users(display_name="janE"))
    assert len(users) == 1
    assert users[0].display_name == "Jane Doe"


@pytest.mark.usefixtures("read_only_db")
def test_get_users_by_email(sarc_client):
    users = list(sarc_client.get_users(email="bonhomme@mila.quebec"))
    assert len(users) == 1
    assert users[0].email == "bonhomme@mila.quebec"


@pytest.mark.usefixtures("read_only_db")
def test_get_users_empty_result(sarc_client):
    users = list(sarc_client.get_users(email="nobody@nowhere.example"))
    assert len(users) == 0


@pytest.mark.usefixtures("read_only_db")
def test_get_users_pagination(sarc_client):
    """Verify pagination: block_size=3 should still return all users."""
    sarc_client.block_size = 3
    users = list(sarc_client.get_users())
    assert len(users) == 11


@pytest.mark.usefixtures("read_only_db")
def test_get_user_by_id(sarc_client):
    user = sarc_client.get_user_by_id(1)
    assert user.id == 1
    assert user.email == "jdoe@example.com"


@pytest.mark.usefixtures("read_only_db")
def test_get_user_by_id_not_found(sarc_client):
    with pytest.raises(httpx.HTTPStatusError) as exc:
        sarc_client.get_user_by_id(999_999)
    assert exc.value.response.status_code == 404


@pytest.mark.usefixtures("read_only_db")
def test_get_user_by_email(sarc_client):
    user = sarc_client.get_user_by_email("jsmith@example.com")
    assert user.email == "jsmith@example.com"
    assert user.display_name == "John Smith"


@pytest.mark.usefixtures("read_only_db")
def test_get_user_by_email_not_found(sarc_client):
    with pytest.raises(httpx.HTTPStatusError) as exc:
        sarc_client.get_user_by_email("nobody@nowhere.example")
    assert exc.value.response.status_code == 404


@pytest.mark.usefixtures("read_only_db")
def test_get_clusters(sarc_client):
    clusters = sarc_client.get_clusters()
    names = {c.name for c in clusters}
    assert {"raisin", "fromage", "patate"}.issubset(names)


@pytest.mark.usefixtures("read_only_db")
def test_get_rgus(sarc_client):
    rgus = sarc_client.get_rgus()
    (rgu_a100,) = [rgu for rgu in rgus if rgu.name == "A100-SXM4-40GB"]
    assert rgu_a100.rgu == 4.0
    assert rgu_a100.drac_rgu == 4.0
