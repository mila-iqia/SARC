import pytest

from tests.functional.jobs.test_func_load_job_series import (
    BaseTestLoadJobSeries,
    SeriesOps,
)


class TestRestLoadJobSeries(BaseTestLoadJobSeries):
    """
    Runs the standard load_job_series test suite against the REST API implementation.
    """

    client_only = True

    @pytest.fixture
    def ops(self, mock_client_class) -> SeriesOps:
        """Provides REST API implementations of the operations."""
        from sarc.rest.client import (
            get_jobs as client_get_jobs,
            get_users as client_get_users,
            load_job_series as client_load_job_series,
        )

        return SeriesOps(
            load_job_series=client_load_job_series,
            get_jobs=client_get_jobs,
            get_users=client_get_users,
        )
