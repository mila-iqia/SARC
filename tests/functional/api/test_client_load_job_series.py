from __future__ import annotations

from types import SimpleNamespace

import pytest

from sarc.client.api import SarcApiClient
from sarc.client.api import get_jobs as client_get_jobs
from sarc.client.api import get_users as client_get_users
from sarc.client.api import load_job_series as client_load_job_series
from tests.functional.jobs.test_func_load_job_series import BaseTestLoadJobSeries


class TestRestLoadJobSeries(BaseTestLoadJobSeries):
    """
    Runs the standard load_job_series test suite against the REST API implementation.
    """

    @pytest.fixture
    def ops(self, client, monkeypatch):
        """
        Provides REST API implementations of the operations, with SarcApiClient patched
        to use the test client session.
        """

        # Patch SarcApiClient to use our test session (connected to the app/DB fixtures)
        def client_factory(*args, **kwargs):
            return SarcApiClient(remote_url="http://testserver", session=client)

        # We must patch sarc.client.api.SarcApiClient because get_jobs/get_users instantiate it directly.
        monkeypatch.setattr("sarc.client.api.SarcApiClient", client_factory)

        return SimpleNamespace(
            load_job_series=client_load_job_series,
            get_jobs=client_get_jobs,
            get_users=client_get_users,
        )
