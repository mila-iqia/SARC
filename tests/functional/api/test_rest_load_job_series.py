from typing import Callable

import pytest

from tests.functional.job_series.base import BaseTestLoadJobSeries


class TestRestLoadJobSeries(BaseTestLoadJobSeries):
    """
    Runs the standard load_job_series test suite against the REST API implementation.
    """

    client_only = True

    @pytest.fixture
    def fn_load_job_series(self, mock_client_class) -> Callable:
        """Provides REST API implementations for looad_job_series."""
        # todo
        pass
