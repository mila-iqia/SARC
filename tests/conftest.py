import os
import sys
import zoneinfo
from pathlib import Path
from unittest.mock import MagicMock

import gifnoc
import pytest
from freezegun.api import FakeDatetime
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import set_tracer_provider
from pytest_regressions.data_regression import RegressionYamlDumper

from sarc.config import config, using_sarc_mode

_tracer_provider = TracerProvider()
_exporter = InMemorySpanExporter()
_tracer_provider.add_span_processor(SimpleSpanProcessor(_exporter))
set_tracer_provider(_tracer_provider)
del _tracer_provider

sys.path.append(os.path.join(os.path.dirname(__file__), "common"))

pytest_plugins = "fabric.testing.fixtures"

RegressionYamlDumper.add_custom_yaml_representer(
    FakeDatetime, lambda dumper, data: dumper.represent_datetime(data)
)


@pytest.fixture
def client_mode():
    with using_sarc_mode("client"):
        yield


@pytest.fixture
def scraping_mode():
    with using_sarc_mode("scraping"):
        yield


@pytest.fixture(scope="session")
def test_config_path():
    yield Path(__file__).parent / "sarc-test.yaml"


@pytest.fixture(scope="session", autouse=True)
def base_config(test_config_path):
    with gifnoc.use(test_config_path):
        with using_sarc_mode("scraping"):
            yield


@pytest.fixture(scope="session")
def base_config_with_logging():
    """To be used where config.logging is required"""
    with gifnoc.use(Path(__file__).parent / "sarc-test-with-logging.yaml"):
        with using_sarc_mode("scraping"):
            yield


@pytest.fixture
def enabled_cache(tmp_path):
    with gifnoc.overlay({"sarc.cache": str(tmp_path / "sarc-tmp-test-cache")}):
        yield


@pytest.fixture
def disabled_cache():
    with gifnoc.overlay({"sarc.cache": None}):
        yield


@pytest.fixture(scope="function", autouse=True)
def tzlocal_is_mtl(monkeypatch):
    monkeypatch.setattr("sarc.config.TZLOCAL", zoneinfo.ZoneInfo("America/Montreal"))
    monkeypatch.setattr(
        "sarc.client.job.TZLOCAL", zoneinfo.ZoneInfo("America/Montreal")
    )
    monkeypatch.setattr(
        "sarc.client.series.TZLOCAL", zoneinfo.ZoneInfo("America/Montreal")
    )
    monkeypatch.setattr(
        "sarc.cli.fetch.slurmconfig.TZLOCAL", zoneinfo.ZoneInfo("America/Montreal")
    )


@pytest.fixture
def test_config(request):
    with gifnoc.overlay({"sarc": getattr(request, "param", dict())}):
        yield config()


@pytest.fixture
def captrace():
    """
    To get the captured traces, use the `.get_finished_traces()`
    method on the captrace object in your test method. This will
    return a list of ReadableSpan objects documented here:
    https://opentelemetry-python.readthedocs.io/en/latest/sdk/trace.html#opentelemetry.sdk.trace.ReadableSpan
    """
    _exporter.clear()
    yield _exporter
    _exporter.clear()


@pytest.fixture
def cli_main():
    from sarc.cli import main
    from sarc.cli.utils import clusters

    # Update possible choices based on the current test config
    clusters.choices = list(config().clusters.keys())

    yield main


@pytest.fixture
def prom_custom_query_mock(monkeypatch):
    """Mock the custom_query method of PrometheusConnect to avoid any real query.
    The object `prom_custom_query_mock` may then be used to check the query strings passed
    to `custom_query` using `prom_custom_query_mock.call_args[0][0]`."""
    from prometheus_api_client import PrometheusConnect

    monkeypatch.setattr(
        PrometheusConnect,
        "custom_query",
        MagicMock(return_value=[]),
    )

    yield PrometheusConnect.custom_query


@pytest.fixture
def patch_return_values(monkeypatch):
    def returner(v):
        return lambda *_, **__: v

    def patch(values):
        for k, v in values.items():
            monkeypatch.setattr(k, returner(v))

    yield patch
