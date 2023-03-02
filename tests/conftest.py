import zoneinfo
from pathlib import Path

import pytest

from sarc.config import parse_config, using_config


@pytest.fixture(scope="session")
def standard_config_object():
    yield parse_config(Path(__file__).parent / "sarc-test.json")


@pytest.fixture(scope="session", autouse=True)
def use_standard_config(standard_config_object):  # pylint: disable=redefined-outer-name
    with using_config(standard_config_object):
        yield


@pytest.fixture
def tzlocal_is_mtl(monkeypatch):
    monkeypatch.setattr("sarc.config.TZLOCAL", zoneinfo.ZoneInfo("America/Montreal"))
    monkeypatch.setattr("sarc.jobs.job.TZLOCAL", zoneinfo.ZoneInfo("America/Montreal"))
