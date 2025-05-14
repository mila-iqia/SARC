from pathlib import Path

import gifnoc
import pytest

from sarc.config import config

here = Path(__file__).parent


@pytest.fixture
def frozen_gifnoc_time():
    with gifnoc.overlay(
        {"time": {"class": "FrozenTime", "time": "2024-01-01T00:00", "sleep_beat": 0}}
    ):
        yield


@pytest.fixture
def beans_config(tmpdir):
    setdir = {"sarc": {"health_monitor": {"directory": str(tmpdir)}}}
    cfgdir = here / "configs"
    with gifnoc.overlay(cfgdir / "base.yaml", cfgdir / "beans.yaml", setdir):
        yield config().health_monitor


@pytest.fixture
def deps_config(tmpdir):
    setdir = {"sarc": {"health_monitor": {"directory": str(tmpdir)}}}
    cfgdir = here / "configs"
    with gifnoc.overlay(cfgdir / "base.yaml", cfgdir / "deps.yaml", setdir):
        yield config().health_monitor


@pytest.fixture
def params_config(tmpdir):
    setdir = {"sarc": {"health_monitor": {"directory": str(tmpdir)}}}
    cfgdir = here / "configs"
    with gifnoc.overlay(cfgdir / "base.yaml", cfgdir / "params.yaml", setdir):
        yield config().health_monitor
