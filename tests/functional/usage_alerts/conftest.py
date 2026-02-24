from pathlib import Path

import gifnoc
import pytest

from sarc.config import config


@pytest.fixture
def health_config():
    """Special sarc config with health checks for testing"""
    path = Path(__file__).parent / "health-test.yaml"
    assert path.is_file()
    with gifnoc.overlay(path):
        yield config().health_monitor
