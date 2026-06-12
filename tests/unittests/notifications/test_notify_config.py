"""Validation tests for UnderusageNotifyConfig.__post_init__."""

import pytest

from sarc.config import SlackConfig, UnderusageNotifyConfig

_SLACK = SlackConfig(description="test", token="xoxb-test", channel="#test")

_VALID_KWARGS = dict(
    slack=_SLACK,
    window_weeks=2,
    recurrence_window_weeks=6,
    cycle_length_weeks=2,
    recurrence_display_cycles=5,
    recurrence_active_cycles=3,
    usage_report_window_weeks=4,
)


def _make(**overrides):
    return UnderusageNotifyConfig(**{**_VALID_KWARGS, **overrides})


def test_valid_config_constructs():
    cfg = _make()
    assert cfg.window_weeks == 2


@pytest.mark.parametrize("field", [
    "window_weeks",
    "usage_report_window_weeks",
    "recurrence_window_weeks",
    "cycle_length_weeks",
    "recurrence_display_cycles",
    "recurrence_active_cycles",
])
def test_zero_value_raises(field):
    with pytest.raises(ValueError, match=field):
        _make(**{field: 0})


@pytest.mark.parametrize("field", [
    "window_weeks",
    "usage_report_window_weeks",
    "recurrence_window_weeks",
    "cycle_length_weeks",
    "recurrence_display_cycles",
    "recurrence_active_cycles",
])
def test_negative_value_raises(field):
    with pytest.raises(ValueError, match=field):
        _make(**{field: -1})


def test_active_cycles_greater_than_display_raises():
    with pytest.raises(ValueError, match="recurrence_active_cycles"):
        _make(recurrence_active_cycles=6, recurrence_display_cycles=5)


def test_active_cycles_equal_to_display_is_valid():
    cfg = _make(recurrence_active_cycles=5, recurrence_display_cycles=5)
    assert cfg.recurrence_active_cycles == 5
