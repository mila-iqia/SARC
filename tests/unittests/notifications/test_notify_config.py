"""Validation tests for UnderusageNotifyConfig.__post_init__."""

import pytest

from sarc.config import SlackConfig, UnderusageNotifyConfig

_SLACK = SlackConfig(description="test", token="xoxb-test", channel="#test")

_VALID_KWARGS = dict(
    slack=_SLACK,
    window_weeks=2,
    recurrence_display_cycles=5,
    recurrence_active_cycles=3,
    usage_report_window_weeks=4,
)


def _make(**overrides):
    return UnderusageNotifyConfig(**{**_VALID_KWARGS, **overrides})


def test_valid_config_constructs():
    cfg = _make()
    assert cfg.window_weeks == 2


@pytest.mark.parametrize(
    "field",
    [
        "window_weeks",
        "usage_report_window_weeks",
        "recurrence_display_cycles",
        "recurrence_active_cycles",
    ],
)
def test_zero_value_raises(field):
    with pytest.raises(ValueError, match=field):
        _make(**{field: 0})


@pytest.mark.parametrize(
    "field",
    [
        "window_weeks",
        "usage_report_window_weeks",
        "recurrence_display_cycles",
        "recurrence_active_cycles",
    ],
)
def test_negative_value_raises(field):
    with pytest.raises(ValueError, match=field):
        _make(**{field: -1})


def test_active_cycles_greater_than_display_raises():
    with pytest.raises(ValueError, match="recurrence_active_cycles"):
        _make(recurrence_active_cycles=6, recurrence_display_cycles=5)


def test_active_cycles_equal_to_display_is_valid():
    cfg = _make(recurrence_active_cycles=5, recurrence_display_cycles=5)
    assert cfg.recurrence_active_cycles == 5


def test_utilization_ceiling_boundary_one_is_valid():
    cfg = _make(utilization_ceiling=1.0)
    assert cfg.utilization_ceiling == 1.0


def test_utilization_ceiling_zero_raises():
    with pytest.raises(ValueError, match="utilization_ceiling"):
        _make(utilization_ceiling=0.0)


def test_utilization_ceiling_negative_raises():
    with pytest.raises(ValueError, match="utilization_ceiling"):
        _make(utilization_ceiling=-0.1)


def test_utilization_ceiling_above_one_raises():
    with pytest.raises(ValueError, match="utilization_ceiling"):
        _make(utilization_ceiling=1.01)


def test_usage_report_min_rgu_hours_zero_is_valid():
    cfg = _make(usage_report_min_rgu_hours=0.0)
    assert cfg.usage_report_min_rgu_hours == 0.0


def test_usage_report_min_rgu_hours_negative_raises():
    with pytest.raises(ValueError, match="usage_report_min_rgu_hours"):
        _make(usage_report_min_rgu_hours=-1.0)


def test_personalized_action_min_rgu_hours_zero_is_valid():
    cfg = _make(personalized_action_min_rgu_hours=0.0)
    assert cfg.personalized_action_min_rgu_hours == 0.0


def test_personalized_action_min_rgu_hours_negative_raises():
    with pytest.raises(ValueError, match="personalized_action_min_rgu_hours"):
        _make(personalized_action_min_rgu_hours=-1.0)


def test_clusters_list_of_strings_is_valid():
    cfg = _make(clusters=["mila"])
    assert cfg.clusters == ["mila"]


def test_clusters_empty_list_is_valid():
    cfg = _make(clusters=[])
    assert cfg.clusters == []


def test_clusters_non_string_entry_raises():
    with pytest.raises(ValueError, match="clusters"):
        _make(clusters=[123])


def test_clusters_empty_string_entry_raises():
    with pytest.raises(ValueError, match="clusters"):
        _make(clusters=[""])
