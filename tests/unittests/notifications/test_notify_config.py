"""Validation tests for UnderusageNotifyConfig.__post_init__."""

import pytest

from sarc.config import SlackConfig, UnderusageNotifyConfig

_SLACK = SlackConfig(description="test", token="xoxb-test", channel="#test")

_VALID_KWARGS = dict(
    slack=_SLACK,
    recurrence_display_cycles=5,
    recurrence_active_cycles=3,
    usage_report_window_weeks=4,
)


def _make(**overrides):
    return UnderusageNotifyConfig(**{**_VALID_KWARGS, **overrides})


@pytest.mark.parametrize(
    "field",
    [
        "digest_top_n",
        "historical_months",
        "recurrence_active_cycles",
        "recurrence_display_cycles",
        "top_jobs_per_user",
        "usage_report_window_weeks",
        "utilization_ceiling",
    ],
)
def test_zero_value_raises(field):
    with pytest.raises(ValueError, match=field):
        _make(**{field: 0})


@pytest.mark.parametrize(
    "field",
    [
        "digest_top_n",
        "historical_months",
        "recurrence_active_cycles",
        "recurrence_display_cycles",
        "top_jobs_per_user",
        "usage_report_window_weeks",
        "utilization_ceiling",
        "min_ratio",
        "min_rgu_hours",
        "recurrence_cluster_share",
        "usage_report_min_rgu_hours",
        "personalized_action_min_rgu_hours",
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


def test_utilization_ceiling_above_one_raises():
    with pytest.raises(ValueError, match="utilization_ceiling"):
        _make(utilization_ceiling=1.01)


def test_usage_report_min_rgu_hours_zero_is_valid():
    cfg = _make(usage_report_min_rgu_hours=0.0)
    assert cfg.usage_report_min_rgu_hours == 0.0


def test_personalized_action_min_rgu_hours_zero_is_valid():
    cfg = _make(personalized_action_min_rgu_hours=0.0)
    assert cfg.personalized_action_min_rgu_hours == 0.0


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
