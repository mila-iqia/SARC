"""Validation tests for UnderusageNotifyConfig.__post_init__."""

from sarc.config import SlackConfig, UnderusageNotifyConfig

_SLACK = SlackConfig(description="test", token="xoxb-test", channel="#test")

_VALID_KWARGS = dict(
    slack=_SLACK,
    recurrence_display_cycles=5,
    recurrence_active_cycles=3,
    usage_report_cycles=2,
)


def _make(**overrides):
    return UnderusageNotifyConfig(**{**_VALID_KWARGS, **overrides})
