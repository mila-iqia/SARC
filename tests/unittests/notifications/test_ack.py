"""Unit tests for the acknowledgement button blocks (SARC-802)."""

from datetime import UTC, datetime

import pytest

from sarc.notifications.ack import (
    ACK_ACTION_ID,
    ACK_BUTTON_LABEL,
    ack_value,
    build_ack_blocks,
    parse_ack_value,
)

_WINDOW_END = datetime(2024, 6, 16, tzinfo=UTC)

# ── value round-trip ────────────────────────────────────────────────────────


@pytest.mark.parametrize("kind", ["underusage", "usage"])
def test_ack_value_round_trip(kind):
    value = ack_value(kind, _WINDOW_END)
    assert parse_ack_value(value) == (kind, _WINDOW_END)


def test_parse_ack_value_rejects_unknown_kind():
    with pytest.raises(ValueError):
        parse_ack_value("digest:2024-06-16T00:00:00+00:00")


def test_parse_ack_value_rejects_malformed_timestamp():
    with pytest.raises(ValueError):
        parse_ack_value("underusage:not-a-date")


# ── block shape ─────────────────────────────────────────────────────────────


def test_build_ack_blocks_underusage_shape():
    blocks = build_ack_blocks("underusage", _WINDOW_END)
    assert blocks is not None
    (block,) = blocks
    assert block["type"] == "actions"
    assert block["block_id"] == ACK_ACTION_ID
    (button,) = block["elements"]
    assert button["type"] == "button"
    assert button["action_id"] == ACK_ACTION_ID
    assert button["style"] == "primary"
    assert button["text"] == {
        "type": "plain_text",
        "text": ACK_BUTTON_LABEL,
        "emoji": True,
    }
    assert parse_ack_value(button["value"]) == ("underusage", _WINDOW_END)


def test_build_ack_blocks_none_for_usage_report():
    """The neutral usage report stays buttonless (ticket scope: underusage
    alerts only)."""
    assert build_ack_blocks("usage", _WINDOW_END) is None
