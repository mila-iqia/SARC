"""Unit tests for the acknowledgement and help button blocks (SARC-802)."""

from datetime import UTC, datetime

import pytest

from sarc.notifications.ack import (
    ACK_ACTION_ID,
    ACK_BUTTON_LABEL,
    HELP_ACTION_ID,
    HELP_BUTTON_LABEL,
    ack_value,
    build_ack_blocks,
    clicked_blocks,
    parse_ack_value,
)

_WINDOW_END = datetime(2024, 6, 16, tzinfo=UTC)
_CONFIRMED_AT = datetime(2024, 6, 17, 9, 30, tzinfo=UTC)

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
    assert [b["block_id"] for b in blocks] == [ACK_ACTION_ID, HELP_ACTION_ID]
    expected = [
        (ACK_ACTION_ID, ACK_BUTTON_LABEL, "primary"),
        (HELP_ACTION_ID, HELP_BUTTON_LABEL, "default"),
    ]
    expected_value = ack_value("underusage", _WINDOW_END)
    for block, (action_id, label, style) in zip(blocks, expected, strict=True):
        assert block["type"] == "actions"
        (button,) = block["elements"]
        assert button["type"] == "button"
        assert button["action_id"] == action_id
        assert button["style"] == style
        assert button["text"] == {"type": "plain_text", "text": label, "emoji": True}
        # Both buttons carry the same (kind, window_end) target value.
        assert button["value"] == expected_value


def test_build_ack_blocks_none_for_usage_report():
    """The neutral usage report stays buttonless (ticket scope: underusage
    alerts only)."""
    assert build_ack_blocks("usage", _WINDOW_END) is None


# ── clicked_blocks ──────────────────────────────────────────────────────────

_BODY = {"type": "section", "text": {"type": "mrkdwn", "text": "body"}}


def _actions(action_id):
    return {
        "type": "actions",
        "block_id": action_id,
        "elements": [{"type": "button", "action_id": action_id}],
    }


def test_clicked_blocks_replaces_only_the_clicked_button():
    blocks = [_BODY, _actions(ACK_ACTION_ID), _actions(HELP_ACTION_ID)]

    result = clicked_blocks(blocks, [ACK_ACTION_ID], _CONFIRMED_AT)

    assert result[0] == _BODY
    assert result[1]["type"] == "section"
    assert "Confirmed on 2024-06-17" in result[1]["text"]["text"]
    # The help button stays live.
    assert result[2] == _actions(HELP_ACTION_ID)


def test_clicked_blocks_both_buttons():
    blocks = [_BODY, _actions(ACK_ACTION_ID), _actions(HELP_ACTION_ID)]

    result = clicked_blocks(blocks, [ACK_ACTION_ID, HELP_ACTION_ID], _CONFIRMED_AT)

    assert result[0] == _BODY
    assert "Confirmed on" in result[1]["text"]["text"]
    assert "Help requested on 2024-06-17" in result[2]["text"]["text"]


def test_clicked_blocks_without_message_blocks():
    """Payload variants that omit the message degrade to just the
    confirmation lines."""
    result = clicked_blocks(None, [HELP_ACTION_ID], _CONFIRMED_AT)
    (only,) = result
    assert only["type"] == "section"
    assert "Help requested" in only["text"]["text"]


def test_clicked_blocks_already_replaced_appends_confirmation():
    """Re-click whose echoed blocks no longer carry the button (its block was
    replaced by a confirmation whose block_id differs): a fresh confirmation
    line is appended rather than silently dropped."""
    confirmation = {
        "type": "section",
        "text": {"type": "mrkdwn", "text": "✅ Confirmed on 2024-06-17 — thanks!"},
    }
    result = clicked_blocks([_BODY, confirmation], [ACK_ACTION_ID], _CONFIRMED_AT)
    assert result[-1]["text"]["text"].startswith("✅ Confirmed on")
    assert len(result) == 3
