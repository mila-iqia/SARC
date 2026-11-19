"""Acknowledgement button blocks for usage notifications (SARC-802).

The block-kit button appended to an underusage DM carries no database id —
the delivery row is only created after the post succeeds, and the id is
therefore unknown while the message is being built. Instead the button's
``value`` encodes (kind, window_end); a click is matched to its delivery row
by ``(slack_user_id, kind, window_end)`` — the unique key of
notification_messages, whose slack_user_id is both the only recipient of a DM
and the interactivity payload's user identity.
"""

from datetime import datetime

from sarc.notifications.delivery import NotificationKind

ACK_ACTION_ID = "sarc_notification_ack"
"""action_id of the single confirm button; the interactivity endpoint routes
on it."""

ACK_BUTTON_LABEL = "✅ I've read this message"

_ACK_VALUE_SEPARATOR = ":"


def ack_value(kind: NotificationKind, window_end: datetime) -> str:
    """The button ``value`` tagging a message's acknowledgement target."""
    return f"{kind}{_ACK_VALUE_SEPARATOR}{window_end.isoformat()}"


def parse_ack_value(value: str) -> tuple[NotificationKind, datetime]:
    """Inverse of `ack_value`. Raises ValueError on a malformed value (an
    unknown kind or a non-ISO timestamp — i.e. not our own button)."""
    kind, _, stamp = value.partition(_ACK_VALUE_SEPARATOR)
    if kind not in ("underusage", "usage"):
        raise ValueError(f"unknown acknowledgement kind in value {value!r}")
    return kind, datetime.fromisoformat(stamp)


def build_ack_blocks(kind: NotificationKind, window_end: datetime) -> list[dict] | None:
    """The actions block appended below the message body, or None when this
    notification carries no button (currently: underusage DMs only — the
    ticket's scope; the neutral usage report deliberately stays buttonless).
    """
    if kind != "underusage":
        return None
    return [
        {
            "type": "actions",
            "block_id": ACK_ACTION_ID,
            "elements": [
                {
                    "type": "button",
                    "action_id": ACK_ACTION_ID,
                    "style": "primary",
                    "text": {
                        "type": "plain_text",
                        "text": ACK_BUTTON_LABEL,
                        "emoji": True,
                    },
                    "value": ack_value(kind, window_end),
                }
            ],
        }
    ]
