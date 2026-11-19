"""Acknowledgement and help buttons for usage notifications (SARC-802).

The block-kit buttons appended to an underusage DM carry no database id —
the delivery row is only created after the post succeeds, and the id is
therefore unknown while the message is being built. Instead each button's
``value`` encodes (kind, window_end); a click is matched to its delivery row
by ``(slack_user_id, kind, window_end)`` — the unique key of
notification_messages, whose slack_user_id is both the only recipient of a DM
and the interactivity payload's user identity.

Each button lives in its OWN actions block (block_id = its action_id) so a
chat.update replacing one button's block leaves the other button live.
"""

from datetime import datetime

from sarc.notifications.delivery import ACK_ACTION_ID, HELP_ACTION_ID, NotificationKind

ACK_BUTTON_LABEL = "✅ I've read this message"
HELP_BUTTON_LABEL = "🙋 I would like some help"

ACKED_FALLBACK_TEXT = "✅ Acknowledged — thanks!"
HELPED_FALLBACK_TEXT = "🙋 Help requested — the IDT team will reach out."
"""chat.update fallback texts (notification previews) once clicked."""


def _confirmation_text(action_id: str, confirmed_at: datetime) -> str:
    if action_id == HELP_ACTION_ID:
        return (
            f"🙋 Help requested on {confirmed_at:%Y-%m-%d} — "
            "the IDT team will reach out."
        )
    return f"✅ Confirmed on {confirmed_at:%Y-%m-%d} — thanks!"


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


def _button_block(action_id: str, label: str, value: str, style: str) -> dict:
    return {
        "type": "actions",
        "block_id": action_id,
        "elements": [
            {
                "type": "button",
                "action_id": action_id,
                "style": style,
                "text": {"type": "plain_text", "text": label, "emoji": True},
                "value": value,
            }
        ],
    }


def build_ack_blocks(kind: NotificationKind, window_end: datetime) -> list[dict] | None:
    """The actions blocks appended below the message body (ack + help
    buttons), or None when this notification carries no button (currently:
    underusage DMs only — the ticket's scope; the neutral usage report
    deliberately stays buttonless).
    """
    if kind != "underusage":
        return None
    value = ack_value(kind, window_end)
    return [
        _button_block(ACK_ACTION_ID, ACK_BUTTON_LABEL, value, "primary"),
        _button_block(HELP_ACTION_ID, HELP_BUTTON_LABEL, value, "default"),
    ]


def clicked_blocks(
    blocks: list[dict] | None, clicked_action_ids: list[str], confirmed_at: datetime
) -> list[dict]:
    """The message's blocks after clicks: each clicked button's actions block
    is replaced by its confirmation line (the Confluence doc's «Confirmé le …»
    behavior), so a clicked button cannot be clicked again while the other
    stays live.

    ``blocks`` is the message's block list as echoed back in the interactivity
    payload; when it is missing (payload variants that omit the message),
    degrade to just the confirmation lines — the clicks are recorded either
    way.
    """
    result = list(blocks or [])
    for action_id in clicked_action_ids:
        confirmation = {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": _confirmation_text(action_id, confirmed_at),
            },
        }
        if any(block.get("block_id") == action_id for block in result):
            result = [
                confirmation if block.get("block_id") == action_id else block
                for block in result
            ]
        else:
            # Button already replaced (re-click after an update) or message
            # blocks unavailable: (re-)append the confirmation line.
            result.append(confirmation)
    return result
