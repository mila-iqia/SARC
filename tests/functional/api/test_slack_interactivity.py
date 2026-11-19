"""Functional tests for the public POST /slack/interactivity endpoint (SARC-802)."""

import hashlib
import hmac
import json
import time
import urllib.parse
from datetime import UTC, datetime
from unittest.mock import MagicMock

import gifnoc
import pytest
from fastapi.testclient import TestClient
from sqlmodel import select

from sarc.db.notifications import NotificationFeedback, NotificationMessage
from sarc.db.users import UserDB
from sarc.notifications.ack import ACK_ACTION_ID, HELP_ACTION_ID, ack_value
from sarc.notifications.delivery import ack_stats_for_cycle, record_notification_send
from sarc.notifications.slack import SendResult, SendStatus
from tests.unittests.notifications.test_cli import _NOTIFY_CFG

_SECRET = "test-signing-secret"
_WINDOW_END = datetime(2024, 6, 16, tzinfo=UTC)
_BODY_BLOCK = {"type": "section", "text": {"type": "mrkdwn", "text": "Hi <@U0ABC>"}}
_BUTTON_BLOCK = {
    "type": "actions",
    "block_id": ACK_ACTION_ID,
    "elements": [{"type": "button", "action_id": ACK_ACTION_ID, "value": ""}],
}
_HELP_BUTTON_BLOCK = {
    "type": "actions",
    "block_id": HELP_ACTION_ID,
    "elements": [{"type": "button", "action_id": HELP_ACTION_ID, "value": ""}],
}


def _cfg(*, secret=True):
    su = dict(_NOTIFY_CFG["slack_underusage"])
    if secret:
        su["signing_secret"] = _SECRET
    return {**_NOTIFY_CFG, "slack_underusage": su}


def _signed_headers(body: bytes, *, secret=_SECRET, ts=None):
    ts = str(int(time.time())) if ts is None else str(ts)
    base = f"v0:{ts}:{body.decode()}"
    sig = "v0=" + hmac.new(secret.encode(), base.encode(), hashlib.sha256).hexdigest()
    return {"X-Slack-Signature": sig, "X-Slack-Request-Timestamp": ts}


def _post_click(client, payload, *, secret=_SECRET, ts=None):
    body = urllib.parse.urlencode({"payload": json.dumps(payload)}).encode()
    return client.post(
        "/slack/interactivity",
        content=body,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            **_signed_headers(body, secret=secret, ts=ts),
        },
    )


def _click_payload(*, user="U0ABC", value=None, action_id=ACK_ACTION_ID, blocks=None):
    actions = [
        {
            "action_id": action_id,
            "value": ack_value("underusage", _WINDOW_END) if value is None else value,
        }
    ]
    return {
        "type": "block_actions",
        "user": {"id": user},
        "channel": {"id": "D0XYZ"},
        "message": {
            "ts": "111.222",
            "blocks": blocks
            if blocks is not None
            else [_BODY_BLOCK, _BUTTON_BLOCK, _HELP_BUTTON_BLOCK],
        },
        "actions": actions,
    }


@pytest.fixture
def anon_client(app):
    """No OAuth token: the endpoint is public (Slack itself calls it)."""
    return TestClient(app)


@pytest.fixture
def mock_updater(monkeypatch):
    """Patch the SlackClient the endpoint builds for chat.update."""
    inst = MagicMock()
    inst.update_message.return_value = SendResult(SendStatus.OK)
    cls = MagicMock(return_value=inst)
    monkeypatch.setattr("sarc.api.slack.SlackClient", cls)
    return inst


def _feedback_rows(session):
    session.expire_all()
    return session.exec(select(NotificationFeedback)).all()


def _seed_delivery(session, *, kind="underusage", window_end=_WINDOW_END):
    user_id = session.exec(select(UserDB)).first().id
    record_notification_send(
        user_id=user_id,
        kind=kind,
        window_end=window_end,
        channel="D0XYZ",
        message_ts="111.222",
        slack_user_id="U0ABC",
        sent_at=window_end,
    )


# ── signature gate ──────────────────────────────────────────────────────────


def test_url_verification_challenge(anon_client):
    body = json.dumps(
        {"type": "url_verification", "challenge": "abc123", "token": "t"}
    ).encode()
    with gifnoc.overlay({"sarc.notifications": _cfg()}):
        resp = anon_client.post(
            "/slack/interactivity",
            content=body,
            headers={"Content-Type": "application/json", **_signed_headers(body)},
        )
    assert resp.status_code == 200
    assert resp.json() == {"challenge": "abc123"}


def test_invalid_signature_rejected(anon_client):
    with gifnoc.overlay({"sarc.notifications": _cfg()}):
        resp = _post_click(anon_client, _click_payload(), secret="wrong-secret")
    assert resp.status_code == 401


def test_stale_timestamp_rejected(anon_client):
    stale = int(time.time()) - 600  # beyond the 5-minute replay window
    with gifnoc.overlay({"sarc.notifications": _cfg()}):
        resp = _post_click(anon_client, _click_payload(), ts=stale)
    assert resp.status_code == 401


def test_missing_signing_secret_config_returns_503(anon_client):
    with gifnoc.overlay({"sarc.notifications": _cfg(secret=False)}):
        resp = _post_click(anon_client, _click_payload())
    assert resp.status_code == 503


# ── ack clicks ──────────────────────────────────────────────────────────────


def test_click_records_linked_feedback_and_confirms_message(
    anon_client, read_write_db, mock_updater
):
    _seed_delivery(read_write_db)
    with gifnoc.overlay({"sarc.notifications": _cfg()}):
        resp = _post_click(anon_client, _click_payload())
    assert resp.status_code == 200
    assert resp.json() == {}

    (row,) = _feedback_rows(read_write_db)
    assert row.slack_user_id == "U0ABC"
    assert row.kind == "underusage"
    assert row.window_end == _WINDOW_END
    assert row.channel == "D0XYZ"
    assert row.message_ts == "111.222"
    # Linked to the tracked delivery row seeded above.
    (message,) = read_write_db.exec(select(NotificationMessage)).all()
    assert row.message_id == message.id

    mock_updater.update_message.assert_called_once()
    args, kwargs = mock_updater.update_message.call_args
    assert args == ("D0XYZ", "111.222")
    # The actions block was replaced by the confirmation line, body kept.
    blocks = kwargs["blocks"]
    assert blocks[0] == _BODY_BLOCK
    assert blocks[1]["type"] == "section"
    assert "Confirmed on" in blocks[1]["text"]["text"]


def test_duplicate_click_deduped(anon_client, read_write_db, mock_updater):
    _seed_delivery(read_write_db)
    with gifnoc.overlay({"sarc.notifications": _cfg()}):
        assert _post_click(anon_client, _click_payload()).status_code == 200
        assert _post_click(anon_client, _click_payload()).status_code == 200

    # One feedback row (first click wins), but the confirmation update is
    # re-attempted on every click (the first update may have failed).
    assert len(_feedback_rows(read_write_db)) == 1
    assert mock_updater.update_message.call_count == 2


def test_click_without_tracked_delivery_still_recorded(
    anon_client, read_write_db, mock_updater
):
    with gifnoc.overlay({"sarc.notifications": _cfg()}):
        resp = _post_click(anon_client, _click_payload())
    assert resp.status_code == 200
    (row,) = _feedback_rows(read_write_db)
    assert row.message_id is None


def test_other_action_id_ignored(anon_client, read_write_db, mock_updater):
    with gifnoc.overlay({"sarc.notifications": _cfg()}):
        resp = _post_click(
            anon_client, _click_payload(action_id="some_other_app_button")
        )
    assert resp.status_code == 200
    assert _feedback_rows(read_write_db) == []
    mock_updater.update_message.assert_not_called()


def test_malformed_value_ignored(anon_client, read_write_db, mock_updater):
    with gifnoc.overlay({"sarc.notifications": _cfg()}):
        resp = _post_click(anon_client, _click_payload(value="garbage"))
    assert resp.status_code == 200
    assert _feedback_rows(read_write_db) == []
    mock_updater.update_message.assert_not_called()


def test_update_failure_still_records_click(anon_client, read_write_db, mock_updater):
    mock_updater.update_message.return_value = SendResult(
        SendStatus.FAILED, "message_not_found"
    )
    with gifnoc.overlay({"sarc.notifications": _cfg()}):
        resp = _post_click(anon_client, _click_payload())
    assert resp.status_code == 200
    assert len(_feedback_rows(read_write_db)) == 1


# ── help button ─────────────────────────────────────────────────────────────


def test_help_click_recorded_and_help_button_confirmed(
    anon_client, read_write_db, mock_updater
):
    _seed_delivery(read_write_db)
    with gifnoc.overlay({"sarc.notifications": _cfg()}):
        resp = _post_click(anon_client, _click_payload(action_id=HELP_ACTION_ID))
    assert resp.status_code == 200

    (row,) = _feedback_rows(read_write_db)
    assert row.action_id == HELP_ACTION_ID

    args, kwargs = mock_updater.update_message.call_args
    assert args == ("D0XYZ", "111.222")
    blocks = kwargs["blocks"]
    # The help button became its confirmation; the read button stays live,
    # and the preview text advertises the help request.
    assert blocks[0] == _BODY_BLOCK
    assert blocks[1] == _BUTTON_BLOCK
    assert "Help requested on" in blocks[2]["text"]["text"]
    assert kwargs["text"].startswith("🙋")


def test_read_then_help_both_recorded_without_hiding_each_other(
    anon_client, read_write_db, mock_updater
):
    """A help click after a read click is a distinct feedback row — and the
    second chat.update keeps the read confirmation while turning the help
    button into its own."""
    _seed_delivery(read_write_db)
    ack_confirmation = {
        "type": "section",
        "text": {"type": "mrkdwn", "text": "✅ Confirmed on 2024-06-17 — thanks!"},
    }
    with gifnoc.overlay({"sarc.notifications": _cfg()}):
        assert _post_click(anon_client, _click_payload()).status_code == 200
        # Slack echoes the CURRENT (updated) message: the ack button is
        # already its confirmation line when help is clicked next.
        assert (
            _post_click(
                anon_client,
                _click_payload(
                    action_id=HELP_ACTION_ID,
                    blocks=[_BODY_BLOCK, ack_confirmation, _HELP_BUTTON_BLOCK],
                ),
            ).status_code
            == 200
        )

    rows = _feedback_rows(read_write_db)
    assert {r.action_id for r in rows} == {ACK_ACTION_ID, HELP_ACTION_ID}
    # The read ack still counts once toward the acknowledgement rate.
    stats = ack_stats_for_cycle("underusage", _WINDOW_END)
    assert (stats.sent, stats.acknowledged) == (1, 1)

    _, kwargs = mock_updater.update_message.call_args
    blocks = kwargs["blocks"]
    assert blocks[1] == ack_confirmation  # kept from the first click
    assert "Help requested on" in blocks[2]["text"]["text"]


def test_help_request_alone_is_not_an_acknowledgement(
    anon_client, read_write_db, mock_updater
):
    _seed_delivery(read_write_db)
    with gifnoc.overlay({"sarc.notifications": _cfg()}):
        assert (
            _post_click(anon_client, _click_payload(action_id=HELP_ACTION_ID))
        ).status_code == 200

    stats = ack_stats_for_cycle("underusage", _WINDOW_END)
    assert (stats.sent, stats.acknowledged) == (1, 0)
