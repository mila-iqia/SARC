import logging
import ssl
from unittest.mock import MagicMock, patch

import pytest

from sarc.notifications.slack import MENTION_TOKEN, SendStatus, SlackClient


def _make_client(mock_web_client):
    with patch("sarc.notifications.slack.SlackClient.__init__") as init:
        init.return_value = None
        client = SlackClient.__new__(SlackClient)
        client._client = mock_web_client
        return client


# ── __init__ ──────────────────────────────────────────────────────────────────


def test_init_attaches_rate_limit_retry_handler():
    from slack_sdk.http_retry.builtin_handlers import RateLimitErrorRetryHandler

    # WebClient construction makes no network calls; a fake token is fine.
    client = SlackClient("xoxb-fake")
    handlers = [
        h
        for h in client._client.retry_handlers
        if isinstance(h, RateLimitErrorRetryHandler)
    ]
    assert len(handlers) == 1
    assert handlers[0].max_retry_count == 3


def test_init_configures_certifi_ssl_context():
    client = SlackClient("xoxb-fake")
    assert isinstance(client._client.ssl, ssl.SSLContext)


# ── _message_kwargs ────────────────────────────────────────────────────────────


@pytest.mark.parametrize("preformatted", [True, False])
def test_message_kwargs_preformatted(preformatted):
    client = _make_client(MagicMock())
    kwargs = client._message_kwargs("C123", "hello", preformatted=preformatted)

    assert kwargs["channel"] == "C123"
    assert kwargs["text"] == "hello"
    if preformatted:
        assert kwargs["blocks"] == SlackClient._preformatted_blocks("hello")
    else:
        assert "blocks" not in kwargs


# ── post_channel ──────────────────────────────────────────────────────────────


def test_post_channel_success():
    web = MagicMock()
    web.chat_postMessage.return_value = {"ts": "111.222"}
    client = _make_client(web)
    result = client.post_channel("#alerts", "hello")
    web.chat_postMessage.assert_called_once_with(channel="#alerts", text="hello")
    assert result.status == SendStatus.OK
    assert result.ts == "111.222"


def test_post_channel_thread_ts_passed_through():
    web = MagicMock()
    web.chat_postMessage.return_value = {"ts": "111.223"}
    client = _make_client(web)
    result = client.post_channel("#alerts", "reply", thread_ts="111.222")
    web.chat_postMessage.assert_called_once_with(
        channel="#alerts", text="reply", thread_ts="111.222"
    )
    assert result.status == SendStatus.OK


def test_post_channel_thread_ts_omitted_when_none():
    web = MagicMock()
    web.chat_postMessage.return_value = {"ts": "111.222"}
    client = _make_client(web)
    client.post_channel("#alerts", "hello", thread_ts=None)
    assert "thread_ts" not in web.chat_postMessage.call_args.kwargs


def test_post_channel_api_error():
    web = MagicMock()
    web.chat_postMessage.side_effect = Exception("not_in_channel")
    client = _make_client(web)
    result = client.post_channel("#alerts", "hello")
    assert result.status == SendStatus.FAILED
    assert "not_in_channel" in result.detail
    assert result.ts is None


# ── upload_files ──────────────────────────────────────────────────────────────


def test_upload_files_success():
    web = MagicMock()
    client = _make_client(web)
    result = client.upload_files(
        "#alerts", [("a.csv", "a,b\n1,2"), ("b.csv", "x,y\n3,4")]
    )
    web.files_upload_v2.assert_called_once_with(
        channel="#alerts",
        file_uploads=[
            {"filename": "a.csv", "content": "a,b\n1,2"},
            {"filename": "b.csv", "content": "x,y\n3,4"},
        ],
        initial_comment=None,
        thread_ts=None,
    )
    assert result.status == SendStatus.OK


def test_upload_files_thread_ts_passed_through():
    web = MagicMock()
    client = _make_client(web)
    client.upload_files("#alerts", [("a.csv", "a,b")], thread_ts="111.222")
    assert web.files_upload_v2.call_args.kwargs["thread_ts"] == "111.222"


def test_upload_files_api_error():
    web = MagicMock()
    web.files_upload_v2.side_effect = Exception("missing_scope")
    client = _make_client(web)
    result = client.upload_files("#alerts", [("a.csv", "a,b")])
    assert result.status == SendStatus.FAILED
    assert "missing_scope" in result.detail


# ── dm_user ───────────────────────────────────────────────────────────────────


def test_dm_user_success():
    web = MagicMock()
    web.users_lookupByEmail.return_value = {"user": {"id": "U12345"}}
    web.conversations_open.return_value = {"channel": {"id": "C99999"}}
    client = _make_client(web)

    result = client.dm_user("alice@example.com", "hi alice")

    web.users_lookupByEmail.assert_called_once_with(email="alice@example.com")
    web.conversations_open.assert_called_once_with(users=["U12345"])
    web.chat_postMessage.assert_called_once_with(channel="C99999", text="hi alice")
    assert result.status == SendStatus.OK


def test_dm_user_replaces_mention_token():
    web = MagicMock()
    web.users_lookupByEmail.return_value = {"user": {"id": "U12345"}}
    web.conversations_open.return_value = {"channel": {"id": "C99999"}}
    client = _make_client(web)

    result = client.dm_user("alice@example.com", f"Hi {MENTION_TOKEN}, welcome")

    web.chat_postMessage.assert_called_once_with(
        channel="C99999", text="Hi <@U12345>, welcome"
    )
    assert result.status == SendStatus.OK


def test_dm_user_warns_when_mention_token_missing(caplog):
    web = MagicMock()
    web.users_lookupByEmail.return_value = {"user": {"id": "U12345"}}
    web.conversations_open.return_value = {"channel": {"id": "C99999"}}
    client = _make_client(web)

    with caplog.at_level(logging.WARNING, logger="sarc.notifications.slack"):
        result = client.dm_user("alice@example.com", "hi there, no mention here")

    assert any(r.levelno == logging.WARNING for r in caplog.records)
    web.chat_postMessage.assert_called_once_with(
        channel="C99999", text="hi there, no mention here"
    )
    assert result.status == SendStatus.OK


def test_dm_user_not_found():
    web = MagicMock()
    web.users_lookupByEmail.side_effect = Exception("users_not_found")
    client = _make_client(web)

    result = client.dm_user("ghost@example.com", "hi")

    assert result.status == SendStatus.USER_NOT_FOUND
    web.conversations_open.assert_not_called()
    web.chat_postMessage.assert_not_called()


def test_dm_user_lookup_other_error():
    web = MagicMock()
    web.users_lookupByEmail.side_effect = Exception("ratelimited")
    client = _make_client(web)

    result = client.dm_user("alice@example.com", "hi")

    assert result.status == SendStatus.FAILED
    assert "ratelimited" in result.detail


def test_dm_user_postmessage_error():
    web = MagicMock()
    web.users_lookupByEmail.return_value = {"user": {"id": "U12345"}}
    web.conversations_open.return_value = {"channel": {"id": "C99999"}}
    web.chat_postMessage.side_effect = Exception("msg_too_long")
    client = _make_client(web)

    result = client.dm_user("alice@example.com", "hi")

    assert result.status == SendStatus.FAILED
    assert "msg_too_long" in result.detail


def test_dm_user_not_found_via_response_data():
    """SlackApiError carries error code in .response.data, not in str(exc)."""
    web = MagicMock()
    exc = Exception("some_slack_error")
    exc.response = MagicMock()
    exc.response.data = {"error": "users_not_found"}
    web.users_lookupByEmail.side_effect = exc
    client = _make_client(web)

    result = client.dm_user("ghost@example.com", "hi")

    assert result.status == SendStatus.USER_NOT_FOUND


def test_dm_user_not_found_response_without_data_attr():
    """Response object that lacks .data does not raise AttributeError."""
    web = MagicMock()
    exc = Exception("something_went_wrong")
    exc.response = object()  # has no .data
    web.users_lookupByEmail.side_effect = exc
    client = _make_client(web)

    result = client.dm_user("alice@example.com", "hi")

    assert result.status == SendStatus.FAILED


# ── rapporteur contract ───────────────────────────────────────────────────────
# rapporteur's LogHook only captures records at ERROR level and above; delivery
# failures must be logged at ERROR to reach the Slack error report.


def test_delivery_failures_log_at_error_level(caplog):
    web = MagicMock()
    web.users_lookupByEmail.return_value = {"user": {"id": "U12345"}}
    web.conversations_open.return_value = {"channel": {"id": "C99999"}}
    web.chat_postMessage.side_effect = Exception("msg_too_long")
    client = _make_client(web)

    with caplog.at_level(logging.ERROR, logger="sarc.notifications.slack"):
        client.dm_user("alice@example.com", "hi")

    assert any(r.levelno == logging.ERROR for r in caplog.records)


def test_lookup_failures_log_at_error_level(caplog):
    web = MagicMock()
    web.users_lookupByEmail.side_effect = Exception("internal_error")
    client = _make_client(web)

    with caplog.at_level(logging.ERROR, logger="sarc.notifications.slack"):
        client.dm_user("alice@example.com", "hi")

    assert any(r.levelno == logging.ERROR for r in caplog.records)
