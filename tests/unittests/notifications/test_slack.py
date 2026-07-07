import logging
from unittest.mock import MagicMock, patch

from sarc.notifications.slack import SendStatus, SlackClient


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
