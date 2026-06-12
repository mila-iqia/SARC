from unittest.mock import MagicMock, patch

import pytest

from sarc.notifications.slack import SendStatus, SlackClient


def _make_client(mock_web_client):
    with patch("sarc.notifications.slack.SlackClient.__init__") as init:
        init.return_value = None
        client = SlackClient.__new__(SlackClient)
        client._client = mock_web_client
        return client


# ── post_channel ──────────────────────────────────────────────────────────────


def test_post_channel_success():
    web = MagicMock()
    client = _make_client(web)
    result = client.post_channel("#alerts", "hello")
    web.chat_postMessage.assert_called_once_with(channel="#alerts", text="hello")
    assert result.status == SendStatus.OK


def test_post_channel_api_error():
    web = MagicMock()
    web.chat_postMessage.side_effect = Exception("not_in_channel")
    client = _make_client(web)
    result = client.post_channel("#alerts", "hello")
    assert result.status == SendStatus.FAILED
    assert "not_in_channel" in result.detail


# ── post_channel_file ─────────────────────────────────────────────────────────


def test_post_channel_file_success():
    web = MagicMock()
    client = _make_client(web)
    result = client.post_channel_file("#alerts", "content", title="My Digest")
    web.files_upload_v2.assert_called_once_with(
        channel="#alerts", content="content", filename="digest.txt", title="My Digest"
    )
    assert result.status == SendStatus.OK


def test_post_channel_file_no_title():
    web = MagicMock()
    client = _make_client(web)
    client.post_channel_file("#alerts", "content")
    web.files_upload_v2.assert_called_once_with(
        channel="#alerts", content="content", filename="digest.txt"
    )


def test_post_channel_file_api_error():
    web = MagicMock()
    web.files_upload_v2.side_effect = Exception("missing_scope")
    client = _make_client(web)
    result = client.post_channel_file("#alerts", "content")
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


def test_dm_user_not_found():
    web = MagicMock()
    web.users_lookupByEmail.side_effect = Exception("users_not_found")
    client = _make_client(web)

    result = client.dm_user("ghost@example.com", "hi")

    assert result.status == SendStatus.USER_NOT_FOUND
    assert result.detail == "ghost@example.com"
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
