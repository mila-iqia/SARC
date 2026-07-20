# Required bot scopes: chat:write, im:write, users:read.email
from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any

from slack_sdk import WebClient
from slack_sdk.http_retry.builtin_handlers import RateLimitErrorRetryHandler

logger = logging.getLogger(__name__)


class SendStatus(Enum):
    OK = "ok"
    USER_NOT_FOUND = "user_not_found"
    FAILED = "failed"


@dataclass
class SendResult:
    status: SendStatus
    detail: str = ""
    ts: str | None = None


class SlackClient:
    """Thin wrapper around slack_sdk.WebClient for channel posts and DMs."""

    def __init__(self, token: str) -> None:
        self._client: Any = WebClient(token=token)
        # Auto-retry HTTP 429s (sleeps for the Retry-After duration); applies to
        # every API call made through this client.
        self._client.retry_handlers.append(
            RateLimitErrorRetryHandler(max_retry_count=3)
        )

    @staticmethod
    def _preformatted_blocks(text: str) -> list[dict]:
        return [
            {
                "type": "rich_text",
                "elements": [
                    {
                        "type": "rich_text_preformatted",
                        "elements": [{"type": "text", "text": text}],
                    }
                ],
            }
        ]

    def _message_kwargs(self, channel: str, text: str, *, preformatted: bool) -> dict:
        kwargs: dict = {"channel": channel, "text": text}
        if preformatted:
            kwargs["blocks"] = self._preformatted_blocks(text)
        return kwargs

    def post_channel(
        self,
        channel: str,
        text: str,
        *,
        preformatted: bool = False,
        thread_ts: str | None = None,
    ) -> SendResult:
        """Post a message to a public/private channel.

        Pass thread_ts to reply in a message's thread; the returned
        SendResult carries the posted message's ts for threading follow-ups.
        """
        try:
            kwargs = self._message_kwargs(channel, text, preformatted=preformatted)
            if thread_ts is not None:
                kwargs["thread_ts"] = thread_ts
            resp = self._client.chat_postMessage(**kwargs)
            return SendResult(SendStatus.OK, ts=resp["ts"])
        except Exception as exc:
            logger.error("Slack channel post failed: %s", exc)
            return SendResult(SendStatus.FAILED, str(exc))

    def dm_user(
        self, email: str, text: str, *, preformatted: bool = False
    ) -> SendResult:
        """Send a DM to a user identified by their Slack-registered email."""
        try:
            lookup = self._client.users_lookupByEmail(email=email)
        except Exception as exc:
            err = str(exc)
            # Read the response error from SlackApiError.response.data["error"]
            response_error = getattr(getattr(exc, "response", None), "data", {}).get(
                "error", ""
            )
            if "users_not_found" in err or "users_not_found" in response_error:
                logger.warning("Slack user not found for email %s", email)
                send_status = SendStatus.USER_NOT_FOUND
            else:
                send_status = None
            logger.error("Slack users.lookupByEmail failed for %s: %s", email, exc)
            return SendResult(send_status or SendStatus.FAILED, err)

        user_id = lookup["user"]["id"]

        try:
            conv = self._client.conversations_open(users=[user_id])
            channel_id = conv["channel"]["id"]
            self._client.chat_postMessage(
                **self._message_kwargs(channel_id, text, preformatted=preformatted)
            )
            return SendResult(SendStatus.OK)
        except Exception as exc:
            logger.error("Slack DM failed for %s (%s): %s", email, user_id, exc)
            return SendResult(SendStatus.FAILED, str(exc))
