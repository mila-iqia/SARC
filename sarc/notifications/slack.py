# Required bot scopes: chat:write, users:read.email, im:write
from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)


class SendStatus(Enum):
    OK = "ok"
    USER_NOT_FOUND = "user_not_found"
    FAILED = "failed"


@dataclass
class SendResult:
    status: SendStatus
    detail: str = ""


class SlackClient:
    """Thin wrapper around slack_sdk.WebClient for channel posts and DMs."""

    def __init__(self, token: str) -> None:
        from slack_sdk import WebClient

        self._client: Any = WebClient(token=token)

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

    def post_channel(self, channel: str, text: str, *, preformatted: bool = False) -> SendResult:
        """Post a message to a public/private channel."""
        try:
            kwargs: dict = {"channel": channel, "text": text}
            if preformatted:
                kwargs["blocks"] = self._preformatted_blocks(text)
            self._client.chat_postMessage(**kwargs)
            return SendResult(SendStatus.OK)
        except Exception as exc:
            logger.error("Slack channel post failed: %s", exc)
            return SendResult(SendStatus.FAILED, str(exc))

    def dm_user(self, email: str, text: str, *, preformatted: bool = False) -> SendResult:
        """Send a DM to a user identified by their Slack-registered email."""
        try:
            lookup = self._client.users_lookupByEmail(email=email)
        except Exception as exc:
            err = str(exc)
            if "users_not_found" in err or "users_not_found" in getattr(exc, "response", {}).get("error", ""):
                logger.warning("Slack user not found for email %s", email)
                return SendResult(SendStatus.USER_NOT_FOUND, email)
            logger.error("Slack users.lookupByEmail failed for %s: %s", email, exc)
            return SendResult(SendStatus.FAILED, err)

        user_id = lookup["user"]["id"]

        try:
            conv = self._client.conversations_open(users=[user_id])
            channel_id = conv["channel"]["id"]
            kwargs: dict = {"channel": channel_id, "text": text}
            if preformatted:
                kwargs["blocks"] = self._preformatted_blocks(text)
            self._client.chat_postMessage(**kwargs)
            return SendResult(SendStatus.OK)
        except Exception as exc:
            logger.error("Slack DM failed for %s (%s): %s", email, user_id, exc)
            return SendResult(SendStatus.FAILED, str(exc))
