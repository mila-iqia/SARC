"""Slack interactivity endpoint for notification buttons (SARC-802).

A public route (no OAuth gate, like /upload-report): trust comes not from
who calls it but from Slack's request signing — every request must carry an
``X-Slack-Signature`` HMAC of the raw body keyed by the app's signing secret.
Receives the URL-verification challenge and ``block_actions`` payloads for
the read/help buttons built in ``sarc.notifications.ack``: records the click
in notification_feedback and chat.update's the message so clicked buttons
turn into their confirmation lines.
"""

import hashlib
import hmac
import json
import logging
import time
import urllib.parse
from datetime import UTC, datetime

from fastapi import APIRouter, HTTPException, Request, status

from sarc.config import config
from sarc.notifications.ack import (
    ACK_ACTION_ID,
    ACKED_FALLBACK_TEXT,
    HELP_ACTION_ID,
    HELPED_FALLBACK_TEXT,
    clicked_blocks,
    parse_ack_value,
)
from sarc.notifications.delivery import (
    find_notification_message_id,
    record_notification_feedback,
)
from sarc.notifications.slack import SendStatus, SlackClient

logger = logging.getLogger(__name__)

router = APIRouter(tags=["slack"])

# Slack itself rejects replayed requests older than 5 minutes; same window.
MAX_SIGNATURE_AGE_SECONDS = 300


def _signing_secret() -> str:
    ncfg = config.notifications
    secret = ncfg.slack_underusage.signing_secret if ncfg is not None else None
    if secret is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Slack interactivity is not configured",
        )
    return str(secret)


def verify_request_signature(
    secret: str,
    body: bytes,
    timestamp: str | None,
    signature: str | None,
    *,
    now: float | None = None,
) -> bool:
    """Slack's v0 request-signing scheme: HMAC-SHA256 over
    ``v0:<timestamp>:<raw body>``, keyed by the app's signing secret. A
    timestamp skewed by more than MAX_SIGNATURE_AGE_SECONDS is rejected
    (replay protection)."""
    if not timestamp or not signature:
        return False
    try:
        ts = int(timestamp)
    except ValueError:
        return False
    if abs((time.time() if now is None else now) - ts) > MAX_SIGNATURE_AGE_SECONDS:
        return False
    base = f"v0:{timestamp}:{body.decode('utf-8', 'replace')}"
    expected = (
        "v0=" + hmac.new(secret.encode(), base.encode(), hashlib.sha256).hexdigest()
    )
    return hmac.compare_digest(expected, signature)


def _decode_payload(body: bytes, content_type: str) -> dict:
    """The interactivity payload from either encoding: url_verification
    arrives as JSON, everything else as ``payload=<json>`` form-encoded."""
    try:
        if "application/json" in content_type:
            payload = json.loads(body or b"{}")
        else:
            form = urllib.parse.parse_qs(body.decode("utf-8", "replace"))
            raw = (form.get("payload") or [""])[0]
            payload = json.loads(raw) if raw else {}
    except json.JSONDecodeError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"undecodable Slack payload: {exc}",
        ) from exc
    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Slack payload is not a JSON object",
        )
    return payload


@router.post("/slack/interactivity")
async def slack_interactivity(request: Request) -> dict:
    """Handle a Slack interactivity request (signature-verified).

    Responds to the URL-verification challenge, and for button clicks (read
    / help): records one deduplicated notification_feedback row per clicked
    button, then best-effort chat.update's the message so each clicked button
    turns into its confirmation line while the other stays live. Always
    answers 200 to a genuine click (even a duplicate or an unresolvable one)
    so Slack does not surface an error to the clicker; the click is recorded
    even when the message update fails — a re-click then stays deduped.
    """
    body = await request.body()
    if not verify_request_signature(
        _signing_secret(),
        body,
        request.headers.get("x-slack-request-timestamp"),
        request.headers.get("x-slack-signature"),
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid Slack request signature",
        )

    payload = _decode_payload(body, request.headers.get("content-type", ""))

    if payload.get("type") == "url_verification":
        return {"challenge": payload.get("challenge", "")}

    if payload.get("type") != "block_actions":
        # Anything else the app is registered for (shortcuts, dialogs...) —
        # nothing to do, but the request was authentic.
        return {}

    user = (payload.get("user") or {}).get("id") or ""
    channel = (
        (payload.get("channel") or {}).get("id")
        or ((payload.get("container") or {}).get("channel_id"))
        or ""
    )
    message = payload.get("message") or {}
    message_ts = (
        message.get("ts") or (payload.get("container") or {}).get("message_ts") or ""
    )
    clicked_at = datetime.now(UTC)

    clicked_action_ids: list[str] = []
    for action in payload.get("actions", []):
        action_id = action.get("action_id")
        if action_id not in (ACK_ACTION_ID, HELP_ACTION_ID):
            continue
        if not user:
            logger.warning("Button action without a user in payload — ignored")
            continue
        try:
            kind, window_end = parse_ack_value(action.get("value", ""))
        except ValueError:
            logger.warning(
                "Ignoring unparseable button value %r from user %s",
                action.get("value"),
                user,
            )
            continue
        message_id = find_notification_message_id(
            slack_user_id=user, kind=kind, window_end=window_end
        )
        record_notification_feedback(
            slack_user_id=user,
            kind=kind,
            window_end=window_end,
            action_id=action_id,
            channel=channel,
            message_ts=message_ts,
            clicked_at=clicked_at,
            message_id=message_id,
        )
        if action_id not in clicked_action_ids:
            clicked_action_ids.append(action_id)

    if clicked_action_ids and channel and message_ts:
        _confirm_message(
            channel, message_ts, message.get("blocks"), clicked_action_ids, clicked_at
        )
    return {}


def _confirm_message(
    channel: str,
    message_ts: str,
    blocks: list[dict] | None,
    clicked_action_ids: list[str],
    confirmed_at: datetime,
) -> None:
    """Best-effort chat.update turning the clicked buttons into their
    confirmations. Failures are logged only: the clicks are already recorded,
    and re-clicks are deduped rather than double-counted."""
    ncfg = config.notifications
    assert ncfg is not None  # guarded by _signing_secret()'s 503 earlier
    # A help request is the state worth previewing in the notification text.
    fallback = (
        HELPED_FALLBACK_TEXT
        if HELP_ACTION_ID in clicked_action_ids
        else ACKED_FALLBACK_TEXT
    )
    client = SlackClient(ncfg.slack_underusage.token)
    result = client.update_message(
        channel,
        message_ts,
        blocks=clicked_blocks(blocks, clicked_action_ids, confirmed_at),
        text=fallback,
    )
    if result.status != SendStatus.OK:
        logger.error(
            "Click recorded but chat.update failed for %s/%s: %s",
            channel,
            message_ts,
            result.detail,
        )
