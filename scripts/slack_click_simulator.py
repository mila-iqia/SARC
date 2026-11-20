#!/usr/bin/env python3
"""Simulate a click on a usage-notification button (SARC-802) locally.

Slack cannot reach a dev machine, so the interactivity endpoint is exercised
instead by POSTing a correctly SIGNED block_actions payload — the same scheme
Slack itself uses (v0 HMAC over ``v0:<timestamp>:<body>``). Pair with
``sarc usage notify --send-to <you>`` (real DMs to yourself) to check the
whole chain locally: click -> feedback row -> chat.update attempt ->
``sarc usage ack-stats``.

Example:
    uv run scripts/slack_click_simulator.py \
        --secret "$SLACK_SIGNING_SECRET" \
        --slack-user U0123456789 \
        --action ack \
        --window-end 2026-11-09

Notes:
- --window-end must match the campaign's window end (the button's encoded
  value) for the click to resolve to its notification_messages row; any
  click is recorded either way.
- --message-ts uses Slack's dotted form (1763578293.524512). A message link
  shows it WITHOUT the dot (…/p1763578293524512): re-insert it before the
  last 6 digits, or chat.update answers message_not_found.
- The echoed message blocks come from build_ack_blocks() itself, so the
  endpoint's chat.update receives exactly the blocks the app posted (Slack
  rejects a chat.update whose blocks omit e.g. a button's text, and stub
  blocks would fail against a real message).
"""

import argparse
import hashlib
import hmac
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import UTC, datetime

from sarc.notifications.ack import ack_value, build_ack_blocks
from sarc.notifications.delivery import ACK_ACTION_ID, HELP_ACTION_ID

ACTIONS = {"ack": ACK_ACTION_ID, "help": HELP_ACTION_ID}


def build_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--url", default="http://localhost:8000/slack/interactivity")
    p.add_argument("--secret", required=True, help="App signing secret")
    p.add_argument(
        "--slack-user", required=True, help="Slack user id of the clicker (U…)"
    )
    p.add_argument("--action", choices=sorted(ACTIONS), default="ack")
    p.add_argument("--kind", default="underusage", choices=["underusage", "usage"])
    p.add_argument(
        "--window-end",
        required=True,
        help="Campaign window end (YYYY-MM-DD), as encoded in the button value",
    )
    p.add_argument("--channel", default="D0DEBUG0")
    p.add_argument("--message-ts", default="0000.000100")
    return p.parse_args()


def main() -> int:
    args = build_args()
    window_end = datetime.fromisoformat(args.window_end).replace(tzinfo=UTC)
    action_id = ACTIONS[args.action]

    value = ack_value(args.kind, window_end)
    payload = {
        "type": "block_actions",
        "user": {"id": args.slack_user},
        "channel": {"id": args.channel},
        "message": {
            "ts": args.message_ts,
            "blocks": [
                {"type": "section", "text": {"type": "mrkdwn", "text": "(body)"}},
                # usage reports carry no buttons (mirrors the real message):
                *(build_ack_blocks(args.kind, window_end) or []),
            ],
        },
        "actions": [{"action_id": action_id, "value": value}],
    }
    body = urllib.parse.urlencode({"payload": json.dumps(payload)}).encode()
    stamp = str(int(time.time()))
    base = f"v0:{stamp}:{body.decode()}"
    signature = (
        "v0="
        + hmac.new(args.secret.encode(), base.encode(), hashlib.sha256).hexdigest()
    )

    request = urllib.request.Request(
        args.url,
        data=body,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "X-Slack-Request-Timestamp": stamp,
            "X-Slack-Signature": signature,
        },
    )
    label = f"{args.action} click by {args.slack_user} on {args.kind}/{args.window_end}"
    try:
        with urllib.request.urlopen(request) as response:
            print(f"HTTP {response.status} for {label}: {response.read().decode()!r}")
        return 0
    except urllib.error.HTTPError as error:
        # FastAPI error bodies carry the why ({"detail": …}) — far more useful
        # than a traceback (e.g. 503 = signing_secret missing from config,
        # 401 = --secret does not match the server's).
        print(
            f"HTTP {error.code} {error.reason} for {label}: {error.read().decode()}",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
