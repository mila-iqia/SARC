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
- --channel/--message-ts are only stored on the feedback row (and used for
  the chat.update the endpoint attempts — expected to fail against a
  non-Slack ts, which is logged, not fatal).
"""

import argparse
import hashlib
import hmac
import json
import time
import urllib.parse
import urllib.request
from datetime import UTC, datetime

ACTIONS = {"ack": "sarc_notification_ack", "help": "sarc_notification_help"}


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

    payload = {
        "type": "block_actions",
        "user": {"id": args.slack_user},
        "channel": {"id": args.channel},
        "message": {
            "ts": args.message_ts,
            "blocks": [
                {"type": "section", "text": {"type": "mrkdwn", "text": "(body)"}},
                {
                    "type": "actions",
                    "block_id": ACTIONS["ack"],
                    "elements": [
                        {"type": "button", "action_id": ACTIONS["ack"], "value": ""}
                    ],
                },
                {
                    "type": "actions",
                    "block_id": ACTIONS["help"],
                    "elements": [
                        {"type": "button", "action_id": ACTIONS["help"], "value": ""}
                    ],
                },
            ],
        },
        "actions": [
            {"action_id": action_id, "value": f"{args.kind}:{window_end.isoformat()}"}
        ],
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
    with urllib.request.urlopen(request) as response:
        print(
            f"HTTP {response.status} for {args.action} click by {args.slack_user} "
            f"on {args.kind}/{args.window_end}: {response.read().decode()!r}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
