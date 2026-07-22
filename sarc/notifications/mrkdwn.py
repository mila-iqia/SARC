from typing import cast

import mistune
from mistune.renderers.markdown import MarkdownRenderer


class SlackMrkdwnRenderer(MarkdownRenderer):
    """Re-emit parsed Markdown as Slack mrkdwn.

    Subclasses mistune's own Markdown round-trip renderer: codespan,
    block_code, block_quote, list/list_item, emphasis and inline_html already
    emit syntax that is valid Slack mrkdwn (or must pass through unchanged,
    e.g. a fenced code block), so only the constructs Slack actually spells
    differently need overriding.

    Literal `&`, `<`, `>` are deliberately passed through un-escaped rather
    than converted to HTML entities: verified empirically to render as-is in
    Slack, and escaping would corrupt the `<url|text>` / `<url>` link syntax
    this renderer itself emits.
    """

    NAME = "slack_mrkdwn"

    def emphasis(self, token, state):
        # The base renderer normalizes emphasis to single "*", which would
        # collide with Slack's bold syntax; Slack spells italic with "_".
        return f"_{self.render_children(token, state)}_"

    def strong(self, token, state):
        return f"*{self.render_children(token, state)}*"

    def link(self, token, state):
        text = self.render_children(token, state)
        if token.get("label"):
            # Reference-style links ([text][label]) are unsupported by design.
            # Raise rather than silently dropping the URL, so a template edit
            # that introduces this syntax is caught immediately instead of
            # shipping a Slack message with a quietly broken link.
            raise NotImplementedError(
                "Reference-style links ([text][label]) are not supported by "
                "SlackMrkdwnRenderer; use inline links ([text](url)) in templates."
            )
        url = token["attrs"]["url"]
        if url in (text, f"mailto:{text}"):
            return f"<{url}>"
        return f"<{url}|{text}>"

    def image(self, token, state):
        return self.link(token, state)

    def heading(self, token, state):
        return f"*{self.render_children(token, state)}*\n\n"

    def thematic_break(self, token, state):  # noqa: ARG002
        return "---\n\n"


_slack_mrkdwn = mistune.Markdown(renderer=SlackMrkdwnRenderer())


def to_slack_mrkdwn(text: str) -> str:
    """Convert authored Markdown text to Slack mrkdwn."""
    # mistune.Markdown.__call__ is typed to return str | list[dict] because a
    # renderer of None yields token dicts, but SlackMrkdwnRenderer above always
    # renders to a str.
    return cast(str, _slack_mrkdwn(text)).rstrip("\n")
