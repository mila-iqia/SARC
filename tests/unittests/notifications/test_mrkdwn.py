import pytest

from sarc.notifications.mrkdwn import to_slack_mrkdwn


def test_bold_converted_to_single_asterisk():
    assert to_slack_mrkdwn("**bold**") == "*bold*"


def test_italic_unchanged():
    assert to_slack_mrkdwn("_italic_") == "_italic_"


def test_link_converted_to_angle_pipe_syntax():
    assert (
        to_slack_mrkdwn("[docs](https://example.com)") == "<https://example.com|docs>"
    )


def test_bare_autolink_has_no_pipe():
    assert to_slack_mrkdwn("<https://example.com>") == "<https://example.com>"


def test_nested_list_preserves_indentation():
    md = "- Office Hours:\n  - Tuesdays\n  - Wednesdays\n"
    assert to_slack_mrkdwn(md) == "- Office Hours:\n  - Tuesdays\n  - Wednesdays"


def test_fenced_code_block_passes_through_unchanged():
    table = "┌─ job_6903351 (2026-05-19) — 944.7 RGU-h unused  (GPU utilization: 34 %)"
    md = f"```\n{table}\n```\n"
    assert to_slack_mrkdwn(md) == f"```\n{table}\n```"


def test_blockquote_unchanged():
    assert to_slack_mrkdwn("> quoted text\n") == "> quoted text"


def test_placeholder_angle_bracket_passes_through_unchanged():
    assert to_slack_mrkdwn("**Charter**: <link TBD>") == "*Charter*: <link TBD>"


def test_heading_becomes_bold_not_hash():
    assert to_slack_mrkdwn("# Title") == "*Title*"


def test_reference_style_link_raises():
    with pytest.raises(NotImplementedError, match="Reference-style links"):
        to_slack_mrkdwn("[docs][1]\n\n[1]: https://example.com\n")


def test_image_converted_to_angle_pipe_syntax():
    assert (
        to_slack_mrkdwn("![alt text](https://example.com/img.png)")
        == "<https://example.com/img.png|alt text>"
    )


def test_thematic_break_becomes_dashes():
    assert to_slack_mrkdwn("---\n") == "---"
