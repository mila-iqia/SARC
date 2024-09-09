def _get_warnings(text: str, module: str) -> list:
    """Parse warning messages from given text (typically caplog.text)"""
    warnings = []
    for line in text.split("\n"):
        line = line.strip()
        if line.startswith("WARNING "):
            line_content = line[len("WARNING") :].lstrip()
            line_ref, warning_msg = line_content.split(" ", maxsplit=1)
            assert line_ref.startswith(f"{module}:"), line_ref
            warnings.append(warning_msg.strip())
    return warnings
