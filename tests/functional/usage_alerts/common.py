def _get_warnings(text: str, module: str | list[str]) -> list:
    """Parse warning messages from given text (typically caplog.text)"""
    if not isinstance(module, list):
        module = [module]

    warnings = []
    for line in text.split("\n"):
        line = line.strip()
        if line.startswith("ERROR "):
            line_content = line[len("ERROR") :].lstrip()
            line_ref, warning_msg = line_content.split(" ", maxsplit=1)
            assert any(
                line_ref.startswith(f"{module_str}:") for module_str in module
            ), line_ref
            warnings.append(warning_msg.strip())
    return warnings
