import os
import subprocess
from pathlib import Path

import pytest

examples = [
    p.name for p in (Path(__file__).parent.parent.parent / "examples").glob("*.py")
]
# Exclude utils.py, the utilities used by a few different examples.
examples.remove("utils.py")

example_marks = {
    "allocation_usage.py": pytest.mark.xfail(
        reason="TODO: example is broken!", raises=ImportError, strict=True
    ),
    "milatools_usage_report.py": pytest.mark.xfail(
        reason="TODO: example is broken!", raises=ImportError, strict=True
    ),
    "trends.py": pytest.mark.xfail(
        reason="TODO: example is broken!", raises=ImportError, strict=True
    ),
    "usage_stats.py": pytest.mark.xfail(
        reason="TODO: example is broken!", raises=ImportError, strict=True
    ),
}

IN_GITHUB_CI = "GITHUB_ACTIONS" in os.environ


def test_all_marks_are_for_existing_examples():
    """Check that the marks are for existing examples.

    This is to avoid marks becoming stale when examples are removed.
    """
    for example_name in example_marks.keys():
        assert example_name in examples


@pytest.mark.skipif(
    IN_GITHUB_CI,
    reason="Examples run with real SARC data and can't be run in GitHub CI.",
)
@pytest.mark.parametrize(
    "example",
    [
        pytest.param(example, marks=example_marks.get(example, ()))
        for example in examples
    ],
)
def test_run_example(example: str, capsys: pytest.CaptureFixture[str]):
    # # Importing the module might run it if it doesn't have a 'main' function and if __name__ == "__main__" block.
    # module = importlib.import_module(f"examples.{example.removesuffix('.py')}")
    # # If it has a 'main' function, call it.
    # if hasattr(module, "main"):
    #     module.main()
    # Test the example by running it as the user would, from the command-line.
    output = subprocess.check_output(
        "uv run examples/" + example,
        shell=True,
    )
    assert output
