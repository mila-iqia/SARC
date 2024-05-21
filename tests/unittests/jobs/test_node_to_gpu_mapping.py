import re

import pytest

from sarc.jobs.node_gpu_mapping import (
    DEFAULTS_FLAG,
    MIG_FLAG,
    NodeToGPUMapping,
    _expand_list,
    _find_pattern,
    expand_patterns,
)

GPUS_PER_NODES = {
    "node{{[0-9]}}": {"gpu1": "DESCRIPTIVE GPU 1"},
    "node{{[9-19]}}": {"gpu2": "DESCRIPTIVE GPU 2"},
    "node_mig20": {"gpu3": "DESCRIPTIVE GPU 3", "[0-9]+g\.[0-9]+gb": f"{MIG_FLAG}gpu3"},
    DEFAULTS_FLAG: {"gpu_default": "DESCRIPTIVE GPU DEFAULT"},
}


@pytest.mark.parametrize(
    "pattern,expected",
    [
        ["{{}}{{}}", ("{{}}", "")],
        ["{{pattern}}", ("{{pattern}}", "pattern")],
        ["{{pattern1}}something{{pattern2}}", ("{{pattern1}}", "pattern1")],
    ],
)
def test__find_pattern(pattern, expected):
    assert _find_pattern(pattern) == expected


def test__expand_list():
    start = 9
    stop = 19
    pattern = "{{[9-19]}}"
    expected = f"({'|'.join([f'0*{i}' for i in range(start, stop + 1)])})"

    _, pattern = _find_pattern(pattern)

    assert _expand_list(pattern) == expected

    for i in (start - 1, stop + 1):
        assert re.match(expected, "0" * int(start / 2) + str(i)) is None
        assert re.match(expected, str(i)) is None

    for i in range(start, stop + 1):
        assert re.match(expected, "0" * int(start / 2) + str(i))
        assert re.match(expected, str(i))


@pytest.mark.parametrize(
    "string,expected,match",
    [
        [
            "prefix {{[9-11]}}__{{[11-13]}} suffix",
            re.compile(
                f"prefix {_expand_list('[9-11]')}__{_expand_list('[11-13]')} suffix"
            ),
            "prefix 10__11 suffix",
        ],
        ["{{[9-11]}}{{DoesNotExist}}", None, None],
    ],
)
def test_expand_patterns(string, expected, match):
    if expected is None:
        with pytest.raises(ValueError):
            expand_patterns(string)
        return

    regex = expand_patterns(string)
    assert regex.pattern == expected.pattern
    assert regex.match(match)


@pytest.mark.parametrize(
    "node,gpu_type,expected,gpus_per_nodes",
    [
        [
            "DoesNotExist",
            "DoesNotExist",
            None,
            {},
        ],
        [
            "node1",
            "prefix GPU1:suffix",
            "DESCRIPTIVE GPU 1",
            GPUS_PER_NODES,
        ],
        [
            "node11",
            "prefix GPU2:suffix",
            "DESCRIPTIVE GPU 2",
            GPUS_PER_NODES,
        ],
        [
            "DoesNotExist",
            "prefix GPU_DEFAULT:suffix",
            "DESCRIPTIVE GPU DEFAULT",
            GPUS_PER_NODES,
        ],
        [
            "node1",
            "DoesNotExist",
            None,
            GPUS_PER_NODES,
        ],
        [
            "node_mig20",
            "4g.40gb",
            "DESCRIPTIVE GPU 3 : 4g.40gb",
            GPUS_PER_NODES,
        ],
    ],
)
def test_node_to_gpu_mapping(node, gpu_type, expected, gpus_per_nodes):
    mapping = NodeToGPUMapping("cluster", None, gpus_per_nodes)

    assert mapping._harmonize_gpu(node, gpu_type) == expected
