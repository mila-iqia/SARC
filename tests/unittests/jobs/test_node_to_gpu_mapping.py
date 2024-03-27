import pytest

from sarc.jobs.node_gpu_mapping import NodeToGPUMapping


@pytest.mark.parametrize(
    "gpu_type,expected,harmonize_gpu_map,gpus",
    [
        [
            "DoesNotExist",
            None,
            {},
            [],
        ],
        [
            "prefix GPU1:suffix",
            "gpu1",
            {},
            ["gpu1", "gpu2"],
        ],
        [
            "prefix GPU2 suffix",
            "gpu2",
            {},
            ["gpu1", "gpu2"],
        ],
        [
            "prefix GPU1_suffix",
            "gpu1",
            {".*gpu1_suffix.*": "gpu1"},
            ["gpu1", "gpu2"],
        ],
    ],
)
def test_node_to_gpu_mapping(gpu_type, expected, harmonize_gpu_map, gpus):
    mapping = NodeToGPUMapping("cluster", None, harmonize_gpu_map, gpus)

    assert mapping._harmonize_gpu(gpu_type) == expected
