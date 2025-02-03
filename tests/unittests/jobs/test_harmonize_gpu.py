import pytest

from sarc.config import DEFAULTS_FLAG, MIG_FLAG, ClusterConfig, config

GPUS_PER_NODES = {
    "node[0-9]": {"gpu1": "DESCRIPTIVE GPU 1"},
    "node[9-19]": {"gpu2": "DESCRIPTIVE GPU 2"},
    "node_mig20": {"gpu3": "DESCRIPTIVE GPU 3", "4g.40gb": f"{MIG_FLAG}gpu3"},
    DEFAULTS_FLAG: {"gpu_default": "DESCRIPTIVE GPU DEFAULT"},
}


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
            "GPU1",
            "DESCRIPTIVE GPU 1",
            GPUS_PER_NODES,
        ],
        [
            "node11",
            "GPU2",
            "DESCRIPTIVE GPU 2",
            GPUS_PER_NODES,
        ],
        [
            "DoesNotExist",
            "GPU_DEFAULT",
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
def test_harmonize_gpu(node, gpu_type, expected, gpus_per_nodes):
    cluster = ClusterConfig(timezone="America/Montreal", gpus_per_nodes=gpus_per_nodes)
    assert cluster.harmonize_gpu(node, gpu_type) == expected


@pytest.mark.usefixtures("standard_config")
@pytest.mark.parametrize(
    "node,gpu_type,expected",
    [
        ("cn-c018", "asupergpu", "Nec Plus Plus ULTRA GPU 2000"),
        ("cn-c019", "asupergpu", "Nec Plus ULTRA GPU 2000"),
        ("cn-c024", "asupergpu", "Nec Plus ULTRA GPU 2000"),
    ],
)
def test_clusterconfig_harmonize_gpu(node, gpu_type, expected):
    cluster = config().clusters["raisin_no_prometheus"]
    assert cluster.harmonize_gpu(node, gpu_type) == expected
