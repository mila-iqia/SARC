import pytest

from sarc.config import config


@pytest.mark.usefixtures("standard_config")
def test_clusterconfig_node_to_gpu():
    cluster_config = config().clusters["raisin_no_prometheus"]
    mapping = cluster_config.node_to_gpu

    nodename = "cn-c018"
    result = mapping[nodename]
    assert result == cluster_config.gpus_per_nodes[nodename]["asupergpu"]
