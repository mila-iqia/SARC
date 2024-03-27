import pytest

from sarc.config import config


@pytest.mark.usefixtures("standard_config")
def test_clusterconfig_node_to_gpu():
    cluster_config = config().clusters["raisin_no_prometheus"]
    mapping = cluster_config.node_to_gpu

    result = mapping["cn-c018"]
    assert result in cluster_config.gpus
    assert (
        mapping._harmonize_gpu(f"{cluster_config.gpus[0]}_suffix")
        == cluster_config.gpus[0]
    )
