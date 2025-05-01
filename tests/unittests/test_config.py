import pytest

from sarc.config import config


# @pytest.mark.usefixtures("standard_config")
def test_cluster_config_billing_is_gpu():
    clusters = config().clusters
    cluster_mila = clusters.pop("mila")
    assert clusters

    assert cluster_mila.billing_is_gpu is True

    for cluster in clusters.values():
        assert cluster.billing_is_gpu is False


@pytest.mark.usefixtures("client_mode")
def test_client_block():
    with pytest.raises(Exception, match="only accessible with SARC_MODE=scraping"):
        config().clusters
