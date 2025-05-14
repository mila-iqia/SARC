from pathlib import Path

import gifnoc
import pytest

from sarc.config import config

sarc_configs = Path(__file__).parent.parent.parent / "config"


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


def test_dev_config():
    with gifnoc.use(Path(sarc_configs / "sarc-dev.yaml")):
        assert config().mongo.database_name == "sarc-dev"


def test_client_config():
    with gifnoc.use(Path(sarc_configs / "sarc-client.yaml")):
        assert config().mongo.database_name == "sarc"


def test_prod_config():
    with gifnoc.use(Path(sarc_configs / "sarc-prod.yaml")):
        assert config().mongo.database_name == "sarc"
