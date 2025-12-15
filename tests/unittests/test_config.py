from pathlib import Path
from unittest.mock import patch

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
    mock_mongo_content = """
    connection_string: mongodb://localhost:27017/sarc-test
    database_name: sarc-test
    """

    # Create a selective mock for read_text that returns our content
    original_read_text = Path.read_text

    def mock_read_text_selective(path_obj, *args, **kwargs):
        if "mongo-prod.yaml" in str(path_obj):
            return mock_mongo_content
        if "slack-prod.yaml" in str(path_obj):
            return "null"
        return original_read_text(path_obj, *args, **kwargs)

    # Create a selective mock for exists that returns True for our file
    original_exists = Path.exists

    def mock_exists_selective(path_obj):
        if "mongo-prod.yaml" in str(path_obj) or "slack-prod.yaml" in str(path_obj):
            return True
        return original_exists(path_obj)

    # Apply both patches
    with (
        patch.object(Path, "read_text", mock_read_text_selective),
        patch.object(Path, "exists", mock_exists_selective),
    ):
        with gifnoc.use(Path(sarc_configs / "sarc-prod.yaml")):
            assert config().mongo.database_name == "sarc-test"
