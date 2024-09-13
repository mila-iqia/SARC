import pytest

from sarc.client.job import get_available_clusters


@pytest.mark.usefixtures("read_only_db_with_users_client")
def test_get_clusters():
    clusters = list(get_available_clusters())
    assert len(clusters) >= 3
    cluster_names = {cluster.cluster_name for cluster in clusters}
    assert "fromage" in cluster_names
    assert "raisin" in cluster_names
    assert "patate" in cluster_names
