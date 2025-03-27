import pytest

from sarc.client.job import get_available_clusters


@pytest.mark.usefixtures("read_only_db_with_users_client")
def test_get_clusters():
    clusters = list(get_available_clusters())
    assert len(clusters) >= 3
    cluster_by_name = {cluster.cluster_name: cluster for cluster in clusters}
    assert "fromage" in cluster_by_name
    assert "raisin" in cluster_by_name
    assert "patate" in cluster_by_name
    assert "mila" in cluster_by_name

    assert cluster_by_name["fromage"].billing_is_gpu is False
    assert cluster_by_name["raisin"].billing_is_gpu is False
    assert cluster_by_name["patate"].billing_is_gpu is False
    assert cluster_by_name["mila"].billing_is_gpu is True
