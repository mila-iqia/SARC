import re
from datetime import datetime
from unittest.mock import MagicMock

import pytest

from sarc.alerts.usage_alerts.prometheus_gpu_types import (
    check_prometheus_vs_slurmconfig,
)
from sarc.config import config

TESTING_DATA = {
    "00_hyrule": {
        "cluster": "hyrule",
        "message": "No node_to_gpu, so no slurm config data: should complain.",
        "node_to_gpu": {},
        "prometheus": [],
    },
    "01_gerudo": {
        "cluster": "gerudo",
        "message": "No node_to_gpu, so no slurm config data: should complain.",
        "node_to_gpu": {},
        "prometheus": [{"metric": {"gpu_type": "phantom_gpu"}}],
    },
    "10_patate": {
        "cluster": "patate",
        "message": "No prometheus data to check: no warning.",
        "node_to_gpu": {"node0": "gpu0"},
        "prometheus": [],
    },
    "11_0_fromage": {
        "cluster": "fromage",
        "message": "Both slurm config data and prometheus data available, but prometheus GPU not in slurm config: warning.",
        "node_to_gpu": {"node0": "gpu0"},
        "prometheus": [{"metric": {"gpu_type": "phantom_gpu"}}],
    },
    "11_1_raisin": {
        "cluster": "raisin",
        "message": "Both slurm config data and prometheus data available, and prometheus GPU in slurm config: no warning.",
        "node_to_gpu": {"node0": "phantom_gpu"},
        "prometheus": [{"metric": {"gpu_type": "phantom_gpu"}}],
    },
}


@pytest.mark.usefixtures("empty_read_write_db", "tzlocal_is_mtl")
@pytest.mark.parametrize("params", TESTING_DATA.values(), ids=TESTING_DATA.keys())
def test_check_prometheus_vs_slurmconfig(params, monkeypatch, caplog, file_regression):
    """Test each case from TEST_DATA (one test per cluster)."""

    from prometheus_api_client import PrometheusConnect

    # Mock PrometheusConnect.custom_query() to prevent a real call to Prometheus
    monkeypatch.setattr(
        PrometheusConnect,
        "custom_query",
        MagicMock(return_value=params["prometheus"]),
    )
    # Add node_to_gpu entry in db if necessary
    if params["node_to_gpu"]:
        db = config().mongo.database_instance
        db.node_gpu_mapping.insert_one(
            {
                "cluster_name": params["cluster"],
                "since": datetime.now(),
                "node_to_gpu": params["node_to_gpu"],
            }
        )

    check_prometheus_vs_slurmconfig(cluster_name=params["cluster"])
    file_regression.check(
        params["message"]
        + "\n\n"
        + re.sub(
            r"WARNING +sarc\.alerts\.usage_alerts\.prometheus_gpu_types:prometheus_gpu_types.py:[0-9]+ +",
            "",
            caplog.text,
        )
    )


@pytest.mark.usefixtures("empty_read_write_db", "tzlocal_is_mtl")
def test_check_prometheus_vs_slurmconfig_all(monkeypatch, caplog, file_regression):
    """Test all data at once (all clusters)."""

    from prometheus_api_client import PrometheusConnect

    def _gen_fake_custom_query(self_, query_: str):
        return [
            prom_data
            for params in TESTING_DATA.values()
            if params["cluster"] in query_
            for prom_data in params["prometheus"]
        ]

    monkeypatch.setattr(
        PrometheusConnect,
        "custom_query",
        _gen_fake_custom_query,
    )

    db = config().mongo.database_instance
    collection = db.node_gpu_mapping
    for params in TESTING_DATA.values():
        if params["node_to_gpu"]:
            collection.insert_one(
                {
                    "cluster_name": params["cluster"],
                    "since": datetime.now(),
                    "node_to_gpu": params["node_to_gpu"],
                }
            )

    check_prometheus_vs_slurmconfig()
    file_regression.check(
        re.sub(
            r"WARNING +sarc\.alerts\.usage_alerts\.prometheus_gpu_types:prometheus_gpu_types.py:[0-9]+ +",
            "",
            caplog.text,
        )
    )
