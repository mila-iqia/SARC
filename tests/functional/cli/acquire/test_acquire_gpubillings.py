import json
import pathlib
from typing import List

import pytest

from sarc.cli.acquire.gpubillings import (
    _gpu_type_to_billing_cache_key,
    fetch_gpu_type_to_billing,
)
from sarc.client.gpumetrics import GPUBilling, get_cluster_gpu_billings
from sarc.config import config


@pytest.mark.usefixtures("empty_read_write_db")
def test_acquire_gpubillings(cli_main):
    # Make sure config folder exists
    cfg = config()
    cache_path: pathlib.Path = cfg.cache
    cache_path.mkdir(parents=True)

    # Test fetch_gpu_type_to_billing
    _save_gpubillings(
        cache_path,
        "raisin",
        [{"billing_start_date": "2024-01-01", "gpu_to_billing": {"a": 1}}],
    )
    assert fetch_gpu_type_to_billing("raisin") == [
        {
            "billing_start_date": "2024-01-01",
            "gpu_to_billing": {"a": 1},
        }
    ]

    # Test `sarc acquire gpubillings`
    assert get_cluster_gpu_billings("raisin") == []
    assert (
        cli_main(
            [
                "acquire",
                "gpubillings",
            ]
        )
        == 0
    )
    expected_billing = GPUBilling(
        cluster_name="raisin", billing_start_date="2024-01-01", gpu_to_billing={"a": 1}
    )
    assert_same_billings(get_cluster_gpu_billings("raisin"), [expected_billing])

    # Test again
    assert (
        cli_main(
            [
                "acquire",
                "gpubillings",
            ]
        )
        == 0
    )
    assert_same_billings(get_cluster_gpu_billings("raisin"), [expected_billing])

    # Update existing billing and test
    _save_gpubillings(
        cache_path,
        "raisin",
        [{"billing_start_date": "2024-01-01", "gpu_to_billing": {"a": 2}}],
    )
    assert (
        cli_main(
            [
                "acquire",
                "gpubillings",
            ]
        )
        == 0
    )
    expected_billing.gpu_to_billing["a"] = 2
    assert_same_billings(get_cluster_gpu_billings("raisin"), [expected_billing])

    # Add new billing and test
    _save_gpubillings(
        cache_path,
        "raisin",
        [
            {"billing_start_date": "2024-01-01", "gpu_to_billing": {"a": 2}},
            {"billing_start_date": "2024-01-02", "gpu_to_billing": {"b": 1}},
        ],
    )
    assert (
        cli_main(
            [
                "acquire",
                "gpubillings",
            ]
        )
        == 0
    )
    expected_billings = [
        expected_billing,
        GPUBilling(
            cluster_name="raisin",
            billing_start_date="2024-01-02",
            gpu_to_billing={"b": 1},
        ),
    ]
    assert_same_billings(get_cluster_gpu_billings("raisin"), expected_billings)

    # Add new billing for another cluster and test
    _save_gpubillings(
        cache_path,
        "patate",
        [
            {"billing_start_date": "2024-01-03", "gpu_to_billing": {"c": 1}},
        ],
    )
    assert get_cluster_gpu_billings("patate") == []
    assert (
        cli_main(
            [
                "acquire",
                "gpubillings",
            ]
        )
        == 0
    )
    # Result should be the same for cluster raisin
    assert_same_billings(get_cluster_gpu_billings("raisin"), expected_billings)
    # And we should now get a billing for cluster patate
    assert_same_billings(
        get_cluster_gpu_billings("patate"),
        [
            GPUBilling(
                cluster_name="patate",
                billing_start_date="2024-01-03",
                gpu_to_billing={"c": 1},
            )
        ],
    )


def assert_same_billings(given: List[GPUBilling], expected: List[GPUBilling]):
    assert len(given) == len(expected)
    for given_billing, expected_billing in zip(given, expected):
        assert given_billing.billing_start_date == expected_billing.billing_start_date
        assert given_billing.gpu_to_billing == expected_billing.gpu_to_billing


def _save_gpubillings(cache_path: pathlib.Path, cluster_name: str, mappings: list):
    parent_path = cache_path / "gpu_billing"
    file_path = parent_path / _gpu_type_to_billing_cache_key(cluster_name)
    parent_path.mkdir(exist_ok=True)
    with file_path.open("w") as file:
        json.dump(mappings, file)
