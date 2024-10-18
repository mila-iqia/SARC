import json
import pathlib
from typing import List

import pytest

from sarc.cli.acquire.rgus import _gpu_type_to_rgu_cache_key, fetch_gpu_type_to_rgu
from sarc.client.rgu import RGUBilling, get_cluster_rgus
from sarc.config import config


@pytest.mark.usefixtures("empty_read_write_db")
def test_acquire_rgus(cli_main):
    # Make sure config folder exists
    cfg = config()
    cache_path: pathlib.Path = cfg.cache
    cache_path.mkdir(parents=True)

    # Test fetch_gpu_type_to_rgu
    _save_rgus(
        cache_path, "raisin", [{"rgu_start_date": "2024-01-01", "gpu_to_rgu": {"a": 1}}]
    )
    assert fetch_gpu_type_to_rgu("raisin") == [
        {
            "rgu_start_date": "2024-01-01",
            "gpu_to_rgu": {"a": 1},
        }
    ]

    # Test `sarc acquire rgus`
    assert get_cluster_rgus("raisin") == []
    assert (
        cli_main(
            [
                "acquire",
                "rgus",
            ]
        )
        == 0
    )
    expected_billing = RGUBilling(
        cluster_name="raisin", rgu_start_date="2024-01-01", gpu_to_rgu={"a": 1}
    )
    assert_same_billings(get_cluster_rgus("raisin"), [expected_billing])

    # Test again
    assert (
        cli_main(
            [
                "acquire",
                "rgus",
            ]
        )
        == 0
    )
    assert_same_billings(get_cluster_rgus("raisin"), [expected_billing])

    # Update existing billing and test
    _save_rgus(
        cache_path, "raisin", [{"rgu_start_date": "2024-01-01", "gpu_to_rgu": {"a": 2}}]
    )
    assert (
        cli_main(
            [
                "acquire",
                "rgus",
            ]
        )
        == 0
    )
    expected_billing.gpu_to_rgu["a"] = 2
    assert_same_billings(get_cluster_rgus("raisin"), [expected_billing])

    # Add new billing and test
    _save_rgus(
        cache_path,
        "raisin",
        [
            {"rgu_start_date": "2024-01-01", "gpu_to_rgu": {"a": 2}},
            {"rgu_start_date": "2024-01-02", "gpu_to_rgu": {"b": 1}},
        ],
    )
    assert (
        cli_main(
            [
                "acquire",
                "rgus",
            ]
        )
        == 0
    )
    expected_billings = [
        expected_billing,
        RGUBilling(
            cluster_name="raisin", rgu_start_date="2024-01-02", gpu_to_rgu={"b": 1}
        ),
    ]
    assert_same_billings(get_cluster_rgus("raisin"), expected_billings)

    # Add new billing for another cluster and test
    _save_rgus(
        cache_path,
        "patate",
        [
            {"rgu_start_date": "2024-01-03", "gpu_to_rgu": {"c": 1}},
        ],
    )
    assert get_cluster_rgus("patate") == []
    assert (
        cli_main(
            [
                "acquire",
                "rgus",
            ]
        )
        == 0
    )
    # Result should be the same for cluster raisin
    assert_same_billings(get_cluster_rgus("raisin"), expected_billings)
    # And we should now get a billing for cluster patate
    assert_same_billings(
        get_cluster_rgus("patate"),
        [
            RGUBilling(
                cluster_name="patate", rgu_start_date="2024-01-03", gpu_to_rgu={"c": 1}
            )
        ],
    )


def assert_same_billings(given: List[RGUBilling], expected: List[RGUBilling]):
    assert len(given) == len(expected)
    for given_billing, expected_billing in zip(given, expected):
        assert given_billing.rgu_start_date == expected_billing.rgu_start_date
        assert given_billing.gpu_to_rgu == expected_billing.gpu_to_rgu


def _save_rgus(cache_path: pathlib.Path, cluster_name: str, mappings: list):
    parent_path = cache_path / "rgu"
    file_path = parent_path / _gpu_type_to_rgu_cache_key(cluster_name)
    parent_path.mkdir(exist_ok=True)
    with file_path.open("w") as file:
        json.dump(mappings, file)
