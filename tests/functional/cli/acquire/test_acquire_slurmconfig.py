import io
import re
from datetime import datetime, time
from pathlib import Path
from typing import List

import pytest
from hostlist import expand_hostlist

from sarc.cache import CacheException
from sarc.cli.acquire.slurmconfig import InconsistentGPUBillingError, SlurmConfigParser
from sarc.client.gpumetrics import GPUBilling, get_cluster_gpu_billings
from sarc.config import MTL, config
from sarc.jobs.node_gpu_mapping import NodeGPUMapping, get_node_to_gpu
from tests.functional.jobs.test_func_load_job_series import MOCK_TIME

SLURM_CONF_RAISIN_2020_01_01 = """
NodeName=mynode[1,2,5-20,30,40-43] UselessParam=UselessValue Gres=gpu1

# No Gres for this node, should be totally ignored
NodeName=cpu_node[1-100] UselessParam=UselessValue

NodeName=myothernode[1,2,5-20,30,40-43] UselessParam=UselessValue Gres=gpu2
NodeName=myothernode2[1,2,5-20,30,50] UselessParam=UselessValue Gres=gpu2,gpu:gpu1:5

# Node present in a GPU partition below, but
# GPUs `bad_named_gpu` and `gpu:what;2` are not billed in partitions.
# thus should be ignored. \\
NodeName=myothernode20 Gres=bad_named_gpu,gpu:what:2

# Node with a GPU, but not present in GPU partitions below. GPU should be ignored.
NodeName=myothernode[100-102] Gres=gpu3

NodeName=alone_node UselessParam=UselessValue Gres=gpu:gpu2:1,gpu:gpu1:9


PartitionName=partition1 \\
Nodes=mynode[1,5,6,29-41] \\
TRESBillingWeights=x=1,GRES/gpu=5000,y=2
PartitionName=partition2 Nodes=mynode[2,8-11,42] \\
TRESBillingWeights=x=1,GRES/gpu:gpu1=5000,y=2
PartitionName=partition3 Nodes=myothernode[10,20] TRESBillingWeights=GRES/gpu:gpu2=7500

# No gres specified, but billing for node GPUs (gpu:gpu2:1,gpu:gpu1:9) should be inferred
# thanks to GPU billings parsed in partitions above (gpu1 and gpu2).
PartitionName=partition4 Nodes=alone_node
"""


SLURM_CONF_RAISIN_2020_05_01 = """
NodeName=mynode[1,2,5-20,30,40-43] UselessParam=UselessValue Gres=gpu1

# No Gres for this node, should be totally ignored
NodeName=cpu_node[1-100] UselessParam=UselessValue

NodeName=myothernode[1,2,5-20,30,40-43] UselessParam=UselessValue Gres=gpu2
NodeName=myothernode2[1,2,5-20,30,50] UselessParam=UselessValue Gres=gpu2,gpu:gpu1:5

# Node present in a GPU partition below, but
# GPUs `bad_named_gpu` and `gpu:what;2` are not billed in partitions.
# thus should be ignored.
NodeName=myothernode20 Gres=bad_named_gpu,gpu:what:2

# Node with a GPU, but not present in GPU partitions below. GPU should be ignored.
NodeName=myothernode[100-102] Gres=gpu3

# Node retired in this config. Neither node nor node GPUs should appear in parsed data.
# NodeName=alone_node UselessParam=UselessValue Gres=gpu:gpu2:1,gpu:gpu1:9


PartitionName=partition1 Nodes=mynode[1,5,6,29-41] TRESBillingWeights=x=1,GRES/gpu=4000,y=2
PartitionName=partition2 Nodes=mynode[2,8-11,42] TRESBillingWeights=x=1,GRES/gpu:gpu1=4000,y=2
PartitionName=partition3 Nodes=myothernode[10,20] TRESBillingWeights=GRES/gpu:gpu2=9000

# No gres specified, but billing for node GPUs (gpu:gpu2:1,gpu:gpu1:9) should be inferred
# thanks to GPU billings parsed in partitions above (gpu1 and gpu2).
PartitionName=partition4 Nodes=alone_node
"""


def _setup_logging_do_nothing(*args, **kwargs):
    """
    Mock used to deactivate sarc.logging.setupLogging when using caplog.

    caplog adds a logging handler but this handler is deleted by
    sarc.logging.setupLogging and replaced with custom handlers.

    As we just want to check logging messages, we can simply remove
    call to setupLogging.
    """


@pytest.mark.usefixtures("empty_read_write_db", "enabled_cache")
def test_acquire_slurmconfig(cli_main, caplog, monkeypatch):
    monkeypatch.setattr("sarc.cli.setupLogging", _setup_logging_do_nothing)

    assert get_cluster_gpu_billings("raisin") == []
    assert get_node_to_gpu("raisin") == None

    _save_slurm_conf("raisin", "2020-01-01", SLURM_CONF_RAISIN_2020_01_01)

    with pytest.raises(KeyError) as exc_info:
        cli_main(["acquire", "slurmconfig", "-c", "unknown_raisin", "-d", "2020-01-01"])
        assert str(exc_info.value) == "unknown_raisin"

    with pytest.raises(CacheException):
        cli_main(["acquire", "slurmconfig", "-c", "raisin", "-d", "1999-01-01"])

    assert (
        cli_main(["-v", "acquire", "slurmconfig", "-c", "raisin", "-d", "2020-01-01"])
        == 0
    )

    # No harmonization available for gpu1
    assert re.search(
        r"WARNING +sarc\.cli\.acquire\.slurmconfig:slurmconfig\.py:[0-9]+ \[raisin]\[partition2] +"
        r"Cannot harmonize: gpu1 \(keep this name as-is\) : mynode\[2,8-11,42]",
        caplog.text,
    )

    expected_gpu_billing_1 = GPUBilling(
        cluster_name="raisin",
        since="2020-01-01",
        gpu_to_billing={
            "gpu1": 5000,
            "THE GPU II": 7500,
        },
    )
    assert_same_billings(get_cluster_gpu_billings("raisin"), [expected_gpu_billing_1])

    expected_node_to_gpu_1 = NodeGPUMapping(
        cluster_name="raisin",
        since="2020-01-01",
        node_to_gpu={
            **{
                node_name: ["gpu1"]
                for node_name in expand_hostlist("mynode[1,2,5-20,30,40-43]")
            },
            **{
                node_name: ["gpu2"]
                for node_name in expand_hostlist("myothernode[1,2,5-20,30,40-43]")
            },
            **{
                node_name: ["gpu2", "gpu:gpu1:5"]
                for node_name in expand_hostlist("myothernode2[1,2,5-20,30,50]")
            },
            "myothernode20": ["bad_named_gpu", "gpu:what:2"],
            **{
                node_name: ["gpu3"]
                for node_name in expand_hostlist("myothernode[100-102]")
            },
            "alone_node": ["gpu:gpu2:1", "gpu:gpu1:9"],
        },
    )
    assert_same_node_gpu_mapping(get_node_to_gpu("raisin"), expected_node_to_gpu_1)

    # Save next conf file
    _save_slurm_conf("raisin", "2020-05-01", SLURM_CONF_RAISIN_2020_05_01)
    assert (
        cli_main(["-v", "acquire", "slurmconfig", "-c", "raisin", "-d", "2020-05-01"])
        == 0
    )
    expected_gpu_billing_2 = GPUBilling(
        cluster_name="raisin",
        since="2020-05-01",
        gpu_to_billing={
            "gpu1": 4000,
            "THE GPU II": 9000,
        },
    )
    assert_same_billings(
        get_cluster_gpu_billings("raisin"),
        [expected_gpu_billing_1, expected_gpu_billing_2],
    )

    expected_node_to_gpu_2 = NodeGPUMapping(
        cluster_name="raisin",
        since="2020-05-01",
        node_to_gpu=expected_node_to_gpu_1.node_to_gpu.copy(),
    )
    del expected_node_to_gpu_2.node_to_gpu["alone_node"]
    assert_same_node_gpu_mapping(get_node_to_gpu("raisin"), expected_node_to_gpu_2)

    # Check that we get the right node_to_gpu for a given date
    def _parse_date(value: str):
        return datetime.combine(datetime.fromisoformat(value), time.min).replace(
            tzinfo=MTL
        )

    assert_same_node_gpu_mapping(
        get_node_to_gpu("raisin", _parse_date("2019-12-01")), expected_node_to_gpu_1
    )
    assert_same_node_gpu_mapping(
        get_node_to_gpu("raisin", _parse_date("2020-01-01")), expected_node_to_gpu_1
    )
    assert_same_node_gpu_mapping(
        get_node_to_gpu("raisin", _parse_date("2020-03-07")), expected_node_to_gpu_1
    )
    assert_same_node_gpu_mapping(
        get_node_to_gpu("raisin", _parse_date("2020-05-01")), expected_node_to_gpu_2
    )
    assert_same_node_gpu_mapping(
        get_node_to_gpu("raisin", _parse_date("2020-05-20")), expected_node_to_gpu_2
    )
    assert_same_node_gpu_mapping(
        get_node_to_gpu("raisin", _parse_date("2020-10-10")), expected_node_to_gpu_2
    )


SLURM_CONF_RAISIN_2020_01_01_INCONSISTENT_BILLING = """
NodeName=mynode[1,2,5-20,30,40-43] UselessParam=UselessValue Gres=gpu1

PartitionName=partition1 Nodes=mynode[1,5,6,29-41] TRESBillingWeights=x=1,GRES/gpu=5000,y=2
PartitionName=partition2 Nodes=mynode[2,8-11,42] TRESBillingWeights=x=1,GRES/gpu:gpu1=6000,y=2
"""


@pytest.mark.parametrize("threshold", [None, 0.1, 1, 10, 19])
@pytest.mark.usefixtures("empty_read_write_db", "enabled_cache")
def test_acuire_slurmconfig_inconsistent_billing(cli_main, threshold):
    _save_slurm_conf(
        "raisin",
        "2020-01-01",
        SLURM_CONF_RAISIN_2020_01_01_INCONSISTENT_BILLING,
    )

    command = ["acquire", "slurmconfig", "-c", "raisin", "-d", "2020-01-01"]
    if threshold is not None:
        threshold = float(threshold)
        command += ["--threshold", str(threshold)]
    with pytest.raises(InconsistentGPUBillingError) as exc_info:
        cli_main(command)

    assert f"""
GPU billing differs (threshold {threshold or 0.1} %).
GPU name: gpu1
Previous value: 5000.0
From line: 4
PartitionName=partition1 Nodes=mynode[1,5,6,29-41] TRESBillingWeights=x=1,GRES/gpu=5000,y=2

New value: 6000.0
From line: 5
PartitionName=partition2 Nodes=mynode[2,8-11,42] TRESBillingWeights=x=1,GRES/gpu:gpu1=6000,y=2
""" == str(exc_info.value)


@pytest.mark.parametrize("threshold", [20, 20.1, 30])
@pytest.mark.usefixtures("empty_read_write_db", "enabled_cache")
def test_acquire_slurmconfig_inconsistent_billing_success(cli_main, threshold):
    """Test that parsing succeeds with greater threshold"""
    _save_slurm_conf(
        "raisin",
        "2020-01-01",
        SLURM_CONF_RAISIN_2020_01_01_INCONSISTENT_BILLING,
    )
    assert (
        cli_main(
            [
                "acquire",
                "slurmconfig",
                "-c",
                "raisin",
                "-d",
                "2020-01-01",
                "-t",
                str(threshold),
            ]
        )
        == 0
    )
    (gpu_billing,) = get_cluster_gpu_billings("raisin")
    assert gpu_billing.gpu_to_billing == {"gpu1": (5000 + 6000) / 2}


def assert_same_billings(given: List[GPUBilling], expected: List[GPUBilling]):
    assert len(given) == len(expected)
    for given_billing, expected_billing in zip(given, expected):
        assert given_billing.since == expected_billing.since
        assert given_billing.gpu_to_billing == expected_billing.gpu_to_billing


def assert_same_node_gpu_mapping(
    given_billing: NodeGPUMapping, expected_billing: NodeGPUMapping
):
    assert given_billing.since == expected_billing.since
    assert given_billing.node_to_gpu == expected_billing.node_to_gpu


def _save_slurm_conf(cluster_name: str, day: str, content: str):
    scp = SlurmConfigParser(config().clusters[cluster_name], day)
    folder = "slurm_conf"
    filename = scp._cache_key()
    cache_dir = config().cache
    file_dir = cache_dir / folder
    file_dir.mkdir(parents=True, exist_ok=True)
    file_path = file_dir / filename
    with file_path.open("w") as file:
        file.write(content)


@pytest.mark.usefixtures("enabled_cache")
@pytest.mark.freeze_time(MOCK_TIME)
def test_download_cluster_config(test_config, remote):
    """Test slurm conf file downloading."""

    clusters = test_config.clusters
    # Check default value for "slurm_conf_host_path" (with cluster raisina)
    assert clusters["raisin"].slurm_conf_host_path == Path("/etc/slurm/slurm.conf")
    # Check custom value for "slurm_conf_host_path" (with cluster patate)
    assert clusters["patate"].slurm_conf_host_path == Path("/the/path/to/slurm.conf")

    # Use cluster patate for download test
    cluster = clusters["patate"]
    scp = SlurmConfigParser(cluster)

    file_dir = test_config.cache / "slurm_conf"
    file_dir.mkdir(parents=True, exist_ok=True)
    file_path = file_dir / scp._cache_key()

    # Slurm conf file should not yet exist
    assert not file_path.exists()

    # Get conf file
    expected_content = SLURM_CONF_RAISIN_2020_01_01
    remote.expect(
        host=cluster.host,
        cmd=f"cat {cluster.slurm_conf_host_path}",
        out=expected_content.encode(),
    )
    scp.get_slurm_config()

    # Now, slurm file should exist
    assert file_path.is_file()
    with file_path.open() as file:
        assert file.read() == expected_content


def test_file_lines():
    """
    Test that we correctly parse the file lines,
    merging lines split with "\".
    """
    content = """
line 1
line 2

line 4
line 5\\
line 6 \\
line 7   \\
line 8

line 10\\
line 11

# line 13
# line 14 \\
# line 15 \\
# line 16
line 17
line 18
"""
    with io.StringIO(content) as file:
        lines = [(i, line) for i, line in SlurmConfigParser._file_lines(file)]
    assert lines == [
        (0, ""),
        (1, "line 1"),
        (2, "line 2"),
        (3, ""),
        (4, "line 4"),
        (5, "line 5 line 6  line 7    line 8"),
        (9, ""),
        (10, "line 10 line 11"),
        (12, ""),
        (13, "# line 13"),
        (14, "# line 14 \\"),
        (15, "# line 15 \\"),
        (16, "# line 16"),
        (17, "line 17"),
        (18, "line 18"),
    ]
