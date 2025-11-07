import io
import logging
import re
from datetime import datetime, time
from typing import List
from zipfile import ZipFile

import pytest
from fabric.testing.base import Session, Command
from hostlist import expand_hostlist

from sarc.cache import Cache, CacheEntry
from sarc.cli.parse.slurmconfig import InconsistentGPUBillingError, SlurmConfigParser
from sarc.client.gpumetrics import GPUBilling, get_cluster_gpu_billings
from sarc.config import MTL, UTC
from sarc.jobs.node_gpu_mapping import NodeGPUMapping, get_node_to_gpu

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


DATE_2020_01_01_MTL = datetime(2020, 1, 1, tzinfo=MTL)
DATE_2020_05_01_MTL = datetime(2020, 5, 1, tzinfo=MTL)
DATE_2020_10_01_MTL = datetime(2020, 10, 1, tzinfo=MTL)
DATE_2020_12_01_MTL = datetime(2020, 12, 1, tzinfo=MTL)


@pytest.mark.usefixtures("enabled_cache")
def test_fetch_slurmconfig(cli_main, test_config, remote, caplog, freezer):
    """Test slurm conf file downloading using `fetch slurmconfig`."""
    caplog.set_level(logging.INFO)

    cluster_name = "raisin"

    # Use cluster raisin for download test
    cluster = test_config.clusters[cluster_name]

    cache = Cache(subdirectory=f"slurm_conf/{cluster_name}")
    file_path_2020_01_01 = cache._dir_from_date(
        cache.cache_dir, DATE_2020_01_01_MTL
    ) / DATE_2020_01_01_MTL.astimezone(UTC).time().isoformat("seconds")
    file_path_2020_05_01 = cache._dir_from_date(
        cache.cache_dir, DATE_2020_05_01_MTL
    ) / DATE_2020_05_01_MTL.astimezone(UTC).time().isoformat("seconds")

    assert not file_path_2020_01_01.exists()
    assert not file_path_2020_05_01.exists()

    remote.expect_sessions(
        Session(
            host=cluster.host,
            commands=[
                Command(
                    cmd=f"cat {cluster.slurm_conf_host_path}",
                    out=SLURM_CONF_RAISIN_2020_01_01.encode(),
                ),
                Command(
                    cmd=f"cat {cluster.slurm_conf_host_path}",
                    out=SLURM_CONF_RAISIN_2020_05_01.encode(),
                ),
            ],
        ),
    )

    # Should download from current day
    freezer.move_to(DATE_2020_01_01_MTL)
    assert cli_main(["fetch", "slurmconfig", "-c", "raisin"]) == 0

    # Only file matching current day should exist
    assert file_path_2020_01_01.is_file(), file_path_2020_01_01
    assert not file_path_2020_05_01.is_file(), file_path_2020_05_01
    with ZipFile(file_path_2020_01_01) as zf:
        ((key, blob),) = CacheEntry(zf).items()
        assert key == DATE_2020_01_01_MTL.astimezone(UTC).isoformat()
        assert blob.decode("utf-8") == SLURM_CONF_RAISIN_2020_01_01
    caplog.clear()

    # Now move to another day and download again
    freezer.move_to(DATE_2020_05_01_MTL)
    assert cli_main(["fetch", "slurmconfig", "-c", "raisin"]) == 0

    # Now we must have both files for the two tested days
    assert file_path_2020_01_01.is_file(), file_path_2020_01_01
    assert file_path_2020_05_01.is_file(), file_path_2020_05_01
    with ZipFile(file_path_2020_01_01) as zf:
        ((key, blob),) = CacheEntry(zf).items()
        assert key == DATE_2020_01_01_MTL.astimezone(UTC).isoformat()
        assert blob.decode("utf-8") == SLURM_CONF_RAISIN_2020_01_01
    with ZipFile(file_path_2020_05_01) as zf:
        ((key, blob),) = CacheEntry(zf).items()
        assert key == DATE_2020_05_01_MTL.astimezone(UTC).isoformat()
        assert blob.decode("utf-8") == SLURM_CONF_RAISIN_2020_05_01


@pytest.mark.freeze_time(DATE_2020_01_01_MTL)
@pytest.mark.usefixtures("enabled_cache")
def test_fetch_slurmconfig_no_change(cli_main, test_config, remote, caplog):
    """test fetch slurmconfig when downloaded file is identical to previous cached file"""
    caplog.set_level(logging.INFO)

    cluster_name = "raisin"

    # Use cluster raisin for download test
    cluster = test_config.clusters[cluster_name]

    cache = Cache(subdirectory=f"slurm_conf/{cluster_name}")
    file_path_2020_01_01 = cache._dir_from_date(
        cache.cache_dir, DATE_2020_01_01_MTL
    ) / DATE_2020_01_01_MTL.astimezone(UTC).time().isoformat("seconds")

    # Cache same content in a previous date (2019-01-01(
    prev_date = datetime(2019, 1, 1, tzinfo=MTL).astimezone(UTC)
    _save_slurm_conf(cluster_name, "2019-01-01", SLURM_CONF_RAISIN_2020_01_01)

    remote.expect_sessions(
        Session(
            host=cluster.host,
            commands=[
                Command(
                    cmd=f"cat {cluster.slurm_conf_host_path}",
                    out=SLURM_CONF_RAISIN_2020_01_01.encode(),
                ),
            ],
        ),
    )

    # Should download from current day
    assert not file_path_2020_01_01.exists()
    assert cli_main(["fetch", "slurmconfig", "-c", "raisin"]) == 0
    assert not file_path_2020_01_01.exists()

    assert (
        f"slurm.conf file at {DATE_2020_01_01_MTL.astimezone(UTC)} have not changed since: {prev_date}, skipping."
        in caplog.text
    )


@pytest.mark.freeze_time(DATE_2020_12_01_MTL)
@pytest.mark.usefixtures("enabled_cache")
def test_fetch_slurmconfig_legacy(cli_main, test_config, remote, caplog):
    """test that fetch slurmconfig correctly handles legacy cached files"""
    caplog.set_level(logging.INFO)

    cluster_name = "raisin"

    slurm_conf_dir = test_config.cache / "slurm_conf"
    slurm_conf_dir.mkdir(parents=True, exist_ok=True)

    legacy = [
        (
            slurm_conf_dir / f"slurm.{cluster_name}.2020-01-01.conf",
            SLURM_CONF_RAISIN_2020_01_01,
        ),
        (
            slurm_conf_dir / f"slurm.{cluster_name}.2020-05-01.conf",
            SLURM_CONF_RAISIN_2020_05_01,
        ),
        (
            slurm_conf_dir
            / f"slurm.{cluster_name}.{DATE_2020_10_01_MTL.astimezone(UTC)}.conf",
            SLURM_CONF_RAISIN_2020_05_01,  # same content as previous legacy cache
        ),
    ]
    for legacy_path, legacy_content in legacy:
        with open(legacy_path, "w") as f:
            f.write(legacy_content)

    cache = Cache(subdirectory=f"slurm_conf/{cluster_name}")
    file_path_2020_01_01 = cache._dir_from_date(
        cache.cache_dir, DATE_2020_01_01_MTL
    ) / DATE_2020_01_01_MTL.astimezone(UTC).time().isoformat("seconds")
    file_path_2020_05_01 = cache._dir_from_date(
        cache.cache_dir, DATE_2020_05_01_MTL
    ) / DATE_2020_05_01_MTL.astimezone(UTC).time().isoformat("seconds")
    file_path_2020_10_01 = cache._dir_from_date(
        cache.cache_dir, DATE_2020_10_01_MTL
    ) / DATE_2020_10_01_MTL.astimezone(UTC).time().isoformat("seconds")
    file_path_2020_12_01 = cache._dir_from_date(
        cache.cache_dir, DATE_2020_12_01_MTL
    ) / DATE_2020_12_01_MTL.astimezone(UTC).time().isoformat("seconds")

    # Use cluster raisin for download test
    cluster = test_config.clusters[cluster_name]

    remote.expect_sessions(
        Session(
            host=cluster.host,
            commands=[
                Command(
                    cmd=f"cat {cluster.slurm_conf_host_path}",
                    out=SLURM_CONF_RAISIN_2020_05_01.encode(),  # same content as in latest legacy cache
                ),
            ],
        ),
    )

    # Should download from current day
    assert not file_path_2020_01_01.exists()
    assert not file_path_2020_05_01.exists()
    assert not file_path_2020_10_01.exists()
    assert not file_path_2020_12_01.exists()
    assert cli_main(["fetch", "slurmconfig", "-c", "raisin"]) == 0
    assert file_path_2020_01_01.exists()
    assert file_path_2020_05_01.exists()
    assert not file_path_2020_10_01.exists()  # should be skipped
    assert not file_path_2020_12_01.exists()  # should be skipped

    # Each legacy file should have been seen
    assert (
        f"Legacy cache at {DATE_2020_01_01_MTL.astimezone(UTC)}: slurm.raisin.2020-01-01.conf"
        in caplog.text
    )
    assert (
        f"Legacy cache at {DATE_2020_05_01_MTL.astimezone(UTC)}: slurm.raisin.2020-05-01.conf"
        in caplog.text
    )
    assert (
        f"Legacy cache at {DATE_2020_10_01_MTL.astimezone(UTC)}: "
        f"slurm.raisin.{DATE_2020_10_01_MTL.astimezone(UTC)}.conf" in caplog.text
    )

    # Each legacy file should have been deactivated
    for legacy_path, _ in legacy:
        deactivated_path = legacy_path.parent / f".{legacy_path.parts[-1]}"
        assert not legacy_path.exists()
        assert deactivated_path.is_file()

    # Latest legacy file should have been skipped
    assert (
        f"slurm.conf file at {DATE_2020_10_01_MTL.astimezone(UTC)} "
        f"have not changed since: {DATE_2020_05_01_MTL.astimezone(UTC)}, skipping."
        in caplog.text
    )

    # Newly downloaded file should have been skipped
    assert (
        f"slurm.conf file at {DATE_2020_12_01_MTL.astimezone(UTC)} "
        f"have not changed since: {DATE_2020_05_01_MTL.astimezone(UTC)}, skipping."
        in caplog.text
    )


@pytest.mark.usefixtures("empty_read_write_db", "enabled_cache", "tzlocal_is_mtl")
def test_parse_slurmconfig(cli_main, caplog):
    caplog.set_level(logging.INFO)

    assert get_cluster_gpu_billings("raisin") == []
    assert get_node_to_gpu("raisin") == None

    #  when cache is empty
    assert cli_main(["parse", "slurmconfig", "-c", "raisin"]) == 0
    assert get_cluster_gpu_billings("raisin") == []
    assert get_node_to_gpu("raisin") == None
    caplog.clear()

    _save_slurm_conf("raisin", "2020-01-01", SLURM_CONF_RAISIN_2020_01_01)

    with pytest.raises(KeyError) as exc_info:
        cli_main(["parse", "slurmconfig", "-c", "unknown_raisin"])
        assert str(exc_info.value) == "unknown_raisin"

    assert cli_main(["-v", "parse", "slurmconfig", "-c", "raisin"]) == 0

    # No harmonization available for gpu1
    assert re.search(
        r"WARNING +sarc\.cli\.parse\.slurmconfig:slurmconfig\.py:[0-9]+ \[raisin]\[partition2] +"
        r"Cannot harmonize: gpu1 \(keep this name as-is\) : mynode\[2,8-11,42]",
        caplog.text,
    )

    expected_gpu_billing_1 = GPUBilling(
        cluster_name="raisin",
        since=datetime(2020, 1, 1, tzinfo=MTL).astimezone(UTC),
        gpu_to_billing={
            "gpu1": 5000,
            "THE GPU II": 7500,
        },
    )
    assert_same_billings(get_cluster_gpu_billings("raisin"), [expected_gpu_billing_1])

    expected_node_to_gpu_1 = NodeGPUMapping(
        cluster_name="raisin",
        since=datetime(2020, 1, 1, tzinfo=MTL).astimezone(UTC),
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
    assert cli_main(["-v", "parse", "slurmconfig", "-c", "raisin"]) == 0
    expected_gpu_billing_2 = GPUBilling(
        cluster_name="raisin",
        since=datetime(2020, 5, 1, tzinfo=MTL).astimezone(UTC),
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
        since=datetime(2020, 5, 1, tzinfo=MTL).astimezone(UTC),
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


@pytest.mark.usefixtures("empty_read_write_db", "enabled_cache", "tzlocal_is_mtl")
def test_parse_slurmconfig_since(cli_main, caplog):
    """test parse with argument --since"""
    caplog.set_level(logging.INFO)

    assert get_cluster_gpu_billings("raisin") == []
    assert get_node_to_gpu("raisin") == None

    _save_slurm_conf("raisin", "2020-01-01", SLURM_CONF_RAISIN_2020_01_01)
    _save_slurm_conf("raisin", "2020-05-01", SLURM_CONF_RAISIN_2020_05_01)

    assert (
        cli_main(
            ["-v", "parse", "slurmconfig", "-c", "raisin", "--since", "2020-04-01"]
        )
        == 0
    )

    # No harmonization available for gpu1
    assert re.search(
        r"WARNING +sarc\.cli\.parse\.slurmconfig:slurmconfig\.py:[0-9]+ \[raisin]\[partition2] +"
        r"Cannot harmonize: gpu1 \(keep this name as-is\) : mynode\[2,8-11,42]",
        caplog.text,
    )

    expected_gpu_billing_2 = GPUBilling(
        cluster_name="raisin",
        since=datetime(2020, 5, 1, tzinfo=MTL).astimezone(UTC),
        gpu_to_billing={
            "gpu1": 4000,
            "THE GPU II": 9000,
        },
    )
    assert_same_billings(get_cluster_gpu_billings("raisin"), [expected_gpu_billing_2])

    expected_node_to_gpu_1 = NodeGPUMapping(
        cluster_name="raisin",
        since=datetime(2020, 1, 1, tzinfo=MTL).astimezone(UTC),
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
    expected_node_to_gpu_2 = NodeGPUMapping(
        cluster_name="raisin",
        since=datetime(2020, 5, 1, tzinfo=MTL).astimezone(UTC),
        node_to_gpu=expected_node_to_gpu_1.node_to_gpu.copy(),
    )
    del expected_node_to_gpu_2.node_to_gpu["alone_node"]
    assert_same_node_gpu_mapping(get_node_to_gpu("raisin"), expected_node_to_gpu_2)

    # Check that we get the same node_to_gpu for any date,
    # since there is only 1 node->gpu mapping available.
    def _parse_date(value: str):
        return datetime.combine(datetime.fromisoformat(value), time.min).replace(
            tzinfo=MTL
        )

    for date_str in [
        "2019-12-01",
        "2020-01-01",
        "2020-03-07",
        "2020-05-01",
        "2020-05-20",
        "2020-10-10",
    ]:
        assert_same_node_gpu_mapping(
            get_node_to_gpu("raisin", _parse_date(date_str)), expected_node_to_gpu_2
        )


@pytest.mark.usefixtures("empty_read_write_db", "enabled_cache", "tzlocal_is_mtl")
def test_parse_slurmconfig_mila(cli_main, caplog):
    """Test parse_slurmconfig on cluster mila, where billing_is_gpu is True."""
    cluster_name_mila = "mila"

    caplog.set_level(logging.INFO)

    assert get_cluster_gpu_billings(cluster_name_mila) == []
    assert get_node_to_gpu(cluster_name_mila) == None

    _save_slurm_conf(cluster_name_mila, "2020-01-01", SLURM_CONF_RAISIN_2020_01_01)

    assert cli_main(["-v", "parse", "slurmconfig", "-c", cluster_name_mila]) == 0
    assert (
        f"GPU billing won't be parsed on cluster `{cluster_name_mila}`, "
        f"since billing is directly expressed as number of GPUs on this cluster."
    ) in caplog.text
    caplog.clear()
    # No GPU->billing must be parsed
    assert get_cluster_gpu_billings(cluster_name_mila) == []

    # GPU->node must be parsed
    expected_node_to_gpu_1 = NodeGPUMapping(
        cluster_name=cluster_name_mila,
        since=datetime(2020, 1, 1, tzinfo=MTL).astimezone(UTC),
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
    assert_same_node_gpu_mapping(
        get_node_to_gpu(cluster_name_mila), expected_node_to_gpu_1
    )

    # Save next conf file
    _save_slurm_conf("mila", "2020-05-01", SLURM_CONF_RAISIN_2020_05_01)
    assert cli_main(["-v", "parse", "slurmconfig", "-c", cluster_name_mila]) == 0
    assert (
        f"GPU billing won't be parsed on cluster `{cluster_name_mila}`, "
        f"since billing is directly expressed as number of GPUs on this cluster."
    ) in caplog.text
    caplog.clear()

    # No GPU->billing must be parsed
    assert get_cluster_gpu_billings("mila") == []

    # GPU->node must be parsed
    expected_node_to_gpu_2 = NodeGPUMapping(
        cluster_name=cluster_name_mila,
        since=datetime(2020, 5, 1, tzinfo=MTL).astimezone(UTC),
        node_to_gpu=expected_node_to_gpu_1.node_to_gpu.copy(),
    )
    del expected_node_to_gpu_2.node_to_gpu["alone_node"]
    assert_same_node_gpu_mapping(
        get_node_to_gpu(cluster_name_mila), expected_node_to_gpu_2
    )

    # Check that we get the right node_to_gpu for a given date
    def _parse_date(value: str):
        return datetime.combine(datetime.fromisoformat(value), time.min).replace(
            tzinfo=MTL
        )

    assert_same_node_gpu_mapping(
        get_node_to_gpu(cluster_name_mila, _parse_date("2019-12-01")),
        expected_node_to_gpu_1,
    )
    assert_same_node_gpu_mapping(
        get_node_to_gpu(cluster_name_mila, _parse_date("2020-01-01")),
        expected_node_to_gpu_1,
    )
    assert_same_node_gpu_mapping(
        get_node_to_gpu(cluster_name_mila, _parse_date("2020-03-07")),
        expected_node_to_gpu_1,
    )
    assert_same_node_gpu_mapping(
        get_node_to_gpu(cluster_name_mila, _parse_date("2020-05-01")),
        expected_node_to_gpu_2,
    )
    assert_same_node_gpu_mapping(
        get_node_to_gpu(cluster_name_mila, _parse_date("2020-05-20")),
        expected_node_to_gpu_2,
    )
    assert_same_node_gpu_mapping(
        get_node_to_gpu(cluster_name_mila, _parse_date("2020-10-10")),
        expected_node_to_gpu_2,
    )


SLURM_CONF_RAISIN_2020_01_01_INCONSISTENT_BILLING = """
NodeName=mynode[1,2,5-20,30,40-43] UselessParam=UselessValue Gres=gpu1

PartitionName=partition1 Nodes=mynode[1,5,6,29-41] TRESBillingWeights=x=1,GRES/gpu=5000,y=2
PartitionName=partition2 Nodes=mynode[2,8-11,42] TRESBillingWeights=x=1,GRES/gpu:gpu1=6000,y=2
"""


@pytest.mark.parametrize("threshold", [None, 0.1, 1, 10, 19])
@pytest.mark.usefixtures("empty_read_write_db", "enabled_cache")
def test_parse_slurmconfig_inconsistent_billing(cli_main, threshold):
    _save_slurm_conf(
        "raisin",
        "2020-01-01",
        SLURM_CONF_RAISIN_2020_01_01_INCONSISTENT_BILLING,
    )

    command = ["parse", "slurmconfig", "-c", "raisin"]
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
def test_parse_slurmconfig_inconsistent_billing_success(cli_main, threshold):
    """Test that parsing succeeds with greater threshold"""
    _save_slurm_conf(
        "raisin",
        "2020-01-01",
        SLURM_CONF_RAISIN_2020_01_01_INCONSISTENT_BILLING,
    )
    assert (
        cli_main(
            [
                "parse",
                "slurmconfig",
                "-c",
                "raisin",
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


def assert_same_node_gpu_mapping(given: NodeGPUMapping, expected: NodeGPUMapping):
    assert given.since == expected.since
    assert given.node_to_gpu == expected.node_to_gpu


def _save_slurm_conf(cluster_name: str, day: str, content: str):
    cache = Cache(subdirectory=f"slurm_conf/{cluster_name}")
    date = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=MTL).astimezone(UTC)
    with cache.create_entry(date) as entry:
        entry.add_value(date.isoformat(), content.encode("utf-8"))


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
