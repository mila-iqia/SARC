"""
To run:
SARC_CONFIG=config/sarc-client.json python <this-script> <args>
First dates for transition to RGU:
- beluga: 2024-04-03
- cedar: 2024-04-03
- graham: 2024-04-03
- narval: 2023-11-28

Command lines:

SARC_CONFIG=config/sarc-client.json python scripts/slurmconfutils/extract_gpu_billings.py -i secrets/cluster_config/slurm.beluga.conf -c beluga -d 2024-04-03
SARC_CONFIG=config/sarc-client.json python scripts/slurmconfutils/extract_gpu_billings.py -i secrets/cluster_config/slurm.cedar.conf -c cedar -d 2024-04-03
SARC_CONFIG=config/sarc-client.json python scripts/slurmconfutils/extract_gpu_billings.py -i secrets/cluster_config/slurm.graham.conf -c graham -d 2024-04-03
SARC_CONFIG=config/sarc-client.json python scripts/slurmconfutils/extract_gpu_billings.py -i secrets/cluster_config/slurm.narval.conf -c narval -d 2023-11-28
"""

import argparse
import json
import logging
import pprint
from typing import Dict

from hostlist import expand_hostlist

from sarc.cli.acquire.gpubillings import _gpu_type_to_billing_cache_key
from sarc.config import config


def parse_gpu_to_billing(cluster_conf_filename: str) -> Dict[str, float]:
    partitions = []
    node_to_gpu = {}
    with open(cluster_conf_filename, encoding="utf-8") as file:
        for line_number, line in enumerate(file):
            line = line.strip()
            if line.startswith("PartitionName="):
                partitions.append(
                    {
                        **dict(
                            option.split("=", maxsplit=1) for option in line.split()
                        ),
                        "__line_number__": line_number + 1,
                        "__line__": line,
                    }
                )
            elif line.startswith("NodeName="):
                nodes_config = dict(
                    option.split("=", maxsplit=1) for option in line.split()
                )
                gres = nodes_config.get("Gres")
                if gres:
                    all_nodenames = expand_hostlist(nodes_config["NodeName"])
                    node_to_gpu.update({node_name: gres for node_name in all_nodenames})

    gpu_to_billing = {}
    for partition in partitions:
        tres_billing_weights = partition.get("TRESBillingWeights")
        if not tres_billing_weights:
            continue

        weights = dict(
            option.split("=", maxsplit=1) for option in tres_billing_weights.split(",")
        )
        gpu_weights = {
            key: value for key, value in weights.items() if key.startswith("GRES/gpu")
        }
        local_gpu_to_billing = {}
        for key, value in gpu_weights.items():
            value = float(value)

            if key == "GRES/gpu":
                if len(gpu_weights) == 1:
                    gpu_names = {
                        node_to_gpu[nodename]
                        for nodename in expand_hostlist(partition["Nodes"])
                    }
                    local_gpu_to_billing.update(
                        {gpu_name: value for gpu_name in gpu_names}
                    )
                else:
                    logging.debug(
                        f"[line {partition['__line_number__']}] "
                        f"Ignored ambiguous GPU billing (cannot match to a specific GPU): `{key}={value}`"
                        f"  |  {partition['__line__']}"
                    )
            else:
                _, gpu_name = key.split(":", maxsplit=1)
                local_gpu_to_billing[gpu_name] = value

        for gpu_name, value in local_gpu_to_billing.items():
            if gpu_name in gpu_to_billing:
                assert gpu_to_billing[gpu_name] == value, (
                    gpu_to_billing[gpu_name],
                    value,
                )
            else:
                gpu_to_billing[gpu_name] = value

    return gpu_to_billing


def main():
    parser = argparse.ArgumentParser(
        description="Parse cluster configuration file and generate GPU->billing mapping file for SARC"
    )
    parser.add_argument(
        "-i", "--input", type=str, required=True, help="Cluster configuration file"
    )
    parser.add_argument("-c", "--cluster", type=str, required=True, help="Cluster name")
    parser.add_argument(
        "-d",
        "--date",
        type=str,
        required=True,
        help="RGU start date for current cluster configuration content",
    )
    args = parser.parse_args()
    gpu_to_billing = parse_gpu_to_billing(args.input)
    pprint.pprint(gpu_to_billing)

    content = [{"billing_start_date": args.date, "gpu_to_billing": gpu_to_billing}]
    output_folder = config().cache / "gpu_billing"
    output_path = output_folder / _gpu_type_to_billing_cache_key(args.cluster)

    if output_path.is_file():
        with open(output_path, encoding="utf-8") as file:
            previous_content = json.load(file)
        assert isinstance(previous_content, list)
        for previous_mapping in previous_content:
            assert isinstance(previous_mapping, dict)
            assert "billing_start_date" in previous_mapping
            assert "gpu_to_billing" in previous_mapping
            if previous_mapping["billing_start_date"] == args.date:
                if previous_mapping["gpu_to_billing"] == gpu_to_billing:
                    logging.debug("Mapping already in cache, nothing to do")
                    return
                else:
                    logging.warning("Mapping will be updated in cache")

    output_folder.mkdir(exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as file:
        json.dump(content, file, indent=2)


if __name__ == "__main__":
    # logging.basicConfig(level=logging.NOTSET)
    main()
