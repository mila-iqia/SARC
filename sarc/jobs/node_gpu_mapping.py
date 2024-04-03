"""
This module provides a dict-like class NodeToGPUMapping
to map a cluster's node name to GPU type
by parsing TXT files containing node descriptions like:
    NodeName=<nodes_description> Gres=<gpu-type> ...
"""

import json
import os

import regex as re
from hostlist import expand_hostlist

MIG_FLAG = "__MIG__"
DEFAULTS_FLAG = "__DEFAULTS__"


def _find_pattern(string: str) -> tuple:
    try:
        begin = string.index("{{")
        end = string.index("}}")
    except ValueError:
        return None, None
    return string[begin : end + 2], string[begin + 2 : end]


def _expand_list(list_pattern: str) -> str:
    start, stop = list_pattern[1:-1].split("-")
    start, stop = int(start), int(stop)
    regex = "|".join([f"0*{i}" for i in range(start, stop + 1)])
    return f"({regex})"


EXPAND_PATTERNS = {re.compile(r"^\[.*\]$"): _expand_list}


def expand_patterns(string: str) -> re.Pattern:
    full_pattern, pattern = _find_pattern(string)
    while pattern:
        for pattern_regex, _expand in EXPAND_PATTERNS.items():
            if pattern_regex.match(pattern):
                regex = _expand(pattern)
                string = string.replace(full_pattern, regex)
                break
        else:
            raise ValueError(f"Unknown pattern {full_pattern}")

        full_pattern, pattern = _find_pattern(string)

    return re.compile(string)


class NodeToGPUMapping:
    """Helper class to generate JSON file, load it in memory, and query GPU type for a nodename."""

    def __init__(self, cluster_name, nodes_info_file, gpus_per_nodes: dict):
        """Initialize with cluster name and TXT file path to parse."""

        # Mapping is empty by default.
        self.mapping = {}
        self.json_path = None
        self.harmonize_gpu_map = {}
        for node_pattern, node_gpus in gpus_per_nodes.items():
            if node_pattern != DEFAULTS_FLAG:
                node_pattern = expand_patterns(f"^{node_pattern}$")
            self.harmonize_gpu_map[node_pattern] = {
                re.compile(f".*{gpu}.*"): descriptive_gpu
                for gpu, descriptive_gpu in node_gpus.items()
            }
        self.default_gpu_map = self.harmonize_gpu_map.pop(DEFAULTS_FLAG, {})

        # Mapping is filled only if TXT file is available.
        if nodes_info_file and os.path.exists(nodes_info_file):
            nodes_info_file = os.path.abspath(nodes_info_file)
            # JSON file is expected to be located in same folder as TXT file.
            self.json_path = os.path.join(
                os.path.dirname(nodes_info_file), f"node_to_gpu_{cluster_name}.json"
            )

            info_file_stat = os.stat(nodes_info_file)
            # JSON file is (re)generated if it does not yet exist
            # or if it's older than TXT file.
            if (
                not os.path.exists(self.json_path)
                or os.stat(self.json_path).st_mtime < info_file_stat.st_mtime
            ):
                # Parse TXT file into self.mapping.
                self._parse_nodenames(nodes_info_file, self.mapping)
                # Save self.mapping into JSON file.
                with open(self.json_path, "w", encoding="utf-8") as file:
                    json.dump(self.mapping, file, indent=1)
            else:
                # Otherwise, just load existing JSON file.
                with open(self.json_path, encoding="utf-8") as file:
                    self.mapping = json.load(file)

    def _harmonize_gpu(self, nodename: str, gpu_type: str):
        gpu_type = gpu_type.lower().replace(" ", "-").split(":")
        if gpu_type[0] == "gpu":
            gpu_type.pop(0)
        gpu_type = gpu_type[0]

        for node_regex, gpu_map in self.harmonize_gpu_map.items():
            if node_regex.match(nodename):
                break
        else:
            gpu_map = self.default_gpu_map

        for regex, harmonized_gpu in gpu_map.items():
            if regex.match(gpu_type):
                break
        else:
            harmonized_gpu = None

        if harmonized_gpu and harmonized_gpu.startswith(MIG_FLAG):
            harmonized_gpu = self._harmonize_gpu(
                nodename, harmonized_gpu[len(MIG_FLAG) :]
            )
            harmonized_gpu = f"{harmonized_gpu} : {gpu_type}"

        return harmonized_gpu

    def __getitem__(self, nodename):
        """Return GPU type for nodename, or None if not found."""
        gpu_type = self.mapping.get(nodename, None)
        return self._harmonize_gpu(nodename, gpu_type)

    @staticmethod
    def _parse_nodenames(path: str, output: dict):
        """
        Parse node-to-GPU mapping from a path and save parsed nodes into output dict.

        Path should be a txt file containing lines like:

        NodeName=<nodes_description> Gres=<gpu-type> <...other key=value will be ignored>

        Other lines (e.g. blank lines or commented lines) will be ignored.
        """
        with open(path, encoding="utf-8") as file:
            for line in file:
                # Parse only lines starting with "NodeName"
                line = line.strip()

                if not line.startswith("NodeName"):
                    continue

                nodes_config = dict(
                    option.split("=", maxsplit=1) for option in line.split()
                )
                all_nodenames = expand_hostlist(nodes_config["NodeName"])
                gres = nodes_config["Gres"]
                output.update({node_name: gres for node_name in all_nodenames})
