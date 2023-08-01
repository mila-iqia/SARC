"""
This module provides a dict-like class NodeToGPUMapping
to map a cluster's node name to GPU type
by parsing TXT files containing node descriptions like:
    NodeName=<nodes_description> Gres=<gpu-type> ...
"""
import json
import os

from hostlist import expand_hostlist


class NodeToGPUMapping:
    """Helper class to generate JSON file, load it in memory, and query GPU type for a nodename."""

    def __init__(self, cluster_name, nodes_info_file):
        """Initialize with cluster name and TXT file path to parse."""

        # Mapping is empty by default.
        self.mapping = {}
        self.json_path = None

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
                # Pase TXT file into self.mapping.
                self._parse_nodenames(nodes_info_file, self.mapping)
                # Save self.mapping into JSON file.
                with open(self.json_path, "w", encoding="utf-8") as file:
                    json.dump(self.mapping, file, indent=1)
            else:
                # Otherwise, just load existing JSON file.
                with open(self.json_path, encoding="utf-8") as file:
                    self.mapping = json.load(file)

    def __getitem__(self, nodename):
        """Return GPU type for nodename, or None if not found."""
        return self.mapping.get(nodename, None)

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
