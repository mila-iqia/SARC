"""
This module is used to generate a JSON file mapping node name to GPU type,
by parsing TXT files containing node descriptions like:
    NodeName=<nodes_description> Gres=<gpu-type> ...

It also provides a utility dictionary-like object NODENAME_TO_GPU
to get GPU type associated to a node name by typing NODENAME_TO_GPU[nodename].
This object will do all the necessary work in background (generate and load JSON file).
"""
import json
import os
from hostlist import expand_hostlist


class NodeToGPUMapping:
    """Helper class to generate JSON file, load it in memeory, and query GPU type for a nodename."""

    _JSON_PATH_ = os.path.join(os.path.dirname(__file__), "nodename_to_gpu.json")

    def __init__(self):
        """Initialize."""

        self.mapping = {}

        # If necessary, generate JSON file mapping node names to GPU types.
        if not os.path.exists(self._JSON_PATH_):
            output = {}
            for path in [
                os.path.join(os.path.dirname(__file__), "nodenames_cedar.txt"),
                os.path.join(os.path.dirname(__file__), "nodenames_graham.txt"),
            ]:
                self._parse_nodenames(path, output)
            with open(self._JSON_PATH_, "w", encoding="utf-8") as file:
                json.dump(output, file, indent=1)

        # Load JSON file in memory.
        with open(self._JSON_PATH_, encoding="utf-8") as file:
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

                def parse_nodes_config(nodes_config_str):
                    return dict(option.split("=", maxsplit=1) for option in line.split())

                nodes_config = parse_nodes_config(line)
                all_nodenames = expand_hostlist(nodes_config["NodeName"])
                gres = nodes_config["Gres"]
                output.update({node_name: gres for node_name in all_nodenames})


NODENAME_TO_GPU = NodeToGPUMapping()
