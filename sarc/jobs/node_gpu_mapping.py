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
                if line.startswith("NodeName"):
                    nodes_desc = None
                    gres = None
                    # Split on spaces to get each definition <key>=<value>
                    for definition in line.split():
                        # Split key and value and look for keys "NodeName" and "Gres"
                        key, value = definition.split("=")
                        if key == "NodeName":
                            nodes_desc = value
                        elif key == "Gres":
                            gres = value
                    # We must have saved "NodeName" to node_desc and "Gres" to gres
                    assert nodes_desc
                    assert gres
                    # Parse "NodeName" to get all node names
                    all_nodenames = []
                    if "[" in nodes_desc:
                        # "NodeName" has format <base_name>[<intervals>]
                        base_name, intervals = nodes_desc.split("[")
                        assert base_name
                        assert intervals.endswith("]")
                        intervals = intervals[:-1]
                        # Intervals are separated with commas
                        for interval in intervals.split(","):
                            # Each piece is either a <number>, or a range <a>-<b>
                            if "-" in interval:
                                a, b = interval.split("-")
                                all_nodenames.extend(
                                    f"{base_name}{number}"
                                    for number in range(int(a), int(b) + 1)
                                )
                            else:
                                number = int(interval)
                                all_nodenames.append(f"{base_name}{number}")
                    else:
                        # "NodeName" is a single node name
                        all_nodenames.append(nodes_desc)
                    # We must have parsed some node names
                    assert all_nodenames
                    # We can then map these nodes to gres in output
                    output.update({node_name: gres for node_name in all_nodenames})


NODENAME_TO_GPU = NodeToGPUMapping()
