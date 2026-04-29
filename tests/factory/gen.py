import json
from pathlib import Path

from serieux import Serieux
from serieux.features.fromfile import IncludeFile

from .common import Data, DataFactory, RawSlurmOutput
from .gen_sacct import generate_sacct
from .gen_users import generate_users

here = Path(__file__).parent


def generate_and_write(basedir: Path):
    srx = (Serieux + IncludeFile)()
    factory = srx.deserialize(DataFactory, here / "factory-config.yaml")
    data = Data()
    generate_users(factory, data)
    generate_sacct(factory, data)

    scrapesdir = basedir / "sacct"
    scrapesdir.mkdir(parents=True, exist_ok=True)
    for k, v in data.scrapes.items():
        ser = srx.serialize(RawSlurmOutput, v)
        (scrapesdir / f"{k}.json").write_text(json.dumps(ser))


if __name__ == "__main__":
    generate_and_write(here.parent / "data")
