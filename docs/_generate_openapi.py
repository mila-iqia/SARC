import json
from pathlib import Path

from sarc.api.main import app


def generate_openapi_spec():
    """
    Generate OpenAPI spec and save it into `openapi.json` in same folder as this script.
    **NB**: One must run this script each time REST API changes, before generating the doc.
    """
    spec = app.openapi()
    spec_path = Path(__file__).parent / "openapi.json"
    with open(spec_path, "w", encoding="utf-8") as f:
        json.dump(spec, f, indent=1)
    print(f"OpenAPI spec generated at {spec_path}")


if __name__ == "__main__":
    generate_openapi_spec()
