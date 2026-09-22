import re
from pathlib import Path

from sarc.db import get_meta
from sarc.db.job_series import JobSeriesDB

# Declared `table=True` but created as a view; alembic's include_object skips it
# the same way.
VIEWS = {JobSeriesDB.__tablename__}

# (parent, child) relations drawn from the child instead. Layout only: a parent
# belongs to the leftmost layer, so a small lookup table lands among the hubs
# and its edge crosses theirs. Mirroring the cardinality states the same thing.
MIRRORED = {("gpurgudb", "slurm_jobs")}


def _token(type_) -> str:
    """Mermaid attribute types are single tokens, SQL type names are not."""
    return re.sub(r"\W+", "_", str(type_)).strip("_").lower()


def generate_er_diagram():
    """
    Generate a mermaid ER diagram of the SQL schema and save it into
    `dev_overview/db_schema.mmd`, included by `dev_overview/db_tables.md`.
    NB: To run each time the tables change, before generating the doc.
    """
    tables = {n: t for n, t in get_meta().tables.items() if n not in VIEWS}

    edges: list[str] = []
    linked: set[str] = set()
    for name, table in sorted(tables.items()):
        for column in table.columns:
            for fk in column.foreign_keys:
                target, target_column = fk.target_fullname.split(".")
                if target in VIEWS:
                    continue
                # Mermaid spells an optional side `o`, a mandatory one `|`.
                side = "o" if column.nullable else "|"
                mirrored = (target, name) in MIRRORED
                # An `id` target goes unsaid, the `<parent>_id` naming carries
                # it; a natural key does not, so name both ends, reading the
                # way the edge is drawn.
                label = column.name
                if target_column != "id":
                    label = (
                        f'"{column.name} → {target_column}"'
                        if mirrored
                        else f'"{target_column} ← {column.name}"'
                    )
                if mirrored:
                    edges.append(f"    {name} }}{side}--|| {target} : {label}")
                else:
                    edges.append(f"    {target} ||--{side}{{ {name} : {label}")
                linked.update((name, target))

    # No foreign key reaches these, so relationship lines alone would drop them
    # from the diagram. Declaring their primary key keeps them on it.
    blocks: list[str] = []
    for name, table in sorted(tables.items()):
        if name in linked:
            continue
        keys = "\n".join(
            f"        {_token(c.type)} {c.name} PK" for c in table.primary_key
        )
        blocks.append(f"    {name} {{\n{keys}\n    }}")

    # elk routes edges orthogonally and packs the tables no foreign key reaches
    # into the margin; dagre, the default, ranks them next to the hubs and draws
    # the hubs' own edges across them. It needs `mermaid_include_elk` in
    # conf.py, without which mermaid falls back to dagre silently.
    # LR stacks each hub's children vertically. Under the default TB the 21
    # entities spread across the page width and every box shrinks with it.
    header = ['%%{init: {"layout": "elk"}}%%', "erDiagram", "    direction LR"]
    diagram = "\n".join([*header, *edges, *blocks]) + "\n"
    out = Path(__file__).parent / "dev_overview" / "db_schema.mmd"
    out.write_text(diagram, encoding="utf-8")
    print(
        f"ER diagram generated at {out} ({len(tables)} tables, {len(edges)} relations)"
    )


if __name__ == "__main__":
    generate_er_diagram()
