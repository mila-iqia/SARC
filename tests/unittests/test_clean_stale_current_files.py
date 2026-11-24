"""Test for scripts/clean_stale_current_files.py (imported directly, scripts
having no package to live in)."""

import importlib.util
import io
import os
import sys
import time
from pathlib import Path
from zipfile import ZIP_LZMA, ZipFile

from sarc.config import config


def _load_script(name: str):
    path = Path(__file__).resolve().parents[2] / "scripts" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


script = _load_script("clean_stale_current_files")


def _write_zip(path: Path, complete: bool) -> None:
    """Write a one-entry zip; unless complete, cut it before its index, as
    a writer killed before close leaves it."""
    buffer = io.BytesIO()
    with ZipFile(buffer, "w", compression=ZIP_LZMA) as zf:
        zf.writestr("mila_2026-11-20T00:00_2026-11-20T01:00", b"sacct output")
        index_offset = buffer.tell()
    data = buffer.getvalue()
    path.write_bytes(data if complete else data[:index_offset])


def test_deletes_only_stale_incomplete_entries(enabled_cache, monkeypatch):
    day = config.cache / "jobs" / "2026" / "11" / "20"
    day.mkdir(parents=True)
    killed = day / "01:00:00.current"
    unrenamed = day / "02:00:00.current"
    in_progress = day / "03:00:00.current"
    entry = day / "04:00:00.000"
    _write_zip(killed, complete=False)
    _write_zip(unrenamed, complete=True)
    _write_zip(in_progress, complete=False)
    _write_zip(entry, complete=True)
    stale = time.time() - (script.DEFAULT_MAX_AGE_HOURS + 1) * 3600
    for path in (killed, unrenamed, entry):
        os.utime(path, (stale, stale))
    all_files = sorted([killed.name, unrenamed.name, in_progress.name, entry.name])
    assert sorted(p.name for p in day.iterdir()) == all_files

    monkeypatch.setattr(sys, "argv", ["clean_stale_current_files.py"])
    assert script.main() == 0
    assert sorted(p.name for p in day.iterdir()) == all_files

    monkeypatch.setattr(sys, "argv", ["clean_stale_current_files.py", "--no-dry-run"])
    assert script.main() == 0
    assert sorted(p.name for p in day.iterdir()) == sorted(
        [unrenamed.name, in_progress.name, entry.name]
    )
