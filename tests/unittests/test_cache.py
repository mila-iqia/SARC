import json
import os
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from sarc.cache import FormatterProto, with_cache


class plaintext(FormatterProto):
    read_flags = "r"
    write_flags = "w"

    @staticmethod
    def load(fp):
        return fp.read()

    @staticmethod
    def dump(obj, fp):
        fp.write(obj)


def la_fonction(x, y, version=0):
    return f"{x} * {y} = {x * y} [v{version}]"


def la_cle(x, y, version):
    return f"{x}.{y}.json"


def la_cle_temporelle(x, y, version):
    return f"{x}.{y}.{{time}}.json"


def la_validite(x, y, version):
    return timedelta(days=x)


def test_simple_cache(tmpdir):
    reference = datetime(year=2024, month=1, day=1)

    tmpdir = Path(tmpdir)
    decorator = with_cache(
        subdirectory="xy",
        cache_root=tmpdir,
    )
    fn = decorator(la_fonction)
    assert (result1 := fn(1, 2, version=0, at_time=reference)) == "1 * 2 = 2 [v0]"
    assert fn(1, 2, version=1, at_time=reference) == result1
    assert fn(2, 3, version=1, at_time=reference) == result1
    assert fn(7, 49, version=1, at_time=reference) == result1

    file1 = tmpdir / "xy" / "2024-01-01-00-00-00.json"
    assert file1.exists()
    assert json.loads(file1.read_text()) == result1


def test_cache_key(tmpdir):
    tmpdir = Path(tmpdir)
    decorator = with_cache(
        key=la_cle,
        subdirectory="xy",
        cache_root=tmpdir,
    )
    fn = decorator(la_fonction)
    assert (result1 := fn(1, 2, version=0)) == "1 * 2 = 2 [v0]"
    assert fn(1, 2, version=1) == result1

    assert (result2 := fn(7, 8, version=0)) == "7 * 8 = 56 [v0]"

    file1 = tmpdir / "xy" / "1.2.json"
    assert file1.exists()
    assert json.loads(file1.read_text()) == result1

    file2 = tmpdir / "xy" / "7.8.json"
    assert file2.exists()
    assert json.loads(file2.read_text()) == result2


def test_use_cache(tmpdir):
    tmpdir = Path(tmpdir)
    decorator = with_cache(
        key=la_cle,
        subdirectory="xy",
        cache_root=tmpdir,
    )
    fn = decorator(la_fonction)
    # Compute and save
    assert fn(1, 2, version=0) == "1 * 2 = 2 [v0]"
    # Use cache
    assert fn(1, 2, version=1) == "1 * 2 = 2 [v0]"
    # Recompute and save
    assert fn(1, 2, version=2, cache_policy=False) == "1 * 2 = 2 [v2]"
    # Use cache
    assert fn(1, 2, version=3) == "1 * 2 = 2 [v2]"
    # Recompute but do not save
    assert fn(1, 2, version=4, cache_policy=False, save_cache=False) == "1 * 2 = 2 [v4]"
    # Use cache (older)
    assert fn(1, 2, version=5) == "1 * 2 = 2 [v2]"


def test_require_cache(tmpdir):
    reference = datetime(year=2024, month=1, day=1)

    tmpdir = Path(tmpdir)
    decorator = with_cache(
        subdirectory="xy",
        validity=timedelta(days=1),
        cache_root=tmpdir,
    )
    fn = decorator(la_fonction)
    with pytest.raises(Exception, match="There is no cached result"):
        fn(2, 3, version=0, at_time=reference, cache_policy="always")

    assert fn(2, 3, version=1, at_time=reference) == "2 * 3 = 6 [v1]"
    assert (
        fn(
            2,
            3,
            version=2,
            cache_policy="always",
            at_time=reference + timedelta(days=10),
        )
        == "2 * 3 = 6 [v1]"
    )
    assert (
        fn(
            2,
            3,
            version=4,
            cache_policy="always",
            at_time=reference + timedelta(days=10),
        )
        == "2 * 3 = 6 [v1]"
    )


def test_cache_validity(tmpdir):
    tmpdir = Path(tmpdir)
    fn = with_cache(
        la_fonction,
        key=la_cle_temporelle,
        subdirectory="xy",
        cache_root=tmpdir,
        validity=timedelta(days=1),
    )
    reference = datetime(year=2024, month=1, day=1)

    assert (result1 := fn(2, 3, version=0, at_time=reference)) == "2 * 3 = 6 [v0]"
    file1 = tmpdir / "xy" / "2.3.2024-01-01-00-00-00.json"
    assert file1.exists()
    assert json.loads(file1.read_text()) == result1

    assert fn(2, 3, version=1, at_time=reference) == result1

    assert (
        result2 := fn(2, 3, version=2, at_time=reference + timedelta(days=2))
    ) == "2 * 3 = 6 [v2]"
    file2 = tmpdir / "xy" / "2.3.2024-01-03-00-00-00.json"
    assert file2.exists()
    assert json.loads(file2.read_text()) == result2

    # We still get result1 if we go back in time
    assert fn(2, 3, version=666, at_time=reference) == result1

    # We get result2 again afterwards
    assert fn(2, 3, version=666, at_time=reference + timedelta(days=2.5)) == result2


def test_cache_dynamic_validity(tmpdir):
    tmpdir = Path(tmpdir)
    fn = with_cache(
        la_fonction,
        key=la_cle_temporelle,
        subdirectory="xy",
        cache_root=tmpdir,
        validity=la_validite,
    )
    reference = datetime(year=2024, month=1, day=1)

    assert fn(2, 3, version=0, at_time=reference) == "2 * 3 = 6 [v0]"
    assert (
        fn(2, 3, version=1, at_time=reference + timedelta(days=2)) == "2 * 3 = 6 [v0]"
    )
    assert (
        fn(2, 3, version=1, at_time=reference + timedelta(days=2.1)) == "2 * 3 = 6 [v1]"
    )

    assert fn(7, 3, version=0, at_time=reference) == "7 * 3 = 21 [v0]"
    assert (
        fn(7, 3, version=1, at_time=reference + timedelta(days=7)) == "7 * 3 = 21 [v0]"
    )
    assert (
        fn(7, 3, version=1, at_time=reference + timedelta(days=7.1))
        == "7 * 3 = 21 [v1]"
    )


def test_live_cache(tmpdir):
    tmpdir = Path(tmpdir)
    fn = with_cache(
        la_fonction,
        key=la_cle_temporelle,
        subdirectory="xy",
        cache_root=tmpdir,
        validity=timedelta(days=1),
        live=True,
    )
    reference = datetime(year=2024, month=1, day=1)

    assert (result1 := fn(2, 3, version=0, at_time=reference)) == "2 * 3 = 6 [v0]"
    file1 = tmpdir / "xy" / "2.3.2024-01-01-00-00-00.json"
    assert file1.exists()

    # This is to test that we're getting the live cache and not reading from a file:
    file1.unlink()

    assert fn(2, 3, version=1, at_time=reference) == result1


def test_live_cache_from_disk(tmpdir):
    tmpdir = Path(tmpdir)
    decorator = with_cache(
        key=la_cle,
        subdirectory="xy",
        cache_root=tmpdir,
        live=True,
    )
    fn = decorator(la_fonction)

    os.makedirs(tmpdir / "xy", exist_ok=True)
    file1 = tmpdir / "xy" / "1.2.json"
    file1.write_text('"hello!"')

    assert (result1 := fn(1, 2, version=0)) == "hello!"
    assert fn(1, 2, version=1) == result1

    # This is to test that we're getting the live cache and not reading from a file:
    file1.unlink()

    assert fn(1, 2, version=1) == result1


def test_live_cache_nodisk(tmpdir):
    tmpdir = Path(tmpdir)
    fn = with_cache(
        la_fonction,
        key=la_cle_temporelle,
        subdirectory="xy",
        cache_root=tmpdir,
        validity=timedelta(days=1),
        live=True,
        on_disk=False,
    )
    reference = datetime(year=2024, month=1, day=1)

    assert (result1 := fn(2, 3, version=0, at_time=reference)) == "2 * 3 = 6 [v0]"
    file1 = tmpdir / "xy" / "2.3.2024-01-01-00-00-00.json"
    assert not file1.exists()

    assert fn(2, 3, version=1, at_time=reference) == result1

    assert (
        result2 := fn(2, 3, version=2, at_time=reference + timedelta(days=2))
    ) == "2 * 3 = 6 [v2]"
    file2 = tmpdir / "xy" / "2.3.2024-01-03-00-00-00.json"
    assert not file2.exists()

    assert fn(2, 3, version=3, at_time=reference) == result2


def test_format_txt(tmpdir):
    def cle(x, y, version):
        return f"{x}.{y}.txt"

    tmpdir = Path(tmpdir)
    fn = with_cache(
        la_fonction,
        key=cle,
        formatter=plaintext,
        subdirectory="xy",
        cache_root=tmpdir,
    )

    assert (result := fn(7, 8, version=0)) == "7 * 8 = 56 [v0]"
    file = tmpdir / "xy" / "7.8.txt"
    assert file.exists()
    assert file.read_text() == result


def test_custom_format(tmpdir):
    class duck(FormatterProto):
        read_flags = "r"
        write_flags = "w"

        @staticmethod
        def load(fp):
            return "QUACK"

        @staticmethod
        def dump(obj, fp):
            fp.write(obj)

    def cle(x, y, version):
        return f"{x}.{y}.quack"

    tmpdir = Path(tmpdir)
    fn = with_cache(
        la_fonction,
        formatter=duck,
        key=cle,
        subdirectory="xy",
        cache_root=tmpdir,
    )

    assert fn(7, 8, version=0) == "7 * 8 = 56 [v0]"
    assert fn(7, 8, version=0) == "QUACK"


def test_no_cachedir(disabled_cache):
    decorator = with_cache(
        subdirectory="xy",
    )
    fn = decorator(la_fonction)
    assert fn(2, 3, version=0) == "2 * 3 = 6 [v0]"
    assert fn(2, 3, version=1) == "2 * 3 = 6 [v1]"
    with pytest.raises(Exception, match="There is no cached result"):
        fn(2, 3, version=2, cache_policy="always")


def test_cache_method(tmpdir):
    tmpdir = Path(tmpdir)

    class Booger:
        def __init__(self, value):
            self.value = value

        def key(self, version):
            return f"value-{self.value}.json"

        @with_cache(cache_root=tmpdir, key=key)
        def f1(self, version):
            return f"{self.value} * 2 = {self.value * 2} [v{version}]"

        @with_cache(cache_root=tmpdir, key=key)
        def f2(self, version):
            return f"{self.value} * 3 = {self.value * 3} [v{version}]"

    b1 = Booger(10)
    b2 = Booger(100)

    assert b1.f1(version=0) == "10 * 2 = 20 [v0]"
    assert b2.f1(version=1) == "100 * 2 = 200 [v1]"
    assert b1.f2(version=2) == "10 * 3 = 30 [v2]"
    assert b1.f2(version=3) == "10 * 3 = 30 [v2]"
    assert b1.f1(version=4) == "10 * 2 = 20 [v0]"
    assert b2.f2(version=5) == "100 * 3 = 300 [v5]"

    assert b1.f1.name.endswith("f1")
    assert b1.f2.name.endswith("f2")
