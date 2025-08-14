import json
import logging
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import gifnoc
import pytest

from sarc.cache import (
    BinaryFormatter,
    Cache,
    CacheException,
    CachePolicy,
    FormatterProto,
    JSONFormatter,
    _cache_policy_from_env,
    cache_policy_var,
    make_cached_function,
    with_cache,
)


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


def test_simple_cache(tmp_path):
    reference = datetime(year=2024, month=1, day=1)

    decorator = with_cache(
        subdirectory="xy",
        cache_root=tmp_path,
    )
    fn = decorator(la_fonction)
    assert (result1 := fn(1, 2, version=0, at_time=reference)) == "1 * 2 = 2 [v0]"
    assert fn(1, 2, version=1, at_time=reference) == result1
    assert fn(2, 3, version=1, at_time=reference) == result1
    assert fn(7, 49, version=1, at_time=reference) == result1

    file1 = tmp_path / "xy" / "2024-01-01-00-00-00.json"
    assert file1.exists()
    assert json.loads(file1.read_text()) == result1


def test_cache_key(tmp_path):
    decorator = with_cache(
        key=la_cle,
        subdirectory="xy",
        cache_root=tmp_path,
    )
    fn = decorator(la_fonction)
    assert (result1 := fn(1, 2, version=0)) == "1 * 2 = 2 [v0]"
    assert fn(1, 2, version=1) == result1

    assert (result2 := fn(7, 8, version=0)) == "7 * 8 = 56 [v0]"

    file1 = tmp_path / "xy" / "1.2.json"
    assert file1.exists()
    assert json.loads(file1.read_text()) == result1

    file2 = tmp_path / "xy" / "7.8.json"
    assert file2.exists()
    assert json.loads(file2.read_text()) == result2


def test_use_cache(tmp_path):
    decorator = with_cache(
        key=la_cle,
        subdirectory="xy",
        cache_root=tmp_path,
    )
    fn = decorator(la_fonction)
    # Compute and save
    assert fn(1, 2, version=0) == "1 * 2 = 2 [v0]"
    # Use cache
    assert fn(1, 2, version=1) == "1 * 2 = 2 [v0]"
    # Recompute and save
    assert fn(1, 2, version=2, cache_policy=CachePolicy.refresh) == "1 * 2 = 2 [v2]"
    # Use cache
    assert fn(1, 2, version=3) == "1 * 2 = 2 [v2]"
    # Recompute but do not save
    assert (
        fn(1, 2, version=4, cache_policy=CachePolicy.refresh, save_cache=False)
        == "1 * 2 = 2 [v4]"
    )
    # Use cache (older)
    assert fn(1, 2, version=5) == "1 * 2 = 2 [v2]"


def test_require_cache(tmp_path):
    reference = datetime(year=2024, month=1, day=1)

    decorator = with_cache(
        subdirectory="xy",
        validity=timedelta(days=1),
        cache_root=tmp_path,
    )
    fn = decorator(la_fonction)
    with pytest.raises(Exception, match="There is no cached result"):
        fn(2, 3, version=0, at_time=reference, cache_policy=CachePolicy.always)

    assert fn(2, 3, version=1, at_time=reference) == "2 * 3 = 6 [v1]"
    assert (
        fn(
            2,
            3,
            version=2,
            cache_policy=CachePolicy.always,
            at_time=reference + timedelta(days=10),
        )
        == "2 * 3 = 6 [v1]"
    )
    assert (
        fn(
            2,
            3,
            version=4,
            cache_policy=CachePolicy.always,
            at_time=reference + timedelta(days=10),
        )
        == "2 * 3 = 6 [v1]"
    )


def test_cache_validity(tmp_path):
    fn = with_cache(
        la_fonction,
        key=la_cle_temporelle,
        subdirectory="xy",
        cache_root=tmp_path,
        validity=timedelta(days=1),
    )
    reference = datetime(year=2024, month=1, day=1, tzinfo=UTC)

    assert (result1 := fn(2, 3, version=0, at_time=reference)) == "2 * 3 = 6 [v0]"
    file1 = tmp_path / "xy" / "2.3.2024-01-01-00-00-00.json"
    assert file1.exists()
    assert json.loads(file1.read_text()) == result1

    assert fn(2, 3, version=1, at_time=reference) == result1

    assert (
        result2 := fn(2, 3, version=2, at_time=reference + timedelta(days=2))
    ) == "2 * 3 = 6 [v2]"
    file2 = tmp_path / "xy" / "2.3.2024-01-03-00-00-00.json"
    assert file2.exists()
    assert json.loads(file2.read_text()) == result2

    # We still get result1 if we go back in time
    assert fn(2, 3, version=666, at_time=reference) == result1

    # We get result2 again afterwards
    assert fn(2, 3, version=666, at_time=reference + timedelta(days=2.5)) == result2


def test_cache_dynamic_validity(tmp_path):
    fn = with_cache(
        la_fonction,
        key=la_cle_temporelle,
        subdirectory="xy",
        cache_root=tmp_path,
        validity=la_validite,
    )
    reference = datetime(year=2024, month=1, day=1, tzinfo=UTC)

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


def test_live_cache(tmp_path):
    fn = with_cache(
        la_fonction,
        key=la_cle_temporelle,
        subdirectory="xy",
        cache_root=tmp_path,
        validity=timedelta(days=1),
        live=True,
    )
    reference = datetime(year=2024, month=1, day=1)

    assert (result1 := fn(2, 3, version=0, at_time=reference)) == "2 * 3 = 6 [v0]"
    file1 = tmp_path / "xy" / "2.3.2024-01-01-00-00-00.json"
    assert file1.exists()

    # This is to test that we're getting the live cache and not reading from a file:
    file1.unlink()

    assert fn(2, 3, version=1, at_time=reference) == result1


def test_live_cache_from_disk(tmp_path):
    decorator = with_cache(
        key=la_cle,
        subdirectory="xy",
        cache_root=tmp_path,
        live=True,
    )
    fn = decorator(la_fonction)

    os.makedirs(tmp_path / "xy", exist_ok=True)
    file1 = tmp_path / "xy" / "1.2.json"
    file1.write_text('"hello!"')

    assert (result1 := fn(1, 2, version=0)) == "hello!"
    assert fn(1, 2, version=1) == result1

    # This is to test that we're getting the live cache and not reading from a file:
    file1.unlink()

    assert fn(1, 2, version=1) == result1


def test_live_cache_nodisk(tmp_path):
    fn = with_cache(
        la_fonction,
        key=la_cle_temporelle,
        subdirectory="xy",
        cache_root=tmp_path,
        validity=timedelta(days=1),
        live=True,
        on_disk=False,
    )
    reference = datetime(year=2024, month=1, day=1)

    assert (result1 := fn(2, 3, version=0, at_time=reference)) == "2 * 3 = 6 [v0]"
    file1 = tmp_path / "xy" / "2.3.2024-01-01-00-00-00.json"
    assert not file1.exists()

    assert fn(2, 3, version=1, at_time=reference) == result1

    assert (
        result2 := fn(2, 3, version=2, at_time=reference + timedelta(days=2))
    ) == "2 * 3 = 6 [v2]"
    file2 = tmp_path / "xy" / "2.3.2024-01-03-00-00-00.json"
    assert not file2.exists()

    assert fn(2, 3, version=3, at_time=reference) == result2


def test_format_txt(tmp_path):
    def cle(x, y, version):
        return f"{x}.{y}.txt"

    fn = with_cache(
        la_fonction,
        key=cle,
        formatter=plaintext,
        subdirectory="xy",
        cache_root=tmp_path,
    )

    assert (result := fn(7, 8, version=0)) == "7 * 8 = 56 [v0]"
    file = tmp_path / "xy" / "7.8.txt"
    assert file.exists()
    assert file.read_text() == result


def test_custom_format(tmp_path):
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

    fn = with_cache(
        la_fonction,
        formatter=duck,
        key=cle,
        subdirectory="xy",
        cache_root=tmp_path,
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
        fn(2, 3, version=2, cache_policy=CachePolicy.always)


def test_cache_method(tmp_path):
    class Booger:
        def __init__(self, value):
            self.value = value

        def key(self, version):
            return f"value-{self.value}.json"

        @with_cache(cache_root=tmp_path, key=key)
        def f1(self, version):
            return f"{self.value} * 2 = {self.value * 2} [v{version}]"

        @with_cache(cache_root=tmp_path, key=key)
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


def test_binary_formatter(tmp_path):
    test_data = b"Hello, this is binary data with some bytes!"

    # Test saving binary data
    output_file = tmp_path / "test.bin"
    with open(output_file, "wb") as fp:
        BinaryFormatter.dump(test_data, fp)

    # Test loading binary data
    with open(output_file, "rb") as fp:
        loaded_data = BinaryFormatter.load(fp)

    assert loaded_data == test_data
    assert output_file.exists()


def test_cache_read_save_none_at_time(tmp_path):
    cache = Cache(cache_root=tmp_path, subdirectory="", formatter=JSONFormatter)

    cache.save("test.json", {"data": "value"}, at_time=None)

    result = cache.read("test.json")
    assert result == {"data": "value"}


def test_cache_read_valid_true(tmp_path):
    cache = Cache(cache_root=tmp_path, subdirectory="", formatter=JSONFormatter)

    cache.save("test.json", {"data": "value"})

    result = cache.read("test.json", valid=True)
    assert result == {"data": "value"}


def test_cache_malformed_file(tmp_path):
    cache = Cache(cache_root=tmp_path, subdirectory="", formatter=JSONFormatter)

    # Create a malformed JSON file
    malformed_file = tmp_path / "malformed.json"
    malformed_file.write_text("{invalid json")

    # Try to read from malformed file - should skip it and raise CacheException
    with pytest.raises(CacheException, match="There is no cached result"):
        cache.read("malformed.json", at_time=datetime.now(UTC))


def test_cache_policy_check_same_value(tmp_path):
    decorator = with_cache(
        key=la_cle,
        subdirectory="xy",
        cache_root=tmp_path,
    )
    fn = decorator(la_fonction)

    # First call to create cache
    result1 = fn(1, 2, version=0)

    # Second call with check policy - should pass since values are the same
    result2 = fn(1, 2, version=0, cache_policy=CachePolicy.check)
    assert result1 == result2


def test_cache_policy_check_different_value(tmp_path):
    decorator = with_cache(
        key=la_cle,
        subdirectory="xy",
        cache_root=tmp_path,
    )
    fn = decorator(la_fonction)

    # First call to create cache
    fn(1, 2, version=0)

    # Second call with different version - should raise exception due to mismatch
    with pytest.raises(CacheException, match="Cached result != live result"):
        fn(1, 2, version=1, cache_policy=CachePolicy.check)


def test_cache_policy_check_non_json_diff(tmp_path):
    decorator = with_cache(
        key=la_cle,
        subdirectory="xy",
        cache_root=tmp_path,
        formatter=plaintext,
    )
    fn = decorator(la_fonction)

    # First call to create cache
    fn(1, 2, version=0)

    # Second call with different version - should raise exception with repr diff
    with pytest.raises(CacheException, match="Cached result != live result"):
        fn(1, 2, version=1, cache_policy=CachePolicy.check)


def test_cache_policy_from_env(monkeypatch):
    # Reset the context var
    cache_policy_var.set(None)

    # Test with different environment variables
    test_cases = [
        ("use", CachePolicy.use),
        ("refresh", CachePolicy.refresh),
        ("ignore", CachePolicy.ignore),
        ("always", CachePolicy.always),
        ("check", CachePolicy.check),
        ("invalid", CachePolicy.use),  # Default fallback
    ]

    for env_value, expected_policy in test_cases:
        monkeypatch.setenv("SARC_CACHE", env_value)
        cache_policy_var.set(None)  # Reset for each test
        policy = _cache_policy_from_env()
        assert policy == expected_policy


def test_cache_policy_from_env_cached(monkeypatch):
    # Set a cached value
    cache_policy_var.set(CachePolicy.always)

    # Change environment but should still return cached value
    monkeypatch.setenv("SARC_CACHE", "use")
    policy = _cache_policy_from_env()
    assert policy == CachePolicy.always


def test_make_cached_function_with_config_cache(tmp_path):
    mock_cache_path = tmp_path / "config_cache"
    mock_cache_path.mkdir()

    def test_function(x, y):
        return x + y

    def test_key(x, y):
        return f"{x}_{y}.json"

    with gifnoc.overlay({"sarc.cache": str(mock_cache_path)}):
        cached_fn = make_cached_function(
            fn=test_function,
            formatter=JSONFormatter,
            key=test_key,
            subdirectory="test_subdir",
            validity=True,
            on_disk=True,
            live=False,
            cache_root=None,
        )

        assert cached_fn.cache_dir.parent == mock_cache_path

        result = cached_fn(1, 2)
        assert result == 3

        cache_file = mock_cache_path / "test_subdir" / "1_2.json"
        assert cache_file.exists()


def test_make_cached_function_no_cache_root():
    def test_function(x, y):
        return x + y

    def test_key(x, y):
        return f"{x}_{y}.json"

    cached_fn = make_cached_function(
        fn=test_function,
        formatter=JSONFormatter,
        key=test_key,
        subdirectory="test_subdir",
        validity=True,
        on_disk=True,
        live=False,
        cache_root=None,
    )

    assert cached_fn.cache_dir is None

    result = cached_fn(1, 2)
    assert result == 3


def test_make_cached_function_no_disk(tmp_path):
    def test_function(x, y):
        return x + y

    def test_key(x, y):
        return f"{x}_{y}.json"

    cached_fn = make_cached_function(
        fn=test_function,
        formatter=JSONFormatter,
        key=test_key,
        subdirectory="test_subdir",
        validity=True,
        on_disk=False,
        live=False,
        cache_root=tmp_path,
    )

    assert not cached_fn.on_disk

    result = cached_fn(1, 2)
    assert result == 3

    # Check that no cache file was created
    cache_dir = tmp_path / "test_subdir"
    assert not cache_dir.exists()


def test_with_cache_decorator_no_function():
    # Test decorator mode
    decorator = with_cache(
        formatter=JSONFormatter,
        subdirectory="test_decorator",
    )

    @decorator
    def test_function(x, y):
        return x * y

    # Test the decorated function
    result = test_function(3, 4)
    assert result == 12


def test_with_cache_decorator_with_function():
    def test_function(x, y):
        return x * y

    # Test direct function decoration
    decorated_fn = with_cache(
        test_function,
        formatter=JSONFormatter,
        subdirectory="test_direct",
    )

    # Test the decorated function
    result = decorated_fn(3, 4)
    assert result == 12


def test_cache_read_valid_true_with_time_parsing(tmp_path):
    cache = Cache(cache_root=tmp_path, subdirectory="", formatter=JSONFormatter)

    # Create a cache file with time in filename
    test_time = datetime.now(UTC)
    time_str = test_time.strftime("%Y-%m-%d-%H-%M-%S")
    cache_file = tmp_path / f"test.{time_str}.json"
    cache_file.write_text('{"data": "value"}')

    # Read with valid=True - should parse time from filename
    result = cache.read("test.{time}.json", at_time=test_time, valid=True)
    assert result == {"data": "value"}


def function_with_qualname(x, y):
    return x + y


def test_make_cached_function_none_subdirectory(tmp_path):
    def test_key(x, y):
        return f"{x}_{y}.json"

    cached_fn = make_cached_function(
        fn=function_with_qualname,
        formatter=JSONFormatter,
        key=test_key,
        subdirectory=None,
        validity=True,
        on_disk=True,
        live=False,
        cache_root=Path(tmp_path),
    )
    result = cached_fn(1, 2)
    assert result == 3

    # Check that cache file was created in the function's qualname directory
    cache_dir = Path(tmp_path) / "function_with_qualname"
    cache_file = cache_dir / "1_2.json"
    assert cache_file.exists()


def test_cache_policy_none_uses_env(tmp_path, monkeypatch):
    cache_policy_var.set(None)
    monkeypatch.setenv("SARC_CACHE", "always")

    decorator = with_cache(
        key=la_cle,
        subdirectory="xy",
        cache_root=tmp_path,
    )
    fn = decorator(la_fonction)

    # Call with cache_policy=None - should use env var (always)
    with pytest.raises(Exception, match="There is no cached result"):
        fn(1, 2, version=0, cache_policy=None)


def test_cache_policy_none_uses_env_default(tmp_path, monkeypatch):
    cache_policy_var.set(None)
    monkeypatch.delenv("SARC_CACHE", raising=False)

    decorator = with_cache(
        key=la_cle,
        subdirectory="xy",
        cache_root=tmp_path,
    )
    fn = decorator(la_fonction)

    # Call with cache_policy=None - should use default (use)
    result1 = fn(1, 2, version=0, cache_policy=None)
    result2 = fn(1, 2, version=1, cache_policy=None)
    assert result1 == result2


@pytest.mark.freeze_time("2024-06-01")
def test_cache_read_valid_true_with_time_parsing_complex(tmp_path, caplog):
    cache = Cache(cache_root=tmp_path, subdirectory="", formatter=JSONFormatter)

    test_time1 = datetime(2024, 6, 1, 2, tzinfo=UTC)
    test_time2 = datetime(2024, 6, 1, 4, tzinfo=UTC)
    test_time3 = datetime(2024, 6, 1, 5, tzinfo=UTC)

    time_str1 = test_time1.strftime("%Y-%m-%d-%H-%M-%S")
    time_str2 = test_time2.strftime("%Y-%m-%d-%H-%M-%S")
    time_str3 = test_time3.strftime("%Y-%m-%d-%H-%M-%S")

    cache_file1 = tmp_path / f"test.{time_str1}.json"
    cache_file2 = tmp_path / f"test.{time_str2}.json"
    cache_file3 = tmp_path / f"test.{time_str3}.json"

    cache_file1.write_text('{"data": "value1"}')
    cache_file2.write_text('{"data": "value2"}')
    cache_file3.write_text('{"data": "value3"}')

    result1 = cache.read("test.{time}.json", at_time=test_time1, valid=True)
    assert result1 == {"data": "value1"}

    result2 = cache.read("test.{time}.json", at_time=test_time2, valid=True)
    assert result2 == {"data": "value2"}

    result3 = cache.read("test.{time}.json", at_time=test_time3, valid=True)
    assert result3 == {"data": "value3"}

    test_time_bad = datetime(2024, 6, 1, 6, tzinfo=UTC)
    time_str_bad = test_time_bad.strftime("%Y-%m-%d-%H-%M-%S")
    time_str_bad = time_str_bad[:-2] + "AA"

    cache_file_bad = tmp_path / f"test.{time_str_bad}.json"
    cache_file_bad.write_text("")

    with caplog.at_level(logging.WARNING):
        cache.read(
            "test.{time}.json",
            at_time=datetime(2024, 6, 1, 7, tzinfo=UTC),
            valid=timedelta(days=1),
        )
    assert caplog.messages[0].startswith("Could not parse time from cache file name")


def test_cache_save(tmp_path):
    cache = Cache(cache_root=tmp_path, subdirectory="", formatter=JSONFormatter)

    cache.save("test.json", {"data": "value"})

    # Verify the file was created
    cache_file = tmp_path / "test.json"
    assert cache_file.exists()
    assert cache_file.read_text() == '{"data": "value"}'

    cache.save("test-{time}.json", {"data": "value2"}, datetime(2024, 6, 1))

    cache_file = tmp_path / "test-2024-06-01-00-00-00.json"
    assert cache_file.exists()
    assert cache_file.read_text() == '{"data": "value2"}'


def test_key_function_returns_none(tmp_path):
    def key_returns_none(x, y, version):
        return None

    def test_function(x, y, version=0):
        return f"{x} * {y} = {x * y} [v{version}]"

    decorator = with_cache(
        key=key_returns_none,
        subdirectory="xy",
        cache_root=tmp_path,
    )
    fn = decorator(test_function)

    # First call - should compute and not cache
    result1 = fn(1, 2, version=0)
    assert result1 == "1 * 2 = 2 [v0]"

    # Second call with same parameters - should compute again (not use cache)
    result2 = fn(1, 2, version=0)
    assert result2 == "1 * 2 = 2 [v0]"

    # Third call with different parameters - should compute again
    result3 = fn(3, 4, version=0)
    assert result3 == "3 * 4 = 12 [v0]"

    # Verify no cache files were created since key function returns None
    cache_dir = tmp_path / "xy"
    if cache_dir.exists():
        assert len(list(cache_dir.iterdir())) == 0
    else:
        assert not cache_dir.exists()
