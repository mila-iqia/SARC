from datetime import UTC, datetime, timedelta, timezone
from zipfile import ZIP_LZMA, ZipFile

import gifnoc

from sarc.cache import Cache, CacheEntry
from sarc.utils import ensure_utc


def test_cache_entry_add_get_value(tmp_path):
    """Test CacheEntry add_value and get_value methods."""

    # Create a test zip file manually to test CacheEntry
    test_file = tmp_path / "test.zip"
    with ZipFile(test_file, "w", compression=ZIP_LZMA) as zf:
        zf.writestr("key1", b"value1")
        zf.writestr("key2", b"value2")

    with ZipFile(test_file, "r") as zf:
        entry = CacheEntry(zf, datetime.now(UTC))

        assert list(entry.items()) == [("key1", b"value1"), ("key2", b"value2")]


def test_cache_entry_add_value(tmp_path):
    """Test CacheEntry add_value method."""

    # Create a new zip file for writing
    test_file = tmp_path / "test_write.zip"
    with ZipFile(test_file, "x", compression=ZIP_LZMA) as zf:
        entry = CacheEntry(zf, datetime.now(UTC))

        # Add values
        entry.add_value("test_key", b"test_value")
        entry.add_value("another_key", b"another_value")

        # Close the entry
        entry.close()

    # Verify the zip file was created and contains the data
    with ZipFile(test_file, "r") as zf:
        assert zf.read("test_key") == b"test_value"
        assert zf.read("another_key") == b"another_value"
        assert zf.namelist() == ["test_key", "another_key"]


def test_cache_initialization(enabled_cache):
    """Test Cache class initialization."""
    cache = Cache("test_subdirectory")
    assert cache.subdirectory == "test_subdirectory"


def test_cache_cache_dir_property(tmp_path):
    """Test Cache cache_dir property with config override."""
    cache = Cache("test_subdir")

    with gifnoc.overlay({"sarc.cache": str(tmp_path)}):
        cache_dir = cache.cache_dir
        expected_dir = tmp_path / "test_subdir"
        assert cache_dir == expected_dir
        assert cache_dir.exists()


def test_cache_dir_from_date(tmp_path):
    """Test Cache _dir_from_date method."""

    with gifnoc.overlay({"sarc.cache": str(tmp_path)}):
        cache = Cache("test")
        test_date = datetime(2024, 3, 15, 10, 30, 45, tzinfo=UTC)

        result = cache._dir_from_date(tmp_path, test_date)
        expected = tmp_path / "2024" / "03" / "15"
        assert result == expected


def test_cache_create_entry(tmp_path):
    """Test Cache create_entry method."""

    cache = Cache("test_cache")

    with gifnoc.overlay({"sarc.cache": str(tmp_path)}):
        test_time = datetime(2024, 3, 15, 10, 30, 45, tzinfo=UTC)
        with cache.create_entry(test_time) as entry:
            # Verify the entry is a CacheEntry
            assert isinstance(entry, CacheEntry)

            # Add some data and close
            entry.add_value("test_key", b"test_data")

        # Verify the file was created in the expected location
        expected_file = tmp_path / "test_cache" / "2024" / "03" / "15" / "10:30:45.000"
        assert expected_file.exists()


def test_cache_multiple_key_same_name(enabled_cache):
    cache = Cache("test_entries")
    with cache.create_entry(datetime(2024, 6, 1, tzinfo=UTC)) as ce:
        ce.add_value("key", b"val1")
        ce.add_value("key", b"val2")

    ce = list(cache.read_from(datetime(2024, 5, 30, tzinfo=UTC)))[0]
    assert list(ce.items()) == [("key", b"val1"), ("key", b"val2")]


def test_cache_save(tmp_path):
    """Test Cache save method."""

    cache = Cache("test_cache")

    with gifnoc.overlay({"sarc.cache": str(tmp_path)}):
        test_time = datetime(2024, 3, 15, 10, 30, 45, tzinfo=UTC)
        test_data = b"Hello, this is test data!"

        cache.save("test_key", test_time, test_data)

        # Verify the file was created
        expected_file = tmp_path / "test_cache" / "2024" / "03" / "15" / "10:30:45.000"
        assert expected_file.exists()

        # Verify the data can be read back
        with ZipFile(expected_file, "r") as zf:
            assert zf.read("test_key") == test_data


def test_cache_save_multiple_keys(tmp_path):
    """Test Cache save method with multiple keys in same entry."""

    cache = Cache("test_cache")

    with gifnoc.overlay({"sarc.cache": str(tmp_path)}):
        test_time = datetime(2024, 3, 15, 10, 30, 45, tzinfo=UTC)

        # Create entry and add multiple keys
        with cache.create_entry(test_time) as entry:
            entry.add_value("key1", b"data1")
            entry.add_value("key2", b"data2")
            entry.add_value("key3", b"data3")

        # Verify the file was created
        expected_file = tmp_path / "test_cache" / "2024" / "03" / "15" / "10:30:45.000"
        assert expected_file.exists()

        # Verify all data can be read back
        with ZipFile(expected_file, "r") as zf:
            assert zf.read("key1") == b"data1"
            assert zf.read("key2") == b"data2"
            assert zf.read("key3") == b"data3"
            assert set(zf.namelist()) == {"key1", "key2", "key3"}


def test_cache_paths_from_single_day(enabled_cache):
    """Test Cache _paths_from method with files from a single day."""

    cache = Cache("test_cache")

    # Create test files for the same day
    base_time = datetime(2024, 3, 15, 10, 0, 0, tzinfo=UTC)

    # Create files at different times
    times = [
        datetime(2024, 3, 15, 9, 30, 0, tzinfo=UTC),  # Before from_time
        datetime(2024, 3, 15, 10, 15, 0, tzinfo=UTC),  # After from_time
        datetime(2024, 3, 15, 11, 0, 0, tzinfo=UTC),  # After from_time
    ]

    for time in times:
        cache.save("test_key", time, b"test_data")

    # Test _paths_from starting from base_time
    paths = list(path for path, _ in cache._paths_from(base_time))

    # Should only get files from 10:15 and 11:00 (not 9:30)
    assert len(paths) == 2

    # Verify the paths are sorted correctly
    path_names = [p.name for p in paths]
    assert "10:15:00.000" in path_names
    assert "11:00:00.000" in path_names


def test_cache_paths_from_multiple_days(enabled_cache):
    """Test Cache _paths_from method with files from multiple days."""

    cache = Cache("test_cache")

    # Create test files for different days
    times = [
        datetime(2024, 3, 14, 23, 0, 0, tzinfo=UTC),  # Day before
        datetime(2024, 3, 15, 10, 0, 0, tzinfo=UTC),  # Target day
        datetime(2024, 3, 15, 15, 0, 0, tzinfo=UTC),  # Same day, later
        datetime(2024, 3, 16, 8, 0, 0, tzinfo=UTC),  # Next day
        datetime(2024, 3, 17, 12, 0, 0, tzinfo=UTC),  # Day after next
    ]

    for time in times:
        cache.save("test_key", time, b"test_data")

    # Test _paths_from starting from 2024-03-15 10:00
    from_time = datetime(2024, 3, 15, 10, 0, 0, tzinfo=UTC)
    paths = list(path for path, _ in cache._paths_from(from_time))

    # Should get files from 15:00 on 3/15, and all files from 3/16 and 3/17
    assert len(paths) == 3

    # Verify we get the expected files
    path_names = [p.name for p in paths]
    assert "15:00:00.000" in path_names
    assert "08:00:00.000" in path_names  # From 3/16
    assert "12:00:00.000" in path_names  # From 3/17


def test_cache_entry_datetime(enabled_cache):
    """Test Cache _fetch_date method."""
    cache = Cache("test_cache")
    # Create test files for different days
    times = [
        datetime(2024, 3, 14, 23, 0, 0, tzinfo=UTC),  # Day before
        datetime(2024, 3, 15, 10, 0, 0, tzinfo=UTC),  # Target day
        datetime(2024, 3, 15, 15, 0, 0, tzinfo=UTC),  # Same day, later
        datetime(2024, 3, 16, 8, 0, 0, tzinfo=UTC),  # Next day
        datetime(2024, 3, 17, 12, 0, 0, tzinfo=UTC),  # Day after next
    ]
    for time in times:
        cache.save("test_key", time, b"test_data")

    # parse cache entries
    entries = list(cache.read_from(datetime(2024, 3, 15, 10, 0, 0, tzinfo=UTC)))
    assert len(entries) == 3
    assert entries[0].get_entry_datetime() == times[2]
    assert entries[1].get_entry_datetime() == times[3]
    assert entries[2].get_entry_datetime() == times[4]


def test_cache_latest_entry(enabled_cache):
    """Test Cache latest_entry method."""
    cache = Cache("test_cache")
    assert cache.latest_entry() is None

    cache.save("test_key", datetime(2024, 3, 15, 10, 0, 0, tzinfo=UTC), b"test_data")
    assert cache.latest_entry().get_entry_datetime() == datetime(
        2024, 3, 15, 10, 0, 0, tzinfo=UTC
    )

    cache.save("test_key", datetime(2024, 3, 16, 11, 0, 0, tzinfo=UTC), b"test_data")
    assert cache.latest_entry().get_entry_datetime() == datetime(
        2024, 3, 16, 11, 0, 0, tzinfo=UTC
    )

    cache.save("test_key", datetime(2024, 3, 17, 12, 0, 0, tzinfo=UTC), b"test_data")
    assert cache.latest_entry().get_entry_datetime() == datetime(
        2024, 3, 17, 12, 0, 0, tzinfo=UTC
    )


def test_cache_read_from(enabled_cache):
    """Test Cache read_from method."""

    cache = Cache("test_cache")

    # Create test entries with different data
    times_and_data = [
        (datetime(2024, 3, 15, 9, 0, 0, tzinfo=UTC), {"key1": b"data1"}),
        (datetime(2024, 3, 15, 10, 0, 0, tzinfo=UTC), {"key2": b"data2"}),
        (datetime(2024, 3, 15, 11, 0, 0, tzinfo=UTC), {"key3": b"data3"}),
    ]

    for time, data in times_and_data:
        with cache.create_entry(time) as entry:
            for key, value in data.items():
                entry.add_value(key, value)

    # Read from 10:00 onwards
    from_time = datetime(2024, 3, 15, 10, 0, 0, tzinfo=UTC)
    entries = list(cache.read_from(from_time))

    # Should get 1 entry (11:00)
    assert len(entries) == 1

    # verify the data
    assert list(entries[0].items()) == [("key3", b"data3")]

    # try to read starting the day before
    entries = list(cache.read_from(datetime(2024, 3, 14, 0, 0, 0, tzinfo=UTC)))
    assert len(entries) == 3


def test_cache_read_from_with_multiple_keys_per_entry(enabled_cache):
    """Test Cache read_from method with multiple keys per entry."""

    cache = Cache("test_cache")

    # Create entries with multiple keys
    time1 = datetime(2024, 3, 15, 10, 0, 0, tzinfo=UTC)
    with cache.create_entry(time1) as entry1:
        entry1.add_value("user1", b"user1_data")
        entry1.add_value("user2", b"user2_data")

    time2 = datetime(2024, 3, 15, 11, 0, 0, tzinfo=UTC)
    with cache.create_entry(time2) as entry2:
        entry2.add_value("user3", b"user3_data")

    # Read from 10:00 onwards
    from_time = datetime(2024, 3, 15, 9, 59, 59, tzinfo=UTC)
    entries = list(cache.read_from(from_time))

    assert len(entries) == 2

    assert list(entries[0].items()) == [
        ("user1", b"user1_data"),
        ("user2", b"user2_data"),
    ]

    # Check second entry has 1 key
    assert list(entries[1].items()) == [("user3", b"user3_data")]


def test_cache_ensure_utc():
    """Test ensure_utc function."""
    # Test with UTC datetime
    utc_time = datetime(2024, 3, 15, 10, 0, 0, tzinfo=timezone.utc)
    result = ensure_utc(utc_time)
    assert result == utc_time

    # Test with non-UTC datetime
    est_time = datetime(2024, 3, 15, 5, 0, 0, tzinfo=timezone(timedelta(hours=-5)))
    result = ensure_utc(est_time)
    expected = datetime(2024, 3, 15, 10, 0, 0, tzinfo=timezone.utc)
    assert result == expected


def test_cache_with_different_subdirectories(tmp_path):
    """Test Cache with different subdirectories."""
    from datetime import UTC, datetime
    from zipfile import ZipFile

    cache1 = Cache("subdir1")
    cache2 = Cache("subdir2")

    with gifnoc.overlay({"sarc.cache": str(tmp_path)}):
        test_time = datetime(2024, 3, 15, 10, 0, 0, tzinfo=UTC)

        # Save data to different caches
        cache1.save("key1", test_time, b"data1")
        cache2.save("key2", test_time, b"data2")

        # Verify files are in different subdirectories
        file1 = tmp_path / "subdir1" / "2024" / "03" / "15" / "10:00:00.000"
        file2 = tmp_path / "subdir2" / "2024" / "03" / "15" / "10:00:00.000"

        assert file1.exists()
        assert file2.exists()

        # Verify different data
        with ZipFile(file1, "r") as zf:
            assert zf.read("key1") == b"data1"

        with ZipFile(file2, "r") as zf:
            assert zf.read("key2") == b"data2"


def test_cache_proper_iteration(enabled_cache):
    cache = Cache("datetest")

    # Fill cache
    dates = [
        datetime(2020, 5, 1, tzinfo=UTC),
        datetime(2020, 5, 2, tzinfo=UTC),
        datetime(2021, 1, 1, tzinfo=UTC),
        datetime(2021, 1, 2, tzinfo=UTC),
        datetime(2021, 2, 1, tzinfo=UTC),
        datetime(2021, 12, 1, tzinfo=UTC),
        datetime(2023, 1, 1, tzinfo=UTC),
        datetime(2023, 4, 1, tzinfo=UTC),
        datetime(2023, 9, 1, tzinfo=UTC),
    ]
    for date in dates:
        try:
            with cache.create_entry(date) as cache_entry:
                cache_entry.add_value("cache_date", date.isoformat().encode("utf-8"))
        except FileExistsError:
            pass

    data1 = list(cache.read_from(datetime(2020, 1, 1, tzinfo=UTC)))
    assert len(data1) == 9
    data2 = list(cache.read_from(datetime(2022, 4, 1, tzinfo=UTC)))
    assert len(data2) == 3
