from datetime import UTC, datetime

import pytest

from sarc.core.models.validators import DateMatchError, DateOverlapError, ValidField


class TestValidField:
    """Test cases for ValidField class."""

    def test_empty_field_get_value_raises_error(self):
        """Test that getting value from empty field raises DateMatchError."""
        field = ValidField[str]()
        test_date = datetime(2023, 1, 1, tzinfo=UTC)

        with pytest.raises(DateMatchError):
            field.get_value(test_date)

    def test_insert_single_value_with_defaults(self):
        """Test inserting a single value with default start/end times."""
        field = ValidField[str]()
        field.insert("test_value")

        # Should be valid for any date between START_TIME and END_TIME
        test_date = datetime(2023, 1, 1, tzinfo=UTC)
        assert field.get_value(test_date) == "test_value"
        assert field.get_value() == "test_value"  # Current time

    def test_insert_value_with_specific_dates(self):
        """Test inserting value with specific start and end dates."""
        field = ValidField[str]()
        start = datetime(2023, 1, 1, tzinfo=UTC)
        end = datetime(2023, 12, 31, tzinfo=UTC)

        field.insert("test_value", start, end)

        # Should be valid within the range
        test_date = datetime(2023, 6, 1, tzinfo=UTC)
        assert field.get_value(test_date) == "test_value"

        # Should not be valid outside the range
        before_start = datetime(2022, 12, 31, tzinfo=UTC)
        after_end = datetime(2024, 1, 1, tzinfo=UTC)

        with pytest.raises(DateMatchError):
            field.get_value(before_start)
        with pytest.raises(DateMatchError):
            field.get_value(after_end)

    def test_insert_multiple_non_overlapping_values(self):
        """Test inserting multiple non-overlapping values."""
        field = ValidField[str]()

        # Insert in random order to test sorting
        field.insert(
            "value2",
            datetime(2023, 6, 1, tzinfo=UTC),
            datetime(2023, 12, 31, tzinfo=UTC),
        )
        field.insert(
            "value1",
            datetime(2023, 1, 1, tzinfo=UTC),
            datetime(2023, 5, 31, tzinfo=UTC),
        )
        field.insert(
            "value3",
            datetime(2024, 1, 1, tzinfo=UTC),
            datetime(2024, 12, 31, tzinfo=UTC),
        )

        assert len(field.values) == 3
        assert field.get_value(datetime(2023, 3, 1, tzinfo=UTC)) == "value1"
        assert field.get_value(datetime(2023, 8, 1, tzinfo=UTC)) == "value2"
        assert field.get_value(datetime(2024, 6, 1, tzinfo=UTC)) == "value3"

    def test_insert_overlapping_same_values_merges(self):
        """Test that overlapping values with same content get merged."""
        field = ValidField[str]()

        field.insert(
            "same_value",
            datetime(2023, 1, 1, tzinfo=UTC),
            datetime(2023, 6, 30, tzinfo=UTC),
        )
        field.insert(
            "same_value",
            datetime(2023, 6, 1, tzinfo=UTC),
            datetime(2023, 12, 31, tzinfo=UTC),
        )

        # Should be merged into a single continuous period
        assert len(field.values) == 1
        assert field.get_value(datetime(2023, 3, 1, tzinfo=UTC)) == "same_value"
        assert field.get_value(datetime(2023, 8, 1, tzinfo=UTC)) == "same_value"

    def test_insert_overlapping_different_values_raises_error(self):
        """Test that overlapping values with different content raise DateOverlapError."""
        field = ValidField[str]()

        field.insert(
            "value1",
            datetime(2023, 1, 1, tzinfo=UTC),
            datetime(2023, 6, 30, tzinfo=UTC),
        )

        with pytest.raises(DateOverlapError):
            field.insert(
                "value2",
                datetime(2023, 6, 1, tzinfo=UTC),
                datetime(2023, 12, 31, tzinfo=UTC),
            )

        assert len(field.values) == 1
        assert field.get_value(datetime(2023, 3, 1, tzinfo=UTC)) == "value1"

    def test_insert_adjacent_values_same_content_merges(self):
        """Test that adjacent values with same content get merged."""
        field = ValidField[str]()

        field.insert(
            "same_value",
            datetime(2023, 1, 1, tzinfo=UTC),
            datetime(2023, 6, 1, tzinfo=UTC),
        )
        field.insert(
            "same_value",
            datetime(2023, 6, 1, tzinfo=UTC),
            datetime(2023, 12, 31, tzinfo=UTC),
        )

        # Should be merged into a single continuous period
        assert len(field.values) == 1
        assert field.get_value(datetime(2023, 3, 1, tzinfo=UTC)) == "same_value"
        assert field.get_value(datetime(2023, 8, 1, tzinfo=UTC)) == "same_value"

    def test_insert_adjacent_values_different_content_no_merge(self):
        """Test that adjacent values with different content don't merge."""
        field = ValidField[str]()

        field.insert(
            "value1",
            datetime(2023, 1, 1, tzinfo=UTC),
            datetime(2023, 6, 1, tzinfo=UTC),
        )
        field.insert(
            "value2",
            datetime(2023, 6, 1, tzinfo=UTC),
            datetime(2023, 12, 31, tzinfo=UTC),
        )

        assert len(field.values) == 2
        assert field.get_value(datetime(2023, 3, 1, tzinfo=UTC)) == "value1"
        assert field.get_value(datetime(2023, 8, 1, tzinfo=UTC)) == "value2"

    def test_special_case_current_value_adjustment(self):
        """Test special case where current value with END_TIME gets adjusted."""
        field = ValidField[str]()

        # Insert a "current" value that goes to END_TIME
        field.insert("current_value", datetime(2023, 1, 1, tzinfo=UTC))

        # Insert a new value that starts later - should adjust the current value's end
        new_start = datetime(2024, 1, 1, tzinfo=UTC)
        field.insert("new_value", new_start)

        # Current value should now end at new_start
        assert field.values[1].valid_end == new_start
        assert field.get_value(datetime(2023, 6, 1, tzinfo=UTC)) == "current_value"
        assert field.get_value(datetime(2024, 6, 1, tzinfo=UTC)) == "new_value"

        # Right at the boundary should get the new value since end is exclusive for old, start is inclusive for new
        assert field.get_value(new_start) == "new_value"

    def test_get_value_with_none_uses_current_time(self):
        """Test that get_value with None uses current time."""
        field = ValidField[str]()
        field.insert("current_value")  # Uses default START_TIME to END_TIME

        # Should work with current time
        assert field.get_value(None) == "current_value"
        assert field.get_value() == "current_value"

    def test_merge_with_empty_field(self):
        """Test merging with an empty field."""
        field1 = ValidField[str]()
        field2 = ValidField[str]()

        field1.insert(
            "value1",
            datetime(2023, 1, 1, tzinfo=UTC),
            datetime(2023, 6, 30, tzinfo=UTC),
        )

        field1.merge_with(field2)  # Should not change anything
        assert len(field1.values) == 1
        assert field1.get_value(datetime(2023, 3, 1, tzinfo=UTC)) == "value1"

    def test_merge_with_non_overlapping_fields(self):
        """Test merging with a field that has non-overlapping values."""
        field1 = ValidField[str]()
        field2 = ValidField[str]()

        field1.insert(
            "value1",
            datetime(2023, 1, 1, tzinfo=UTC),
            datetime(2023, 6, 30, tzinfo=UTC),
        )
        field2.insert(
            "value2",
            datetime(2023, 7, 1, tzinfo=UTC),
            datetime(2023, 12, 31, tzinfo=UTC),
        )

        field1.merge_with(field2)

        assert len(field1.values) == 2
        assert field1.get_value(datetime(2023, 3, 1, tzinfo=UTC)) == "value1"
        assert field1.get_value(datetime(2023, 9, 1, tzinfo=UTC)) == "value2"

    def test_merge_with_overlapping_same_values(self):
        """Test merging with a field that has overlapping same values."""
        field1 = ValidField[str]()
        field2 = ValidField[str]()

        field1.insert(
            "same_value",
            datetime(2023, 1, 1, tzinfo=UTC),
            datetime(2023, 6, 30, tzinfo=UTC),
        )
        field2.insert(
            "same_value",
            datetime(2023, 6, 1, tzinfo=UTC),
            datetime(2023, 12, 31, tzinfo=UTC),
        )

        field1.merge_with(field2)

        # Should be merged into one continuous period
        assert len(field1.values) == 1
        assert field1.get_value(datetime(2023, 3, 1, tzinfo=UTC)) == "same_value"
        assert field1.get_value(datetime(2023, 9, 1, tzinfo=UTC)) == "same_value"

    def test_merge_with_overlapping_different_values_raises_error(self):
        """Test merging with a field that has overlapping different values."""
        field1 = ValidField[str]()
        field2 = ValidField[str]()

        field1.insert(
            "value1",
            datetime(2023, 1, 1, tzinfo=UTC),
            datetime(2023, 6, 30, tzinfo=UTC),
        )
        field2.insert(
            "value2",
            datetime(2023, 6, 1, tzinfo=UTC),
            datetime(2023, 12, 31, tzinfo=UTC),
        )

        with pytest.raises(DateOverlapError):
            field1.merge_with(field2)
        assert len(field1.values) == 1

    def test_timezone_validation_in_insert(self):
        """Test that insert validates timezone information."""
        field = ValidField[str]()

        # These should work (UTC timezone)
        field.insert(
            "value",
            datetime(2023, 1, 1, tzinfo=UTC),
            datetime(2023, 12, 31, tzinfo=UTC),
        )

        # These should fail (wrong timezone or no timezone)
        naive_datetime = datetime(2023, 1, 1)  # No timezone
        with pytest.raises(AssertionError):
            field.insert("value", naive_datetime, datetime(2023, 12, 31, tzinfo=UTC))

        with pytest.raises(AssertionError):
            field.insert("value", datetime(2023, 1, 1, tzinfo=UTC), naive_datetime)

    def test_timezone_validation_in_get_value(self):
        """Test that get_value validates timezone information."""
        field = ValidField[str]()
        field.insert("value")

        # Should work with UTC
        assert field.get_value(datetime(2023, 1, 1, tzinfo=UTC)) == "value"

        # Should fail with naive datetime
        naive_datetime = datetime(2023, 1, 1)
        with pytest.raises(AssertionError):
            field.get_value(naive_datetime)

    def test_start_must_be_before_end(self):
        """Test that start time must be before end time."""
        field = ValidField[str]()

        start = datetime(2023, 6, 1, tzinfo=UTC)
        end = datetime(2023, 1, 1, tzinfo=UTC)  # Before start

        with pytest.raises(AssertionError):
            field.insert("value", start, end)

    def test_complex_merge(self):
        """Test a complex scenario with multiple merges and overlaps."""
        field1 = ValidField[str]()
        field2 = ValidField[str]()

        # Field1: value1 from Jan-Mar, value2 from Jul-Sep
        field1.insert(
            "value1",
            datetime(2023, 1, 1, tzinfo=UTC),
            datetime(2023, 3, 31, tzinfo=UTC),
        )
        field1.insert(
            "value2",
            datetime(2023, 7, 1, tzinfo=UTC),
            datetime(2023, 9, 30, tzinfo=UTC),
        )

        # Field2: value1 from Feb-Apr (overlaps and extends field1's value1), value3 from May-Jun
        field2.insert(
            "value1",
            datetime(2023, 2, 1, tzinfo=UTC),
            datetime(2023, 4, 30, tzinfo=UTC),
        )
        field2.insert(
            "value3",
            datetime(2023, 5, 1, tzinfo=UTC),
            datetime(2023, 6, 30, tzinfo=UTC),
        )

        field1.merge_with(field2)

        assert len(field1.values) == 3
        # value1 should be merged and extended from Jan to Apr
        assert field1.get_value(datetime(2023, 1, 15, tzinfo=UTC)) == "value1"
        assert field1.get_value(datetime(2023, 4, 15, tzinfo=UTC)) == "value1"

        # value3 should be in May-Jun
        assert field1.get_value(datetime(2023, 5, 15, tzinfo=UTC)) == "value3"

        # value2 should still be in Jul-Sep
        assert field1.get_value(datetime(2023, 8, 15, tzinfo=UTC)) == "value2"

    def test_edge_case_insert_at_boundaries(self):
        """Test inserting values exactly at validity boundaries."""
        field = ValidField[str]()

        # Insert initial value
        field.insert(
            "value1", datetime(2023, 1, 1, tzinfo=UTC), datetime(2023, 6, 1, tzinfo=UTC)
        )

        # Insert value that starts exactly when the first ends
        field.insert(
            "value2",
            datetime(2023, 6, 1, tzinfo=UTC),
            datetime(2023, 12, 31, tzinfo=UTC),
        )

        # Test boundary behavior - end is exclusive
        assert (
            field.get_value(datetime(2023, 5, 31, 23, 59, 59, tzinfo=UTC)) == "value1"
        )
        assert field.get_value(datetime(2023, 6, 1, tzinfo=UTC)) == "value2"

    def test_recursive_merge(self):
        field = ValidField[str]()

        # Create a scenario where recursive merging is needed
        field.insert(
            "same_value",
            datetime(2023, 3, 1, tzinfo=UTC),
            datetime(2023, 6, 1, tzinfo=UTC),
        )
        field.insert(
            "same_value",
            datetime(2023, 7, 1, tzinfo=UTC),
            datetime(2023, 9, 1, tzinfo=UTC),
        )

        # Insert a value that overlaps both and should trigger recursive merging
        field.insert(
            "same_value",
            datetime(2023, 1, 1, tzinfo=UTC),
            datetime(2023, 8, 1, tzinfo=UTC),
        )

        # Should all be merged into one continuous period
        assert len(field.values) == 1
        assert field.get_value(datetime(2023, 2, 1, tzinfo=UTC)) == "same_value"
        assert field.get_value(datetime(2023, 4, 1, tzinfo=UTC)) == "same_value"
        assert field.get_value(datetime(2023, 7, 15, tzinfo=UTC)) == "same_value"

    def test_recursive_merge_overlap(self):
        field = ValidField[str]()

        # Create a scenario where recursive merging is needed
        field.insert(
            "value1",
            datetime(2023, 3, 1, tzinfo=UTC),
            datetime(2023, 6, 1, tzinfo=UTC),
        )
        field.insert(
            "value2",
            datetime(2023, 7, 1, tzinfo=UTC),
            datetime(2023, 9, 1, tzinfo=UTC),
        )

        with pytest.raises(DateOverlapError):
            field.insert(
                "value2",
                datetime(2023, 1, 1, tzinfo=UTC),
                datetime(2023, 8, 1, tzinfo=UTC),
            )

        assert len(field.values) == 2
        assert field.values[0].value == "value2"
        assert field.values[0].valid_start == datetime(2023, 7, 1, tzinfo=UTC)
        assert field.values[0].valid_end == datetime(2023, 9, 1, tzinfo=UTC)

    def test_recursive_merge_failure_with_restore(self):
        """Test recursive merge that fails and restores the removed tag."""
        field = ValidField[str]()

        # Insert values in a way that will trigger recursive merge
        # value1: Jan-Mar
        field.insert(
            "value1",
            datetime(2023, 1, 1, tzinfo=UTC),
            datetime(2023, 3, 31, tzinfo=UTC),
        )
        # value2: Apr-Jun (same value as value1, will be merged)
        field.insert(
            "value1",
            datetime(2023, 4, 1, tzinfo=UTC),
            datetime(2023, 6, 30, tzinfo=UTC),
        )
        # value3: Jul-Sep (different value, will cause conflict)
        field.insert(
            "value3",
            datetime(2023, 7, 1, tzinfo=UTC),
            datetime(2023, 9, 30, tzinfo=UTC),
        )

        # Now try to insert a value that starts before value1 but overlaps with value3
        # This should trigger recursive merge, but fail due to overlap with value3
        with pytest.raises(DateOverlapError):
            field.insert(
                "value1",
                datetime(2022, 12, 1, tzinfo=UTC),
                datetime(2023, 8, 1, tzinfo=UTC),
            )

        # The field should remain unchanged after the failed insertion
        # Check that we still have the same number of values
        assert len(field.values) == 3

        # Check that we can still get the expected values at their expected times
        assert field.get_value(datetime(2023, 2, 1, tzinfo=UTC)) == "value1"
        assert field.get_value(datetime(2023, 5, 1, tzinfo=UTC)) == "value1"
        assert field.get_value(datetime(2023, 8, 1, tzinfo=UTC)) == "value3"

    def test_recursive_merge_success_with_overlap(self):
        """Test recursive merge that succeeds but encounters overlap during recursive insertion."""
        field = ValidField[str]()

        field.insert(
            "value1",
            datetime(2023, 1, 1, tzinfo=UTC),
            datetime(2023, 3, 31, tzinfo=UTC),
        )
        field.insert(
            "value1",
            datetime(2023, 4, 1, tzinfo=UTC),
            datetime(2023, 6, 30, tzinfo=UTC),
        )
        field.insert(
            "value2",
            datetime(2023, 10, 1, tzinfo=UTC),
            datetime(2023, 12, 31, tzinfo=UTC),
        )

        field.insert(
            "value1",
            datetime(2022, 12, 1, tzinfo=UTC),
            datetime(2023, 8, 1, tzinfo=UTC),
        )

        # The field should remain unchanged after the failed insertion
        assert len(field.values) == 2
        assert field.get_value(datetime(2023, 2, 1, tzinfo=UTC)) == "value1"
        assert field.get_value(datetime(2023, 5, 1, tzinfo=UTC)) == "value1"
        assert field.get_value(datetime(2023, 11, 1, tzinfo=UTC)) == "value2"

    def test_merge_with_truncate_clips_start(self):
        """Test merge with truncate=True clips overlapping values at start."""
        field1 = ValidField[str]()
        field2 = ValidField[str]()

        # Field1 has value1 from Jan-Jun
        field1.insert(
            "value1",
            datetime(2023, 1, 1, tzinfo=UTC),
            datetime(2023, 6, 30, tzinfo=UTC),
        )

        # Field2 has value2 from Mar-Sep (overlaps with field1)
        field2.insert(
            "value2",
            datetime(2023, 3, 1, tzinfo=UTC),
            datetime(2023, 9, 30, tzinfo=UTC),
        )

        # With truncate=True, value2 should be clipped to start at Jun 30
        field1.merge_with(field2, truncate=True)

        assert len(field1.values) == 2
        # Original value1 should be unchanged
        assert field1.get_value(datetime(2023, 4, 1, tzinfo=UTC)) == "value1"
        # value2 should start after value1 ends
        assert field1.get_value(datetime(2023, 7, 1, tzinfo=UTC)) == "value2"

        # Check that value2 was clipped at the start
        assert field1.values[0].value == "value2"
        assert field1.values[0].valid_start == datetime(2023, 6, 30, tzinfo=UTC)

    def test_merge_with_truncate_clips_end(self):
        """Test merge with truncate=True clips overlapping values at end."""
        field1 = ValidField[str]()
        field2 = ValidField[str]()

        # Field1 has value1 from Jul-Dec
        field1.insert(
            "value1",
            datetime(2023, 7, 1, tzinfo=UTC),
            datetime(2023, 12, 31, tzinfo=UTC),
        )

        # Field2 has value2 from Jan-Sep (overlaps with field1)
        field2.insert(
            "value2",
            datetime(2023, 1, 1, tzinfo=UTC),
            datetime(2023, 9, 30, tzinfo=UTC),
        )

        # With truncate=True, value2 should be clipped to end at Jul 1
        field1.merge_with(field2, truncate=True)

        assert len(field1.values) == 2
        # value2 should be clipped at the end
        assert field1.get_value(datetime(2023, 4, 1, tzinfo=UTC)) == "value2"
        # Original value1 should be unchanged
        assert field1.get_value(datetime(2023, 9, 1, tzinfo=UTC)) == "value1"

        # Check that value2 was clipped at the end
        assert field1.values[1].value == "value2"
        assert field1.values[1].valid_end == datetime(2023, 7, 1, tzinfo=UTC)

    def test_merge_with_truncate_splits_overlapping_value(self):
        """Test merge with truncate=True splits a value that fully overlaps existing one."""
        field1 = ValidField[str]()
        field2 = ValidField[str]()

        # Field1 has value1 from Apr-Aug
        field1.insert(
            "value1",
            datetime(2023, 4, 1, tzinfo=UTC),
            datetime(2023, 8, 31, tzinfo=UTC),
        )

        # Field2 has value2 from Jan-Dec (fully overlaps field1's value)
        field2.insert(
            "value2",
            datetime(2023, 1, 1, tzinfo=UTC),
            datetime(2023, 12, 31, tzinfo=UTC),
        )

        # With truncate=True, value2 should be split into two parts
        field1.merge_with(field2, truncate=True)

        assert len(field1.values) == 3

        # Should have value2 from Jan-Apr, value1 from Apr-Aug, value2 from Aug-Dec
        assert field1.get_value(datetime(2023, 2, 1, tzinfo=UTC)) == "value2"
        assert field1.get_value(datetime(2023, 6, 1, tzinfo=UTC)) == "value1"
        assert field1.get_value(datetime(2023, 10, 1, tzinfo=UTC)) == "value2"

        # One part should end at Apr 1, other should start at Aug 31
        assert field1.values[2].valid_end == datetime(2023, 4, 1, tzinfo=UTC)
        assert field1.values[0].valid_start == datetime(2023, 8, 31, tzinfo=UTC)

    def test_merge_with_truncate_completely_overlapped_value_dropped(self):
        """Test that a completely overlapped value gets dropped when truncated."""
        field1 = ValidField[str]()
        field2 = ValidField[str]()

        # Field1 has value1 from Jan-Dec
        field1.insert(
            "value1",
            datetime(2023, 1, 1, tzinfo=UTC),
            datetime(2023, 12, 31, tzinfo=UTC),
        )

        # Field2 has value2 from Mar-Jun (completely within field1's range)
        field2.insert(
            "value2",
            datetime(2023, 3, 1, tzinfo=UTC),
            datetime(2023, 6, 30, tzinfo=UTC),
        )

        # With truncate=True, value2 should be completely dropped
        field1.merge_with(field2, truncate=True)

        assert len(field1.values) == 1
        assert field1.get_value(datetime(2023, 4, 1, tzinfo=UTC)) == "value1"

    def test_merge_with_truncate_multiple_overlaps(self):
        """Test merge with truncate=True handling multiple overlapping values."""
        field1 = ValidField[str]()
        field2 = ValidField[str]()

        # Field1 has non-overlapping values
        field1.insert(
            "existing1",
            datetime(2023, 2, 1, tzinfo=UTC),
            datetime(2023, 4, 1, tzinfo=UTC),
        )
        field1.insert(
            "existing2",
            datetime(2023, 6, 1, tzinfo=UTC),
            datetime(2023, 8, 1, tzinfo=UTC),
        )
        field1.insert(
            "existing3",
            datetime(2023, 10, 1, tzinfo=UTC),
            datetime(2023, 12, 1, tzinfo=UTC),
        )

        # Field2 has values that overlap with multiple existing values
        field2.insert(
            "new1",
            datetime(2023, 1, 1, tzinfo=UTC),
            datetime(2023, 3, 1, tzinfo=UTC),  # Overlaps with existing1
        )
        field2.insert(
            "new2",
            datetime(2023, 5, 1, tzinfo=UTC),
            datetime(2023, 9, 1, tzinfo=UTC),  # Overlaps with existing2
        )

        field1.merge_with(field2, truncate=True)

        assert len(field1.values) == 6
        # new1 should be clipped to end before existing1
        assert field1.get_value(datetime(2023, 1, 15, tzinfo=UTC)) == "new1"
        assert field1.get_value(datetime(2023, 2, 15, tzinfo=UTC)) == "existing1"

        # new2 should be clipped to fit around existing2
        assert field1.get_value(datetime(2023, 5, 15, tzinfo=UTC)) == "new2"
        assert field1.get_value(datetime(2023, 7, 1, tzinfo=UTC)) == "existing2"
        assert field1.get_value(datetime(2023, 8, 15, tzinfo=UTC)) == "new2"

    def test_merge_with_truncate_false_still_raises_error(self):
        """Test that merge_with with truncate=False (default) still raises errors."""
        field1 = ValidField[str]()
        field2 = ValidField[str]()

        field1.insert(
            "value1",
            datetime(2023, 1, 1, tzinfo=UTC),
            datetime(2023, 6, 30, tzinfo=UTC),
        )
        field2.insert(
            "value2",
            datetime(2023, 6, 1, tzinfo=UTC),
            datetime(2023, 12, 31, tzinfo=UTC),
        )

        # Without truncate=True, should still raise DateOverlapError
        with pytest.raises(DateOverlapError):
            field1.merge_with(field2, truncate=False)

        # Default behavior should also raise error
        with pytest.raises(DateOverlapError):
            field1.merge_with(field2)

    def test_merge_with_truncate_same_values_still_merges(self):
        """Test that truncate=True still merges overlapping same values."""
        field1 = ValidField[str]()
        field2 = ValidField[str]()

        field1.insert(
            "same_value",
            datetime(2023, 1, 1, tzinfo=UTC),
            datetime(2023, 6, 30, tzinfo=UTC),
        )
        field2.insert(
            "same_value",
            datetime(2023, 6, 1, tzinfo=UTC),
            datetime(2023, 12, 31, tzinfo=UTC),
        )

        field1.merge_with(field2, truncate=True)

        # Should still merge same values even with truncate=True
        assert len(field1.values) == 1
        assert field1.get_value(datetime(2023, 3, 1, tzinfo=UTC)) == "same_value"
        assert field1.get_value(datetime(2023, 9, 1, tzinfo=UTC)) == "same_value"
