from datetime import UTC, datetime

import pytest
from sqlmodel import select

from sarc.db.users import CredentialsDB, CredentialsValid, DateOverlapError, UserDB


def dt(year: int, month: int = 1, day: int = 1) -> datetime:
    return datetime(year, month, day, tzinfo=UTC)


@pytest.fixture
def user_and_field(empty_read_write_db):
    sess = empty_read_write_db
    user = UserDB(display_name="Test User", email="test@example.com")
    sess.add(user)
    sess.flush()
    field = CredentialsValid(sess, user.id, "test")
    return sess, user, field


def get_records(sess, user_id: int) -> list[CredentialsDB]:
    return sess.exec(
        select(CredentialsDB).where(
            CredentialsDB.user_id == user_id, CredentialsDB.domain == "test"
        )
    ).all()


def test_insert_stores_value(user_and_field):
    sess, user, field = user_and_field
    field.insert("alice", start=dt(2020), end=dt(2023))
    assert field.get_value(dt(2021)) == "alice"


def test_insert_upper_inf_stores_indefinitely(user_and_field):
    sess, user, field = user_and_field
    field.insert("alice", start=dt(2020))
    assert field.get_value(dt(2030)) == "alice"
    assert field.get_value(dt(2100)) == "alice"


def test_non_overlapping_different_values_both_stored(user_and_field):
    sess, user, field = user_and_field
    field.insert("alice", start=dt(2020), end=dt(2022))
    field.insert("bob", start=dt(2022), end=dt(2024))
    assert field.get_value(dt(2021)) == "alice"
    assert field.get_value(dt(2023)) == "bob"
    assert len(get_records(sess, user.id)) == 2


def test_input_upper_inf_inserted_when_no_conflicts(user_and_field):
    """Input [start, None) is stored as-is when there are no conflicting ranges."""
    sess, user, field = user_and_field
    field.insert("alice", start=dt(2020))
    records = get_records(sess, user.id)
    assert len(records) == 1
    assert records[0].valid.lower == dt(2020)
    assert records[0].valid.upper_inf


def test_overlapping_same_value_merged(user_and_field):
    sess, user, field = user_and_field
    field.insert("alice", start=dt(2020), end=dt(2022))
    field.insert("alice", start=dt(2021), end=dt(2024))
    records = get_records(sess, user.id)
    assert len(records) == 1
    assert records[0].valid.lower == dt(2020)
    assert records[0].valid.upper == dt(2024)


def test_adjacent_same_value_merged(user_and_field):
    sess, user, field = user_and_field
    field.insert("alice", start=dt(2020), end=dt(2022))
    field.insert("alice", start=dt(2022), end=dt(2024))
    records = get_records(sess, user.id)
    assert len(records) == 1
    assert records[0].valid.lower == dt(2020)
    assert records[0].valid.upper == dt(2024)


def test_contained_same_value_is_noop(user_and_field):
    sess, user, field = user_and_field
    field.insert("alice", start=dt(2020), end=dt(2024))
    field.insert("alice", start=dt(2021), end=dt(2023))
    records = get_records(sess, user.id)
    assert len(records) == 1
    assert records[0].valid.lower == dt(2020)
    assert records[0].valid.upper == dt(2024)


def test_three_ranges_same_value_merged(user_and_field):
    sess, user, field = user_and_field
    field.insert("alice", start=dt(2020), end=dt(2022))
    field.insert("alice", start=dt(2023), end=dt(2025))
    field.insert("alice", start=dt(2021), end=dt(2024))
    records = get_records(sess, user.id)
    assert len(records) == 1
    assert records[0].valid.lower == dt(2020)
    assert records[0].valid.upper == dt(2025)


def test_overlapping_different_values_raises(user_and_field):
    sess, user, field = user_and_field
    field.insert("alice", start=dt(2020), end=dt(2024))
    with pytest.raises(DateOverlapError):
        field.insert("bob", start=dt(2022), end=dt(2026))


def test_fully_contained_different_value_raises(user_and_field):
    sess, user, field = user_and_field
    field.insert("alice", start=dt(2020), end=dt(2024))
    with pytest.raises(DateOverlapError):
        field.insert("bob", start=dt(2021), end=dt(2023))


def test_error_does_not_modify_db(user_and_field):
    sess, user, field = user_and_field
    field.insert("alice", start=dt(2020), end=dt(2024))
    sess.commit()
    with pytest.raises(DateOverlapError):
        field.insert("bob", start=dt(2022), end=dt(2026))
    records = get_records(sess, user.id)
    assert len(records) == 1
    assert records[0].username == "alice"


def test_input_upper_inf_clipped_by_later_conflict(user_and_field):
    """Input [start, None) is clipped to [start, conflict_start) when a different-value range starts later."""
    sess, user, field = user_and_field
    field.insert("bob", start=dt(2022), end=dt(2024))
    field.insert("alice", start=dt(2020))
    records = get_records(sess, user.id)
    assert len(records) == 2
    alice_records = [r for r in records if r.username == "alice"]
    assert len(alice_records) == 1
    assert alice_records[0].valid.lower == dt(2020)
    assert alice_records[0].valid.upper == dt(2022)


def test_db_upper_inf_clipped_by_new_range(user_and_field):
    """Existing [start, None) in DB is clipped to [start, new_start) when a different-value range is inserted after it."""
    sess, user, field = user_and_field
    field.insert("alice", start=dt(2020))
    field.insert("bob", start=dt(2022), end=dt(2024))
    records = get_records(sess, user.id)
    assert len(records) == 2
    alice_records = [r for r in records if r.username == "alice"]
    assert len(alice_records) == 1
    assert alice_records[0].valid.lower == dt(2020)
    assert alice_records[0].valid.upper == dt(2022)


def test_input_upper_inf_clipped_to_earliest_conflict(user_and_field):
    """Input [start, None) is clipped to the earliest conflicting range's start."""
    sess, user, field = user_and_field
    field.insert("bob", start=dt(2022), end=dt(2024))
    field.insert("charlie", start=dt(2024), end=dt(2026))
    field.insert("alice", start=dt(2020))
    records = get_records(sess, user.id)
    assert len(records) == 3
    alice_records = [r for r in records if r.username == "alice"]
    assert len(alice_records) == 1
    assert alice_records[0].valid.lower == dt(2020)
    assert alice_records[0].valid.upper == dt(2022)


def test_input_upper_inf_clip_would_be_empty_raises(user_and_field):
    """Input [t, None) raises when a different-value range starts at the same t, since clipping gives an empty range."""
    sess, user, field = user_and_field
    field.insert("bob", start=dt(2022), end=dt(2024))
    with pytest.raises(DateOverlapError):
        field.insert("alice", start=dt(2022))


def test_db_upper_inf_clip_would_be_empty_raises(user_and_field):
    """Existing [t, None) raises when a different-value range also starts at t, since clipping gives an empty range."""
    sess, user, field = user_and_field
    field.insert("alice", start=dt(2022))
    with pytest.raises(DateOverlapError):
        field.insert("bob", start=dt(2022), end=dt(2024))


def test_db_upper_inf_and_input_upper_inf_same_value_merged(user_and_field):
    """Two upper-infinite ranges with the same value and overlapping start dates are merged."""
    sess, user, field = user_and_field
    field.insert("alice", start=dt(2022))
    field.insert("alice", start=dt(2020))
    records = get_records(sess, user.id)
    assert len(records) == 1
    assert records[0].valid.lower == dt(2020)
    assert records[0].valid.upper_inf


def test_db_upper_inf_and_input_upper_inf_diff_value_clipped(user_and_field):
    """Two upper-infinite ranges with the same value and overlapping start dates are merged."""
    sess, user, field = user_and_field
    field.insert("alice", start=dt(2020))
    field.insert("bob", start=dt(2022))
    records = get_records(sess, user.id)
    assert len(records) == 2
    alice_records = [r for r in records if r.username == "alice"]
    assert len(alice_records) == 1
    assert alice_records[0].valid.lower == dt(2020)
    assert alice_records[0].valid.upper == dt(2022)
    bob_records = [r for r in records if r.username == "bob"]
    assert len(bob_records) == 1
    assert bob_records[0].valid.lower == dt(2022)
    assert bob_records[0].valid.upper_inf


def test_db_upper_inf_and_input_upper_inf_diff_value_clipped2(user_and_field):
    """Two upper-infinite ranges with the same value and overlapping start dates are merged."""
    sess, user, field = user_and_field
    field.insert("alice", start=dt(2022))
    field.insert("bob", start=dt(2020))
    records = get_records(sess, user.id)
    assert len(records) == 2
    alice_records = [r for r in records if r.username == "alice"]
    assert len(alice_records) == 1
    assert alice_records[0].valid.lower == dt(2022)
    assert alice_records[0].valid.upper_inf
    bob_records = [r for r in records if r.username == "bob"]
    assert len(bob_records) == 1
    assert bob_records[0].valid.lower == dt(2020)
    assert bob_records[0].valid.upper == dt(2022)
