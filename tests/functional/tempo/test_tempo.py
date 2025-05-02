import datetime

import pytest


@pytest.mark.usefixtures("client_mode")
def test_query():
    from sarc.client.tempo import query

    id = "2f3e0cee77ae5dc9c17ade3689eb2e54"
    start = datetime.datetime(2023, 5, 1, tzinfo=datetime.timezone.utc)
    end = datetime.datetime(2023, 6, 1, tzinfo=datetime.timezone.utc)
    response = query(
        id,
        start=start,
        end=end,
    )

    assert response
    assert response["traceID"] == id
    start_time = int(response["startTimeUnixNano"]) / 10**9
    start_time = datetime.datetime.fromtimestamp(start_time, datetime.timezone.utc)
    assert start_time >= start
    assert start_time < end
    assert start_time + datetime.timedelta(milliseconds=response["durationMs"]) < end
