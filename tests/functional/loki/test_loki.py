import pytest


@pytest.mark.usefixtures("client_mode")
def test_query_range():
    from sarc.client.loki import query_range

    response = query_range(
        'query=sum(rate({job="varlogs"}[10m])) by (level)', end="now", since="1h"
    )

    assert response["status"] == "success"
    assert response["data"]["resultType"] in ("matrix", "streams")
    assert len(response["data"]["result"])

    for result in response["data"]["result"]:
        assert isinstance(result["metric"], dict)
        seconds = [int(id) for id, _ in result["values"]]
        assert seconds
        assert seconds == sorted(seconds)
