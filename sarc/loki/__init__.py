import requests
import requests_mock

from sarc.config import config

_URI = config().loki.uri


def query_range(
    query: str, end: str = "now", since: str = "1h", direction: str = "backward"
):
    """Query Loki over a range of time.

    https://grafana.com/docs/loki/latest/reference/api/#query-loki-over-a-range-of-time

    Arguments:
        query: The LogQL query to perform.
        end: The end time for the query as a nanosecond Unix epoch or another
             supported format. Loki returns results with timestamp lower than this
             value.
        since: A duration used to calculate start relative to end. If end is in
               the future, start is calculated as this duration before now.
        direction: Determines the sort order of logs. Supported values are
                   forward or backward.
    """
    url = f"{_URI}/api/v1/query_range"
    limit = 100

    with requests_mock.Mocker() as m:
        m.get(
            url,
            json={
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {"level": "info"},
                            "values": [
                                [1588889221, "137.95"],
                                [1588889221, "467.115"],
                                [1588889221, "658.8516666666667"],
                            ],
                        },
                        {
                            "metric": {"level": "warn"},
                            "values": [
                                [1588889221, "137.27833333333334"],
                                [1588889221, "467.69"],
                                [1588889221, "660.6933333333334"],
                            ],
                        },
                    ],
                    "stats": {},
                },
            },
        )
        r = requests.get(
            url,
            params={
                "query": query,
                "limit": limit,
                "end": end,
                "since": since,
                "direction": direction,
            },
            timeout=60,
        )

    return r.json()
