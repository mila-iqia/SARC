import requests
import requests_mock

from sarc.config import config

_URI = config().tempo.uri


def query(traceid: str, start: str = None, end: str = None):
    """Query Tempo to retrieve a trace from the query frontend service.

    https://grafana.com/docs/tempo/latest/api_docs/#query

    Arguments:
        traceid: The trace id to retreive.
        start: Along with end define a time range from which traces should be
               returned.
        end: Along with start define a time range from which traces should be
             returned. Providing both start and end will include traces for the
             specified time range only. If the parameters are not provided then
             Tempo will check for the trace across all blocks in backend. If the
             parameters are provided, it will only check in the blocks within
             the specified time range, this can result in trace not being found
             or partial results if it does not fall in the specified time
             range.
    """
    url = f"{_URI}/api/traces/{traceid}"
    params = {}
    if start is not None:
        params["start"] = start
    if end is not None:
        params["end"] = end

    with requests_mock.Mocker() as m:
        m.get(
            url,
            json={
                "traceID": "2f3e0cee77ae5dc9c17ade3689eb2e54",
                "rootServiceName": "shop-backend",
                "rootTraceName": "update-billing",
                "startTimeUnixNano": "1684778327699392724",
                "durationMs": 557,
                "spanSets": [
                    {
                        "spans": [
                            {
                                "spanID": "563d623c76514f8e",
                                "startTimeUnixNano": "1684778327735077898",
                                "durationNanos": "446979497",
                                "attributes": [
                                    {
                                        "key": "status",
                                        "value": {"stringValue": "error"},
                                    }
                                ],
                            }
                        ],
                        "matched": 1,
                    }
                ],
            },
        )
        r = requests.get(url, params=params, timeout=60)

    return r.json()
