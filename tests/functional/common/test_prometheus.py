from prometheus_api_client import PrometheusConnect


# test connection to test prometheus instance
def test_prometheus_connection():
    c = PrometheusConnect(url="http://localhost:9090/")
    assert c.check_prometheus_connection()
