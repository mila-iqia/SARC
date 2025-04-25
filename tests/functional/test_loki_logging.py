# test logging to loki service

import logging

import pytest
from pytest_httpserver import HTTPServer

from sarc.config import config
from sarc.logging import getOpenTelemetryLoggingHandler


@pytest.fixture
def httpserver(httpserver: HTTPServer):
    # Configurer le mock pour l'endpoint Loki
    httpserver.expect_request("/otlp/v1/logs", method="POST").respond_with_json(
        {"status": "success", "data": {"result": []}}
    )
    return httpserver


# I gave up on testing the whole chain (sending a logger message all the way to loki),
# because pytest intercepts logging, and I tried everything to disable that without success.
# Instead, a more restricted test of the opentelemetry logging handler
# to loki is set up, with an httpserver to mock the endpoint.
# The only thing that is not tested in the message pipeline is the logging library itself,
# which can be considered reliable ?
def test_loki_logging_handler(standard_config, httpserver):
    # Configurer l'URL de l'endpoint Loki
    # print(f"http://{httpserver.host}:{httpserver.port}/otlp/v1/logs")
    loki_url = httpserver.url_for("/otlp/v1/logs")
    config().logging.OTLP_endpoint = loki_url

    ot_handler = getOpenTelemetryLoggingHandler()

    ot_handler.flush()
    assert len(httpserver.log) == 0

    ot_handler.emit(
        logging.LogRecord(
            "test_ERROR", logging.ERROR, "test_ERROR", 0, "test ERROR log", [], None
        )
    )
    ot_handler.flush()
    assert len(httpserver.log) == 1

    ot_handler.emit(
        logging.LogRecord(
            "test_WARNING",
            logging.WARNING,
            "test_WARNING",
            0,
            "test WARNING log",
            [],
            None,
        )
    )
    ot_handler.flush()
    assert len(httpserver.log) == 2
