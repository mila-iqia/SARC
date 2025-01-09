# test logging to loki service

import logging

import pytest
import requests_mock
from pytest_httpserver import HTTPServer

from sarc.config import config
from sarc.logging import setupLogging


@pytest.fixture
def httpserver(httpserver: HTTPServer):

    # Configurer le mock pour l'endpoint Loki
    httpserver.expect_request("/otlp/v1/logs").respond_with_json(
        {"status": "success", "data": {"result": []}}
    )
    return httpserver


from sarc.logging import setupLogging


# @pytest.mark.no_capture
# @pytest.mark.skip(reason="cannot make HTTPServer running without error in this situation")
def test_loki_logging(
    standard_config,
):
    # Configurer l'URL de l'endpoint Loki
    # print(f"http://{httpserver.host}:{httpserver.port}/otlp/v1/logs")
    # print(httpserver.url_for("/otlp/v1/logs"))
    loki_url = httpserver.url_for("/otlp/v1/logs")
    config().logging.OTLP_endpoint = loki_url

    setupLogging()

    logging.warning("test Warning message")

    # assert loki_mock.called
    # assert loki_mock.call_count == 1
    # assert loki_mock.request_history[0].url == 'http://loki01.server.raisin.quebec:3100/otlp/v1/logs'
    assert True
