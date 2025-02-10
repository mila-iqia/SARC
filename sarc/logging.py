import logging
import os

from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.resources import Resource

from sarc.config import config


def getOpenTelemetryLoggingHandler(log_level=logging.WARNING):

    logger_provider = LoggerProvider(
        resource=Resource.create(
            {
                "service.name": config().logging.service_name,
                "service.instance.id": os.uname().nodename,
            }
        ),
    )
    set_logger_provider(logger_provider)

    otlp_exporter = OTLPLogExporter(config().logging.OTLP_endpoint)
    logger_provider.add_log_record_processor(BatchLogRecordProcessor(otlp_exporter))
    return LoggingHandler(level=log_level, logger_provider=logger_provider)


def setupLogging(verbose_level: int = 0):
    verbose_levels = {0: logging.WARNING, 1: logging.INFO, 2: logging.DEBUG}

    logging_levels = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }

    if config().logging:
        # take the lowest log level between the config and the verbose level
        config_log_level = logging_levels.get(
            config().logging.log_level, logging.WARNING
        )
        verbose_log_level = verbose_levels.get(verbose_level, config_log_level)
        log_level = min(config_log_level, verbose_log_level)

        handler = getOpenTelemetryLoggingHandler(log_level)

        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)

        logging.basicConfig(
            handlers=[handler, console_handler],
            format="%(asctime)-15s::%(levelname)s::%(name)s::%(message)s",
            level=log_level,
        )

        logging.error("ERROR test log")

    else:
        logging.basicConfig(
            handlers=[logging.StreamHandler()],
            format="%(asctime)-15s::%(levelname)s::%(name)s::%(message)s",
            level=verbose_levels.get(verbose_level, logging.DEBUG),  # Default log level
        )
