import logging
import os

from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.resources import Resource

from sarc.config import LoggingConfig, config


def getOpenTelemetryLoggingHandler(
    log_conf: LoggingConfig, log_level: int = logging.WARNING
):
    logger_provider = LoggerProvider(
        resource=Resource.create(
            {
                "service.name": log_conf.service_name,
                "service.instance.id": os.uname().nodename,
            }
        ),
    )
    set_logger_provider(logger_provider)

    otlp_exporter = OTLPLogExporter(log_conf.OTLP_endpoint)
    logger_provider.add_log_record_processor(BatchLogRecordProcessor(otlp_exporter))
    return LoggingHandler(level=log_level, logger_provider=logger_provider)


def setupLogging(verbose_level: int = 0):
    verbose_levels = {1: logging.INFO, 2: logging.DEBUG}

    logging_levels = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }

    conf = config()
    # Apparently this can be called in client mode which doesn't have logging
    if hasattr(conf, "logging") and conf.logging:
        config_log_level = logging_levels.get(conf.logging.log_level, logging.WARNING)
        # verbose priority:
        # in 0 (not specified in command line) then config log level is used
        # otherwise, command-line verbose level is used
        log_level = verbose_levels.get(verbose_level, config_log_level)

        handler = getOpenTelemetryLoggingHandler(conf.logging, log_level)

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

    else:
        # no logging level in config file
        logging.basicConfig(
            handlers=[logging.StreamHandler()],
            format="%(asctime)-15s::%(levelname)s::%(name)s::%(message)s",
            level=verbose_levels.get(
                verbose_level, logging.DEBUG
            ),  # Default log level, if not specidied in config
        )
