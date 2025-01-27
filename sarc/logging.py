import logging
import os

from sarc.config import config


def getOpenTelemetryLoggingHandler(log_level=logging.WARNING):

    from opentelemetry._logs import set_logger_provider
    from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
    from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
    from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
    from opentelemetry.sdk.resources import Resource

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


def setupLogging():

    logging_levels = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }

    if config().logging:
        log_level = logging_levels.get(config().logging.log_level, logging.WARNING)

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
            level=logging.WARNING,  # Default log level
        )
