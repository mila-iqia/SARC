import logging
import os

import opentelemetry

from sarc.config import config

logging_handler = None


def getSarcLogger(name):
    logger = logging.getLogger(name)

    if config().loki:
        from opentelemetry._logs import set_logger_provider
        from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
        from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
        from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
        from opentelemetry.sdk.resources import Resource

        global logging_handler

        if not logging_handler:
            logger_provider = LoggerProvider(
                resource=Resource.create(
                    {
                        "service.name": config().loki.service_name,
                        "service.instance.id": os.uname().nodename,
                    }
                ),
            )
            set_logger_provider(logger_provider)

            endpoint = config().loki.endpoint

            otlp_exporter = OTLPLogExporter(endpoint)
            logger_provider.add_log_record_processor(
                BatchLogRecordProcessor(otlp_exporter)
            )
            logging_handler = LoggingHandler(
                level=logging.NOTSET, logger_provider=logger_provider
            )

        logger.addHandler(logging_handler)

        debug_levels = {
            "DEBUG": logging.DEBUG,
            "INFO": logging.INFO,
            "WARNING": logging.WARNING,
            "ERROR": logging.ERROR,
            "CRITICAL": logging.CRITICAL,
        }
        if config().logging.log_level in debug_levels:
            logger.setLevel(debug_levels[config().logging.log_level])
        else:
            logger.setLevel(logging.WARNING)

    return logger
