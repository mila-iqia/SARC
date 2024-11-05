import logging
import os

from sarc.config import config

# import opentelemetry


# logging_handler = None

# def getHandler():
#     global logging_handler

#     if config().loki:
#         from opentelemetry._logs import set_logger_provider
#         from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
#         from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
#         from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
#         from opentelemetry.sdk.resources import Resource

#         if not logging_handler:
#             print(f"Loki logging enabled for {config().loki.endpoint}, service name: {config().loki.service_name}")

#             logger_provider = LoggerProvider(
#                 resource=Resource.create(
#                     {
#                         "service.name": config().loki.service_name,
#                         "service.instance.id": os.uname().nodename,
#                     }
#                 ),
#             )
#             set_logger_provider(logger_provider)

#             endpoint = config().loki.endpoint

#             otlp_exporter = OTLPLogExporter(endpoint)
#             logger_provider.add_log_record_processor(
#                 BatchLogRecordProcessor(otlp_exporter)
#             )
#             logging_handler = LoggingHandler(
#                 level=logging.NOTSET, logger_provider=logger_provider
#             )
#             print(f"logging_handler: {logging_handler}")
#         print(f"logging_handler: {logging_handler}")
#         return logging_handler

#     return logging.defaultHandler()

# def getLogger(name):
#     logger = logging.getLogger(name)

#     handler = getHandler()

#     if handler:

#         logger.addHandler(handler)

#         debug_levels = {
#             "DEBUG": logging.DEBUG,
#             "INFO": logging.INFO,
#             "WARNING": logging.WARNING,
#             "ERROR": logging.ERROR,
#             "CRITICAL": logging.CRITICAL,
#         }
#         if config().logging and config().logging.log_level in debug_levels:
#             logger.setLevel(debug_levels[config().logging.log_level])
#         else:
#             logger.setLevel(logging.WARNING)

#         logging.info(f"added Loki logging handler to {name}")

#     return logger


def setupLogging():
    from opentelemetry._logs import set_logger_provider
    from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
    from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
    from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
    from opentelemetry.sdk.resources import Resource

    logging_levels = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }
    log_level = logging.WARNING  # default log level
    if config().logging:
        log_level = logging_levels.get(config().logging.log_level, logging.WARNING)

    if config().loki:
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
        logger_provider.add_log_record_processor(BatchLogRecordProcessor(otlp_exporter))
        handler = LoggingHandler(level=logging.NOTSET, logger_provider=logger_provider)

        logging.basicConfig(
            handlers=[handler, logging.StreamHandler()],
            format="%(asctime)-15s::%(levelname)s::%(name)s::%(message)s",
            level=log_level,
        )
    else:
        logging.basicConfig(
            handlers=[logging.StreamHandler()],
            format="%(asctime)-15s::%(levelname)s::%(name)s::%(message)s",
            level=logging.INFO,
        )

    # logging.info("SARC Test info log")
    # logging.debug("SARC Test debug log")
    # logging.warning("SARC Test warning log")
    # logging.error("SARC Test error log")
