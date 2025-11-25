import logging
import os

from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.resources import Resource

from rapporteur.report import Report
from rapporteur.slack import SlackReporter

from sarc.config import LoggingConfig, SlackConfig, config

logger = logging.getLogger(__name__)

rapporteur_report: Report | None = None


def getOpenTelemetryLoggingHandler(log_conf: LoggingConfig):
    if log_conf.OTLP_endpoint is None or log_conf.service_name is None:
        return None
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
    # Use logging.NOTSET to let the logger level control filtering, not the handler
    return LoggingHandler(level=logging.NOTSET, logger_provider=logger_provider)


def setupSlackReport(slack_config: SlackConfig, command_name: str | None = None):
    global rapporteur_report  # noqa: PLW0603
    slack_reporter = SlackReporter(
        token=slack_config.token, channel=slack_config.channel
    )
    desc = slack_config.description
    if command_name is not None:
        desc += f" ({command_name})"
    rapporteur_report = Report(
        description=desc, reporters=[slack_reporter]
    )


def getSlackReport() -> Report | None:
    return rapporteur_report


def setupLogging(verbose_level: int = 0, command_name: str | None = None):
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
        if conf.logging.slack:
            setupSlackReport(conf.logging.slack, command_name)

        config_log_level = logging_levels.get(conf.logging.log_level, logging.WARNING)
        # verbose priority:
        # in 0 (not specified in command line) then config log level is used
        # otherwise, command-line verbose level is used
        log_level = verbose_levels.get(verbose_level, config_log_level)

        # Create the OpenTelemetry handler with NOTSET level
        ot_handler = getOpenTelemetryLoggingHandler(conf.logging)

        # Create a single formatter that will be used by both handlers
        formatter = logging.Formatter(
            "%(asctime)-15s::%(levelname)s::%(name)s::%(message)s"
        )

        # Configure console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        console_handler.setLevel(logging.NOTSET)  # Let logger level control filtering

        # Configure OpenTelemetry handler
        if ot_handler is not None:
            ot_handler.setFormatter(formatter)  # Apply the same formatter
            ot_handler.setLevel(logging.NOTSET)  # Let logger level control filtering

        # Clear any existing handlers and configure logging
        root_logger = logging.getLogger()
        root_logger.handlers.clear()  # Remove any existing handlers

        # Add our handlers
        if ot_handler is not None:
            root_logger.addHandler(ot_handler)
        root_logger.addHandler(console_handler)

        # Set the logger level (this controls what messages get processed)
        root_logger.setLevel(log_level)

        logger.debug("setupLogging done")

    else:
        # no logging level in config file
        logging.basicConfig(
            handlers=[logging.StreamHandler()],
            format="%(asctime)-15s::%(levelname)s::%(name)s::%(message)s",
            level=verbose_levels.get(
                verbose_level, logging.DEBUG
            ),  # Default log level, if not specidied in config
        )
