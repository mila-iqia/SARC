# Loki

SARC uses opentelemetry to send logging messages to the loki server; this is done in [logging.py](../../sarc/logging.py)


## config file

The following section of the [config file](config_file.md) handles the loki/opentelemetry settings:
```json
    "logging": {
        "log_level": "WARNING",
        "OTLP_endpoint": "http://loki01.server.mila.quebec:3100/otlp/v1/logs",
        "service_name": "sarc-dev"
    },
```

## restrictions

### send
To be able to send logs to the loki server, SARC must be run by a machine in the Mila network. That means that, SARC running on `sarc01-dev` can send logs to loki, whereas it won't be able to from your workstation if you work remotely.

### read dashboard

The dashboard is only accessible by authenticated members. (google account)

[link to dashboard](https://dashboard.server.mila.quebec/goto/zAK2ZeTHg?orgId=1)
 