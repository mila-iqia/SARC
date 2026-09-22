.. SARC API documentation file

REST API
========

REST API server
---------------

There is currently a minimal REST API available in module ``sarc/api``.
To launch REST server::

    SARC_CONFIG=<sarc-config> uv run fastapi run sarc/api/main.py --port <port>

Server will be available at ``http://0.0.0.0:<port>``.

Server documentation, listing all endpoints with parameters and expected output, is available at ``http://0.0.0.0:<port>/docs``.

Host can be configured using parameter ``--host``.

To launch server in develop mode (with reload enabled), use ``fastapi dev`` instead of ``fastapi run``.

More documentation:

- FastAPI: https://fastapi.tiangolo.com/
- Command line:

  - ``uv run fastapi run -h``
  - ``uv run fastapi dev -h``

Python client
-------------

``sarc.rest.SarcClient`` calls the endpoints listed below. It needs no
configuration file — ``base_url`` already points at production, so a token is
enough::

    import os
    from datetime import UTC, datetime

    from sarc.rest import SarcClient

    client = SarcClient(token=os.environ["SARC_TOKEN"])

    for job in client.get_job_series(
        cluster_name="mila",
        start=datetime(2026, 9, 1, tzinfo=UTC),
        end=datetime(2026, 9, 15, tzinfo=UTC),
    ):
        print(job.job_id, job.usage_metric)

``get_jobs``, ``get_job_series`` and ``get_users`` are generators: they follow
the cursor pagination themselves, ``block_size`` results per request. Datetimes
must be time-aware — a naive one comes back as a 422.

A token is issued by https://sarc.mila.quebec/token, in the ``refresh_token``
field of its answer. That route, like ``/login`` and ``/auth``, comes from the
``easy_oauth`` manager configured under the ``server.auth`` config key and
mounted by ``sarc/api/app.py``.

``SarcClient.load()`` is the other door, for a client configured from a
``sarc.client`` block in the configuration file rather than in code.

.. note::

   This API has an external consumer: the ``mila-sarc`` skill in
   https://github.com/mila-iqia/skills documents it for coding agents, in raw
   HTTP. Nothing links the two repositories, so a change of contract breaks it
   silently — when one changes, check that skill's ``SKILL.md`` and
   ``quiz.yaml``.

API Reference
-------------

.. openapi:: openapi.json
   :examples:
