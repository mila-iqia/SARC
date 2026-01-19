.. SARC API documentation file

REST API
========

REST API server
---------------

There is currently a minimal REST API available in module ``sarc/api``.
To launch REST server::

    SARC_MODE=<sarc-mode> SARC_CONFIG=<sarc-config> uv run fastapi run sarc/api/main.py --port <port>

Server will be available at ``http://0.0.0.0:<port>``.

Sever documentation, listing all endpoints with parameters and expected output, is available at ``http://0.0.0.0:<port>/docs``.

Host can be configured using parameter ``--host``.

To launch server in develop mode (with reload enabled), use ``fastapi dev`` instead of ``fastapi run``.

More documentation:

- FasAPI: https://fastapi.tiangolo.com/
- Command line:

  - ``uv run fastapi run -h``
  - ``uv run fastapi dev -h``

API Reference
-------------

.. openapi:: openapi.json
   :examples:
