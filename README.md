# SARC

SARC stands for "Supervision et Analyde des Resources de Calcul". It's a Mila in-house tool to monitor the usage of computational resources by Mila users. It is not meant to be deployed on the users workstations.

## Installation and setup

To install sarc, git clone the repo and install `uv`.

```bash
$ git clone git@github.com:mila-iqia/SARC.git
$ cd SARC
```

If you want to run the API server locally for trying out things, run:

```bash
$ SARC_CONFIG=<config file> uv run uvicorn sarc.api.main:app
```

It will print the URL to visit in your browser.

## Contributing

### Before commits

Those commands are for the proper formatting.

```
uv run ruff check --select I --fix
uv run ruff format
uv run tox -e ruff
uv run tox -e ty
```

### How to add dependencies

For dependecies that are core to the package:

```
uv add <package-name>
```

For dependencies that are only useful for developping:

```
uv add --dev <package-name>
```

### How to run the tests suite

This runs the tests.

```
uv run tox -e test
```

You need to have an instance of postgresql 18 running on localhost:5432 with authentification set so that the local user can connect without a password for the tests to work.

### How to generate doc

To generate documentation in HTML format in folder `docs\_build` install `pandoc` on your machine (`apt install pandoc` for debian-like linux), then:

```
uv run sphinx-build -b html docs/ docs/_build
```

You can then open `docs\_build\index.html` on a web browser.
