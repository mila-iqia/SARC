# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "SARC"
copyright = "2023, Mila"
author = "Mila"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = ["myst_parser", "sphinxcontrib.openapi", "sphinxcontrib.mermaid"]

# Without this, myst passes ```mermaid fences to Pygments as a language
# and the diagrams render as plain code blocks.
myst_fence_as_directive = ["mermaid"]

# The pages use GitHub's alert syntax (`> [!NOTE]`), which myst renders as a
# plain quote with the marker left in the text unless this is enabled.
myst_enable_extensions = ["alert"]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]
