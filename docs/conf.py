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

# The ER diagram in dev_overview/db_tables.md asks for the elk layout engine;
# without this mermaid falls back to dagre without a word.
mermaid_include_elk = True

# The default fullscreen glyph is U+26F6, which no font here covers, so the
# button shows a tofu box. U+2922 is the same idea and is covered.
mermaid_fullscreen_button = "⤢"

# The default pins every diagram's svg to 500px tall, which letterboxes the
# small ones and shrinks the big ones; auto lets each keep its aspect ratio
# at full width.
mermaid_height = "auto"

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]
