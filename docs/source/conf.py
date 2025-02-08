import os\nimport sys\n\n# Add paths to the system path\nsys.path.insert(0, os.path.abspath("../.."))\nsys.path.insert(0, os.path.abspath("pygments"))\n\n# Project information\nproject = "Gloe"\ncopyright = "2023, Samir Braga"\nauthor = "Samir Braga"\nrelease = "0.4.3"\n\n# General configuration\nextensions = [\n    "sphinx_toolbox.more_autodoc.variables",\n    "sphinx.ext.autosectionlabel",\n    "sphinx.ext.autodoc",\n    "sphinx.ext.autosummary",\n    "sphinx.ext.viewcode",\n    "sphinx.ext.napoleon",\n    "sphinx.ext.intersphinx",\n    "sphinxext.opengraph",\n    # "sphinx_autodoc_typehints",  # Uncomment if type hints are needed\n    "myst_parser",\n    "sphinx_copybutton",\n]\noverloads_location = "bottom"\nnapoleon_google_docstring = True\nautosectionlabel_prefix_document = True\nnapoleon_use_rtype = False\nintersphinx_mapping = {"httpx": ("https://www.python-httpx.org/", None)}\nogp_site_url = "https://gloe.ideos.com.br/"\nogp_image = "https://gloe.ideos.com.br/_static/assets/gloe-logo.png"\ntemplates_path = ["_templates"]\nexclude_patterns = ["Thumbs.db", ".DS_Store"]\nautodoc_typehints = "description"\nautodoc_type_aliases = {\n    "PreviousTransformer": "gloe.base_transformer.PreviousTransformer"\n}\n\n# Options for HTML output\nhtml_title = "Gloe"\n# html_logo = "assets/gloe-logo-small.png"  # Uncomment if a logo is needed\nhtml_theme = "furo"\nhtml_last_updated_fmt = ""\n# html_use_index = False  # Don't create index\n# html_domain_indices = False  # Don't need module indices\n# html_copy_source = False  # Don't need sources\nhtml_sidebars: dict[str, list[str]] = {\n    "Home": ["/"],\n}\n# autodoc_default_options = {"ignore-module-all": True}  # Uncomment if needed\n\nhtml_static_path = ["_static"]\nhtml_css_files = ["theme_customs.css"]\nhtml_favicon = "_static/assets/favicon.ico"\nhtml_theme_options = {\n    "light_logo": "assets/gloe-logo-small.png",\n    "dark_logo": "assets/gloe-logo-small.png",\n    "dark_css_variables": {\n        "color-brand-primary": "#00e6bf",\n        "color-brand-content": "#00e6bf",\n        "font-stack": "Roboto, sans-serif",\n        "font-stack--monospace": "Courier, monospace",\n        "font-size--normal": "Courier, monospace",\n    },\n    "footer_icons": [\n        {\n            "name": "GitHub",\n            "url": "https://github.com/ideos/gloe",\n            "html": """<svg stroke=\"currentColor\" fill=\"currentColor\" stroke-width=\"0\" viewBox=\"0 0 16 16\"><path fill-rule=\"evenodd\" d=\"M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0 0 16 8c0-4.42-3.58-8-8-8z\"></path></svg>""",\n            "class": "",\n        },\n    ],\n}\n\n# Pygments styles\npygments_dark_style = "styles.GloeDarkStyle"\npygments_light_style = "styles.GloeLightStyle"\n