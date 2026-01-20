import os
import sys

sys.path.insert(0, os.path.abspath('..'))  # Убедись, что путь добавляется

print("PYTHONPATH:", sys.path)  # Для отладки, потом можно убрать

# -- Project information -----------------------------------------------------
project = 'TenderMonitor'
copyright = '2025, Dimitriev_S_A'
author = 'Dimitriev_S_A'
release = 'version 1.0 Beta'

# -- General configuration ---------------------------------------------------
extensions = ['breathe', 'sphinx.ext.autodoc']

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

language = 'ru'

# -- Options for HTML output -------------------------------------------------
html_theme = 'alabaster'
html_static_path = ['_static']
