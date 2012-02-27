# -*- coding: utf-8 -*-

import sys
import os

this = os.path.dirname(os.path.abspath(__file__))

# If your extensions are in another directory, add it here. If the directory
# is relative to the documentation root, use os.path.abspath to make it
# absolute, like shown here.
sys.path.append(os.path.join(os.pardir, "tests"))
sys.path.append(os.path.join(this, "_ext"))
settings = os.environ.setdefault("DJANGO_SETTINGS_MODULE", "settings")
import django
if django.VERSION < (1, 4):
    from django.core.management import setup_environ
    setup_environ(__import__(settings))
import cyme


# use app loader
from celery import Celery
app = Celery(set_as_current=True)
app.conf.update(BROKER_TRANSPORT="memory",
                CELERY_RESULT_BACKEND="cache",
                CELERY_CACHE_BACKEND="memory",
                CELERYD_HIJACK_ROOT_LOGGER=False,
                CELERYD_LOG_COLOR=False)

# General configuration
# ---------------------

extensions = ['sphinx.ext.autodoc',
              'sphinx.ext.coverage',
              'sphinx.ext.pngmath',
              'sphinx.ext.intersphinx',
              'sphinxcontrib.issuetracker',
              'celerydocs']

# Add any paths that contain templates here, relative to this directory.
templates_path = ['.templates']

# The suffix of source filenames.
source_suffix = '.rst'

# The master toctree document.
master_doc = 'index'

# General information about the project.
project = u'Cyme'
copyright = u'2011, VMware, Inc.'

# The version info for the project you're documenting, acts as replacement for
# |version| and |release|, also used in various other places throughout the
# built documents.
#
# The short X.Y version.
version = ".".join(map(str, cyme.VERSION[0:2]))
# The full version, including alpha/beta/rc tags.
release = cyme.__version__

exclude_trees = ['.build']

# If true, '()' will be appended to :func: etc. cross-reference text.
add_function_parentheses = True

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = 'trac'

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['.static']

html_use_smartypants = True

# If false, no module index is generated.
html_use_modindex = True

# If false, no index is generated.
html_use_index = True

latex_documents = [
  ('index', 'cyme.tex', ur'Cyme Documentation',
   ur'VMWare, Inc.', 'manual'),
]

html_show_sphinx = False
intersphinx_mapping = {
        "http://docs.python.org/dev": None,
        "http://kombu.readthedocs.org/en/latest/": None,
        "http://django-celery.readthedocs.org/en/latest": None,
}

html_theme = "celery"
html_theme_path = ["_theme"]
html_sidebars = {
    'index': ['sidebarintro.html', 'sourcelink.html', 'searchbox.html'],
    '**': ['sidebarlogo.html', 'relations.html',
           'sourcelink.html', 'searchbox.html'],
}

### Issuetracker

#issuetracker = "github"
#issuetracker_user = "ask"
#issuetracker_project = "celery"
#issuetracker_issue_pattern = r'[Ii]ssue #(\d+)'
