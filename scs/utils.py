"""scs.utils"""

from __future__ import absolute_import

from importlib import import_module


def shellquote(v):
    return "\\'".join("'" + p + "'" for p in v.split("'"))


def imerge_settings(a, b):
    orig = import_module(a.SETTINGS_MODULE)
    for key, value in vars(b).iteritems():
        if not hasattr(orig, key):
            setattr(a, key, value)
