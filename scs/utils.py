"""scs.utils"""

from __future__ import absolute_import

import time

from email.utils import formatdate
from importlib import import_module


def rfc2822(dt):
    return formatdate(time.mktime(dt.timetuple()))


def maybe_list(l):
    if hasattr(l, "__iter__"):
        return l
    elif l is not None:
        return [l]


def shellquote(v):
    return "\\'".join("'" + p + "'" for p in v.split("'"))


def imerge_settings(a, b):
    orig = import_module(a.SETTINGS_MODULE)
    for key, value in vars(b).iteritems():
        if not hasattr(orig, key):
            setattr(a, key, value)
