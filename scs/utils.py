"""scs.utils"""

from __future__ import absolute_import

from importlib import import_module

from cl.utils.functional import promise, maybe_promise # noqa
from kombu.utils import gen_unique_id as uuid          # noqa
from kombu.utils import cached_property                # noqa


def shellquote(v):
    # XXX Not currently used, but may be of use later,
    # and don't want to write it again.
    return "\\'".join("'" + p + "'" for p in v.split("'"))


def imerge_settings(a, b):
    """Merge two django settings modules,
    keys in ``b`` have precedence."""
    orig = import_module(a.SETTINGS_MODULE)
    for key, value in vars(b).iteritems():
        if not hasattr(orig, key):
            setattr(a, key, value)
