"""scs.utils"""

from __future__ import absolute_import

from importlib import import_module

from cl.utils.functional import promise, maybe_promise # noqa
from kombu.utils import gen_unique_id as uuid          # noqa
from kombu.utils import cached_property                # noqa
from unipath import Path as _Path


class Path(_Path):
    """Path that can use the ``/`` operator to combine paths.

        >>> p = Path("foo")
        >>> p / "bar" / "baz"
        Path("foo/bar/baz")
    """

    def __div__(self, other):
        return Path(self, other)


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


def setup_logging(loglevel="INFO", logfile=None):
    from celery import current_app as celery
    from celery.utils import LOG_LEVELS
    if isinstance(loglevel, basestring):
        loglevel = LOG_LEVELS[loglevel]
    return celery.log.setup_logging_subsystem(loglevel, logfile)
