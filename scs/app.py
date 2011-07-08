try:
    import eventlet
except ImportError:
    import warnings
    warnings.warn("Eventlet not installed, continuing without it.")
else:
    eventlet.monkey_patch()


import os
import sys
import getpass


from scs import settings as default_settings
from scs.utils import imerge_settings


from django.conf import settings
from django.core.management import setup_environ

def configure():
    if not settings.configured:
        setup_environ(default_settings)
    else:
        imerge_settings(settings, default_settings)



def run_scs(argv):
    from scs.management.commands import scs
    scs.Command().run_from_argv([argv[0], "scs"] + argv[1:])


def main(argv=sys.argv):
    from django.core import management
    configure()
    gp, getpass.getpass = getpass.getpass, getpass.fallback_getpass
    try:
        management.call_command("syncdb")
    finally:
        getpass.getpass = gp
    run_scs(argv)


if __name__ == "__main__":
    main()

