"""scs.bin.scs_agent"""

from __future__ import absolute_import

from .base import app


@app()
def scs(env, argv):
    from ..management.commands import scs
    scs.Command(env=env).run_from_argv([argv[0], "scs"] + argv[1:])


if __name__ == "__main__":
    scs()
