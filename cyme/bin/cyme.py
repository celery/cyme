"""cyme.bin.cyme"""

from __future__ import absolute_import

from .base import app


@app()
def cyme(env, argv):
    from ..management.commands import cyme
    cyme.Command(env=env).run_from_argv([argv[0], "cyme"] + argv[1:])


if __name__ == "__main__":
    cyme()
