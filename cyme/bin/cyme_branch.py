"""cyme.bin.cyme_branch

- This is the script run by the :program:`cyme-branch` script installed
  by the Cyme distribution (defined in setup.py's ``entry_points``).

- It in turn executes the cyme-branch management command.

"""

from __future__ import absolute_import

from .base import app


@app(needs_eventlet=True)
def cyme_branch(env, argv):
    from ..management.commands import cyme_branch
    cyme_branch.Command(env).run_from_argv([argv[0], "cyme-branch"] + argv[1:])


if __name__ == "__main__":
    cyme_branch()
