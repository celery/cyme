"""cyme.bin.cyme_agent

- This is the script run by the :program:`cyme-agent` script installed
  by the Cyme distribution (defined in setup.py's ``entry_points``).

- It in turn executes the cyme-agent management command.

"""

from __future__ import absolute_import

from .base import app


@app(needs_eventlet=True)
def cyme_agent(env, argv):
    from ..management.commands import cyme_agent
    cyme_agent.Command(env).run_from_argv([argv[0], "cyme-agent"] + argv[1:])


if __name__ == "__main__":
    cyme_agent()
