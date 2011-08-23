"""scs.apps.scs_agent

- This is the script run by the :program:`scs-agent` script installed
  by the SCS distribution (defined in setup.py's ``entry_points``).

- It in turn executes the scs-agent management command.

"""

from __future__ import absolute_import

from .base import app


@app()
def scs_agent(env, argv):
    from ..management.commands import scs_agent
    scs_agent.Command(env).run_from_argv([argv[0], "scs-agent"] + argv[1:])


if __name__ == "__main__":
    scs_agent()
