"""scs.apps.scs_agent"""

from __future__ import absolute_import

from .base import app


@app()
def scs_agent(argv):
    from ..management.commands import scs_agent
    scs_agent.Command().run_from_argv([argv[0], "scs-agent"] + argv[1:])


if __name__ == "__main__":
    scs_agent()
