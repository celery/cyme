from scs.apps.base import app


@app
def scs_agent(argv):
    from scs.management.commands import scs_agent
    scs_agent.Command().run_from_argv([argv[0], "scs-agent"] + argv[1:])


if __name__ == "__main__":
    scs_agent()
