from django.core import management

from scs.apps.base import app


@app
def scssh(argv):
    management.call_command("shell")


if __name__ == "__main__":
    scssh()
