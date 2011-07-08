import sys
import threading

from django.core.management.commands import runserver

from djcelery.management.base import CeleryCommand



class WebserverThread(threading.Thread):

    def __init__(self, addrport="", *args, **options):
        threading.Thread.__init__(self)
        self.addrport = addrport
        self.args = args
        self.options = options

    def run(self):
        options = dict(self.options, use_reloader=False)
        command = runserver.Command()
        command.stdout, command.stderr = sys.stdout, sys.stderr
        command.handle(self.addrport, *self.args, **self.options)


def other():
    from time import sleep
    while 1:
        print("HELLO")
        sleep(1)


class Command(CeleryCommand):
    args = '[optional port number, or ipaddr:port]'
    option_list = runserver.Command.option_list
    help = 'Starts Django Admin instance and celerycam in the same process.'
    # see http://code.djangoproject.com/changeset/13319.
    stdout, stderr = sys.stdout, sys.stderr

    def handle(self, addrport="", *args, **options):
        """Handle the management command."""
        server = WebserverThread(addrport, *args, **options)
        server.start()
        options["camera"] = "djcelery.snapshot.Camera"
        options["prog_name"] = "djcelerymon"
        other()

