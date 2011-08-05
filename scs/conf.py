from django.conf import settings

SCS_INSTANCE_DIR = getattr(settings, "SCS_INSTSANCE_DIR", "/var/run/scs")
