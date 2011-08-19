DEBUG = True

# Broker settings.
BROKER_HOST = "127.0.0.1"
BROKER_POOL_LIMIT = 100

import djcelery
djcelery.setup_loader()

CELERYD_LOG_FORMAT = """\
[%(asctime)s: %(levelname)s] %(message)s"\
""".strip()

# Databases
DATABASES = {'default': {
                'ENGINE': 'django.db.backends.sqlite3',
                'NAME': 'scs.sqlite',
            }}

# URL and file paths.
SITE_ID = 1
STATIC_URL = "/static"
ADMIN_MEDIA_PREFIX = "/adminstatic/"
TEMPLATE_LOADERS = (
    ('django.template.loaders.cached.Loader', (
        'django.template.loaders.filesystem.Loader',
        'django.template.loaders.app_directories.Loader',
    )),
)
ROOT_URLCONF = "scs.urls"

# Time and localization.
TIME_ZONE = "UTC"
LANGUAGE_CODE = "en-us"
USE_I18N = USE_L10N = True

# Apps and middleware.
INSTALLED_APPS = ("django.contrib.auth",
                  "django.contrib.contenttypes",
                  "django.contrib.sessions",
                  "django.contrib.sites",
                  "scs",  # scs must come before admin.
                  "django.contrib.admin",
                  "django.contrib.admindocs")

MIDDLEWARE_CLASSES = (
    'django.middleware.common.CommonMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
)

# Make this unique, and don't share it with anybody.
SECRET_KEY = '9a3!m32h23psjjkkjl#()hs+-sv@$3*mgq!m3s!encow2&*738'
