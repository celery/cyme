INSTALLED_APPS = ("django.contrib.auth",
                  "django.contrib.contenttypes",
                  "django.contrib.sessions",
                  "django.contrib.sites",
                  "django.contrib.admin",
                  "django.contrib.admindocs",
                  "scs")

ROOT_URLCONF = "scs.urls"
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': 'scs.sqlite',
        'USER': '',
        'PASSWORD': '',
        'HOST': '',
        'PORT': '',
    }
}

SITE_ID = 1
DEBUG = True
