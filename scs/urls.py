from django.conf.urls.defaults import (patterns, include, url,  # noqa
                                       handler500, handler404)

from django.contrib import admin

from scs import views

admin.autodiscover()


urlpatterns = patterns('',
    (r'^admin/doc/', include('django.contrib.admindocs.urls')),

    (r'^admin/', include(admin.site.urls)),
    (r'^/?$', views.index),
)
