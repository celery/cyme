"""cyme.api.urls"""

from __future__ import absolute_import

from django.contrib import admin
from django.conf.urls.defaults import (patterns, include, url,  # noqa
                                       handler500, handler404)

from . import views

uApp = r'(?P<app>[^/]+)'
uNowait = r'(?P<nowait>!/)?'

admin.autodiscover()


def _o_(u):
    return (u.replace("APP", uApp)
             .replace("!", uNowait))

urlpatterns = patterns('',
    (r'^ping/$', views.ping.as_view()),
    (r'^admin/doc/', include('django.contrib.admindocs.urls')),

    (r'^admin/', include(admin.site.urls)),
    (r'^branches/(?P<branch>.+?)?/?$', views.Branch.as_view()),
    (_o_(r'^APP/queue/!(?P<rest>.+)'), views.apply.as_view()),
    (_o_(r'^APP/queues/!/?$'), views.Queue.as_view()),
    (_o_(r'^APP/queues/!(?P<name>.+?)/?$'), views.Queue.as_view()),
    (_o_(r'^APP/instances/!(?P<name>.+?)/queues/(?P<queue>.+?)?/?$'),
        views.Consumer.as_view()),
    (_o_(r'^APP/instances/!?(?P<name>.+)?/autoscale/?'),
        views.autoscale.as_view()),
    (_o_(r'^APP/instances/!(?P<name>.+)?/stats/?'),
        views.instance_stats.as_view()),
    (_o_(r'^APP/instances/!(?P<name>.+?)?/?$'), views.Instance.as_view()),
    (_o_(r'^APP/query/(?P<uuid>.+?)/state/?'), views.task_state.as_view()),
    (_o_(r'^APP/query/(?P<uuid>.+?)/result/?'), views.task_result.as_view()),
    (_o_(r'^APP/query/(?P<uuid>.+?)/wait/?'), views.task_wait.as_view()),
    (_o_(r'^APP?/?$'), views.App.as_view()),
)
