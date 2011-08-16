"""scs.urls"""

from __future__ import absolute_import

from django.contrib import admin
from django.conf.urls.defaults import (patterns, include, url,  # noqa
                                       handler500, handler404)

from piston.resource import Resource

from . import api
from . import views

admin.autodiscover()


class CsrfExemptResource(Resource):
    """A Custom Resource that is csrf exempt"""
    def __init__(self, handler, authentication=None):
        super(CsrfExemptResource, self).__init__(handler, authentication)
        self.csrf_exempt = getattr(self.handler, 'csrf_exempt', True)


node_resource = CsrfExemptResource(handler=api.NodeHandler)
queue_resource = CsrfExemptResource(handler=api.QueueHandler)


urlpatterns = patterns('',
    (r'^admin/doc/', include('django.contrib.admindocs.urls')),

    (r'^admin/', include(admin.site.urls)),
    (r'^api/node/(?P<nodename>[^/]+)/', node_resource),
    (r'^api/node/', node_resource),
    (r'^api/queue/(?P<name>[^/]+)/', queue_resource),
    (r'^api/queue/', queue_resource),
    (r'^(?P<app>[^/]+)/queue/(?P<rest>.+)', views.Apply.as_view()),
    (r'^(?P<app>[^/]+)/queues/?$', views.Queue.as_view()),
    (r'^(?P<app>[^/]+)/queues/(?P<name>.+?)/?$', views.Queue.as_view()),
    (r'^(?P<app>[^/]+)/instances/(?P<name>.+?)/queues/(?P<queue>.+?)?/?$',
        views.Consumer.as_view()),
    (r'^(?P<app>[^/]+)/instances/(?P<name>.+)?/autoscale/?',
        views.Autoscale.as_view()),
    (r'^(?P<app>[^/]+)/instances/(?P<name>.+)?/stats/?',
        views.InstanceStats.as_view()),
    (r'^(?P<app>[^/]+)/instances/(?P<name>.+)?/?$', views.Instance.as_view()),
    (r'^(?P<app>[^/]+)/query/(?P<uuid>.+?)/state/?', views.State.as_view()),
    (r'^(?P<app>[^/]+)/query/(?P<uuid>.+?)/result/?', views.Result.as_view()),
    (r'^(?P<app>[^/]+)/query/(?P<uuid>.+?)/wait/?', views.Wait.as_view()),
    (r'^(?P<app>[^/]+)?/?$', views.App.as_view()),
)
