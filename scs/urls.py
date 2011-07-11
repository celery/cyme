from django.conf.urls.defaults import (patterns, include, url,  # noqa
                                       handler500, handler404)

from django.contrib import admin

from piston.resource import Resource

from scs import views
from scs import api

admin.autodiscover()


class CsrfExemptResource(Resource):
    """A Custom Resource that is csrf exempt"""
    def __init__(self, handler, authentication=None):
        super(CsrfExemptResource, self).__init__(handler, authentication)
        self.csrf_exempt = getattr(self.handler, 'csrf_exempt', True)


node_resource = CsrfExemptResource(handler=api.NodeHandler)


urlpatterns = patterns('',
    (r'^admin/doc/', include('django.contrib.admindocs.urls')),

    (r'^admin/', include(admin.site.urls)),
    (r'^api/node/(?P<nodename>[^/]+)/', node_resource),
    (r'^api/node/', node_resource),
    (r'^/?$', views.index),
)
