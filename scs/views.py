"""scs.views"""

from __future__ import absolute_import

from django.http import HttpResponseRedirect
from django.shortcuts import render_to_response
from django.template import RequestContext

from .models import Node
from .agent import cluster


def index(request, template_name="scs/index.html"):
    return render_to_response(template_name,
            context_instance=RequestContext(request, dict(
                nodes=Node.objects.all(),
                next=request.path)))


def enable_node(request, nodename):
    cluster.enable(nodename)
    return HttpResponseRedirect(request.REQUEST.get("next"))


def disable_node(request, nodename):
    cluster.disable(nodename)
    return HttpResponseRedirect(request.REQUEST.get("next"))


def restart_node(request, nodename):
    cluster.restart(nodename)
    return HttpResponseRedirect(request.REQUEST.get("next"))


def create_node(request):
    cluster.add(request.POST.get("name"))
    return HttpResponseRedirect(request.POST.get("next"))


def delete_node(request, nodename):
    cluster.remove(nodename)
    return HttpResponseRedirect(request.GET.get("next"))
