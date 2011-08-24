"""scs.admin

- Django Admin interface extensions for SCS.

"""
from __future__ import absolute_import

from django.contrib import admin
from django.utils.html import escape
from django.utils.translation import ugettext_lazy as _

from djcelery.admin_utils import action, display_field, fixedwidth
from djcelery.utils import naturaldate

from .models import Broker, Node, Queue
from .agent.supervisor import supervisor


@display_field(_("max/min concurrency"), "max_concurrency")
def maxmin_concurrency(node):
    return "%s / %s" % (node.max_concurrency, node.min_concurrency)


@display_field(_("created"), "created_at")
def created_at(node):
    return """<div title="%s">%s</div>""" % (
                escape(str(node.created_at)),
                escape(naturaldate(node.created_at)))


@display_field(_("status"), "is_enabled")
def status(node):
    enabled = "Enabled" if node.is_enabled else "Disabled"
    if node.alive():
        state, color = "ONLINE", "green"
    else:
        state, color = "OFFLINE", "red"
    return """<b>%s (<span style="color: %s;">%s</span>)</b>""" % (
            enabled, color, state)


class NodeAdmin(admin.ModelAdmin):
    detail_title = _("Instance detail")
    list_page_title = _("Instances")
    date_hierarchy = "created_at"
    fieldsets = (
            (None, {
                "fields": ("name", "max_concurrency", "min_concurrency",
                           "_queues", "is_enabled", "_broker"),
                "classes": ("extrapretty", ),
            }), )
    list_display = (fixedwidth("name", pt=10), maxmin_concurrency,
                    status, "broker")
    read_only_fields = ("created_at", )
    list_filter = ("name", "max_concurrency", "min_concurrency", "_queues")
    search_fields = ("name", "max_concurrency", "min_concurrency", "_queues")
    actions = ["disable_nodes",
               "enable_nodes",
               "restart_nodes"]

    @action(_("Disable selected instances"))
    def disable_nodes(self, request, queryset):
        for node in queryset:
            node.disable()
        supervisor.verify(queryset).wait()

    @action(_("Enable selected instances"))
    def enable_nodes(self, request, queryset):
        for node in queryset:
            node.enable()
        supervisor.verify(queryset).wait()

    @action(_("Restart selected instances"))
    def restart_nodes(self, request, queryset):
        supervisor.restart(queryset).wait()


admin.site.register(Broker)
admin.site.register(Node, NodeAdmin)
admin.site.register(Queue)
