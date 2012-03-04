"""cyme.admin

- Django Admin interface extensions for Cyme.

"""
from __future__ import absolute_import

from django.contrib import admin
from django.utils.html import escape
from django.utils.translation import ugettext_lazy as _

from djcelery.admin_utils import action, display_field, fixedwidth
from djcelery.humanize import naturaldate

from .models import Broker, Instance, Queue
from .branch.supervisor import supervisor


@display_field(_("max/min concurrency"), "max_concurrency")
def maxmin_concurrency(instance):
    return "%s / %s" % (instance.max_concurrency, instance.min_concurrency)


@display_field(_("created"), "created_at")
def created_at(instance):
    return """<div title="%s">%s</div>""" % (
                escape(str(instance.created_at)),
                escape(naturaldate(instance.created_at)))


@display_field(_("status"), "is_enabled")
def status(instance):
    enabled = "Enabled" if instance.is_enabled else "Disabled"
    if instance.alive():
        state, color = "ONLINE", "green"
    else:
        state, color = "OFFLINE", "red"
    return """<b>%s (<span style="color: %s;">%s</span>)</b>""" % (
            enabled, color, state)


class InstanceAdmin(admin.ModelAdmin):
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
    actions = ["disable_instances",
               "enable_instances",
               "restart_instances"]

    @action(_("Disable selected instances"))
    def disable_instances(self, request, queryset):
        for instance in queryset:
            instance.disable()
        supervisor.verify(queryset).wait()

    @action(_("Enable selected instances"))
    def enable_instances(self, request, queryset):
        for instance in queryset:
            instance.enable()
        supervisor.verify(queryset).wait()

    @action(_("Restart selected instances"))
    def restart_instances(self, request, queryset):
        supervisor.restart(queryset).wait()


admin.site.register(Broker)
admin.site.register(Instance, InstanceAdmin)
admin.site.register(Queue)
