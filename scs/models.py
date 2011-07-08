from django.db import models
from django.utils.translation import ugettext_lazy as _

from scs.managers import NodeManager, QueueManager
from scs.utils import shellquote


class Queue(models.Model):
    objects = QueueManager()

    name = models.CharField(_(u"name"), max_length=128, unique=True)
    is_default = models.BooleanField(_("is default"), default=False)
    is_active = models.BooleanField(_("is active"), default=True)

    class Meta:
        verbose_name = _(u"queue")
        verbose_name_plural = _(u"queues")

    def __unicode__(self):
        return self.name


class Node(models.Model):
    Queue = Queue
    objects = NodeManager()

    name = models.CharField(_(u"name"), max_length=128, unique=True)
    queues = models.ManyToManyField(Queue, null=True)
    concurrency = models.IntegerField(default=None, null=True)
    is_active = models.BooleanField(default=True)

    class Meta:
        verbose_name = _(u"node")
        verbose_name_plural = _(u"nodes")

    def __unicode__(self):
        return self.name

    @property
    def strargv(self):
        return " ".join("%s %s" % (k, shellquote(str(v)))
                            for k, v in self.argv if v)

    @property
    def argv(self):
        return (("-n", self.name),
                ("-c", self.concurrency),
                ("-Q", ",".join(q.name for q in self.queues.active())))
