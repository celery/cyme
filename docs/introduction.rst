===============================================
 Introduction
===============================================

.. contents::
    :local:

Overview
========

The SCS agent manages Celery worker instances for a particular
machine (virtual or physical).

Instances can be created, disabled, deleted and configured
via an HTTP API.  The agent also ensures that all the instances
it controls are actually running, and is running with the configuration
described in the database.

Programs
--------

* :mod:`scs-agent <scs.management.commands.scs_agent>`.

Models
------

* :class:`~scs.models.Node`.
* :class:`~scs.models.Queue`.
* :class:`~scs.models.Broker`.

Supervisor
----------
:see: :class:`~scs.supervisor.Supervisor`.

The supervisor wakes up at intervals to monitor changes in the model.
It can also be requested to perform specific operations, and these
operations can be either async or sync.

It is responsible for:

* Stopping removed instances.
* Starting new instances.
* Restarting unresponsive/killed instances.
* Making sure the instances consumes from the queues specified in the model,
  sending ``add_consumer``/- ``cancel_consumer`` broadcast commands to the
  nodes as it finds inconsistencies.
* Making sure the max/min concurrency setting is as specified in the
  model,  sending ``autoscale`` broadcast commands to the nodes
  as it finds inconsistencies.

The supervisor is resilient to intermittent connection failures,
and will autoretry any operation that is dependent on a broker.

Since workers cannot respond to broadcast commands while the
broker is offline, the supervisor will not restart affected
instances until the instance has had a chance to reconnect (decided
by the :attr:`wait_after_broker_revived` attribute).


HTTP
----

The http server currently serves up an admin instance
where you can add, remove and modify instances.

The http server can be disabled using the :option:`--without-http` option.

SRS
---
:see: :class:`~scs.srs.SRSAgent`

The SRS agent can be disabled using the :option:`--without-srs` option.
