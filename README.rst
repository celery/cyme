===============================================
 SCS - Simple Celery Service
===============================================

:Version: 0.0.1
:Web: http://celeryproject.org/
:Download: http://pypi.python.org/pypi/SCS/
:Keywords: celery, task queue, job queue, PaaS, Cloud, Celery as a Service.

--

Introduction
============

The agent consists of the following components:

Supervisor
~~~~~~~~~~

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

See :class:`~scs.supervisor.Supervisor`.






Installation
=============

You can install ``scs`` either via the Python Package Index (PyPI)
or from source.

To install using ``pip``,::

    $ pip install scs

To install using ``easy_install``,::

    $ easy_install scs

Getting Help
============

Mailing list
------------

For discussions about the usage, development, and future of celery,
please join the `celery-users`_ mailing list. 

.. _`celery-users`: http://groups.google.com/group/celery-users/

IRC
---

Come chat with us on IRC. The `#celery`_ channel is located at the `Freenode`_
network.

.. _`#celery`: irc://irc.freenode.net/celery
.. _`Freenode`: http://freenode.net


License
=======

This software is licensed under the ``New BSD License``. See the ``LICENSE``
file in the top distribution directory for the full license text.

.. # vim: syntax=rst expandtab tabstop=4 shiftwidth=4 shiftround

