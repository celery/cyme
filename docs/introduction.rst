===============================================
 Introduction
===============================================

.. contents::
    :local:

Getting started
===============

Start one or more agents::

    $ scs-agent :8001 -i agent1 -D /var/run/scs/agent1

    $ scs-agent :8002 -i agent2 -D /var/run/scs/agent2

Create a new Celery worker instance::

    $ curl -X PUT -i http://localhost:8001/foo/instances/
    HTTP/1.1 201 CREATED
    Content-Type: application/json
    Date: Mon, 15 Aug 2011 15:25:11 GMT
    Transfer-Encoding: chunked

    {"is_enabled": true,
     "name": "a35f2518-13bb-4403-bbdf-dd8751077712",
     "queues": [],
     "broker": {"password": "guest",
                "userid": "guest",
                "hostname": "127.0.0.1",
                "virtual_host": "/",
                "port": 5672},
     "max_concurrency": 1,
     "min_concurrency": 1}

Note that this instance is created on a random agent, not necessarily the
agent that you are currently speaking to over HTTP.  If you want to edit
the data on a specific agent, please do so by using that agents
admin interface at http://localhost:8001/admin/.

In the affected agents log you should now see something like this::

    {582161d7-1187-4242-9874-32cd7186ba91} --> Node.add(name=None)
    {Supervisor} wake-up
    {Supervisor} a35f2518-13bb-4403-bbdf-dd8751077712 node.restart
    celeryd-multi restart --suffix="" --no-color a35f2518-13bb-4403-bbdf-dd8751077712
        -Q 'dq.a35f2518-13bb-4403-bbdf-dd8751077712'
        --workdir=/var/run/scs/agent1
        --pidfile=/var/run/scs/agent1/celeryd@%n.pid
        --logfile=/var/run/scs/agent1/celeryd@%n.log
        --loglevel=DEBUG --autoscale=1,1
        -- broker.host=127.0.0.1 broker.port=5672
           broker.user=guest broker.password=guest broker.vhost=/
    celeryd-multi v2.3.1
    > a35f2518-13bb-4403-bbdf-dd8751077712: DOWN
    > Restarting node a35f2518-13bb-4403-bbdf-dd8751077712: OK
    {Supervisor} a35f2518-13bb-4403-bbdf-dd8751077712 pingWithTimeout: 0.1
    {Supervisor} a35f2518-13bb-4403-bbdf-dd8751077712 pingWithTimeout: 0.5
    {Supervisor} a35f2518-13bb-4403-bbdf-dd8751077712 pingWithTimeout: 0.9
    {Supervisor} a35f2518-13bb-4403-bbdf-dd8751077712 successfully restarted
    {Supervisor} wake-up
    {582161d7-1187-4242-9874-32cd7186ba91} <-- ok={
        'is_enabled': True,
        'name': 'a35f2518-13bb-4403-bbdf-dd8751077712',
        'queues': [],
        'broker': {'password': u'guest',
                   'hostname': u'127.0.0.1',
                   'userid': u'guest',
                   'port': 5672,
                   'virtual_host': u'/'},
        'max_concurrency': 1,
        'min_concurrency': 1}


Now that we have created an instance we can list the available instances::

    $ curl -X GET -i http://localhost:8001/foo/instances/
    HTTP/1.1 200 OK
    Content-Type: application/json
    Date: Mon, 15 Aug 2011 15:28:33 GMT
    Transfer-Encoding: chunked

    ["a35f2518-13bb-4403-bbdf-dd8751077712"]

Note that this will list instances for every agent, not just the agent you are
currently speaking to over HTTP.

Let's create a queue declaration for a queue named ``tasks``.
This queue binds the the exchange ``tasks`` with routing key ``tasks``.
(note that the queue name will be used as both exchange name and routing key
if these are not provided).

    $ curl -X POST -d 'exchange=tasks&routing_key=tasks' \
        -i http://localhost:8001/foo/queues/tasks/
    HTTP/1.1 201 CREATED
    Content-Type: application/json
    Date: Mon, 15 Aug 2011 16:03:07 GMT
    Transfer-Encoding: chunked

    {"exchange": "t2",
     "routing_key": "t2",
     "options": null,
     "name": "t2",
     "exchange_type": null}


The queue declaration should now have been stored on one of the agents,
and we can verify that by retrieving a list of all queues defined on all
agents::

    $ curl -X GET -i http://localhost:8001/foo/queues/
    HTTP/1.1200 OK
    Content-Type: application/json
    Date: Mon, 15 Aug 2011 16:08:37 GMT
    Transfer-Encoding: chunked

    ["tasks"]

Now we can make our worker node consume from the ``tasks`` queue to process
tasks sent to it::

    $ curl -X PUT -i \
        http://localhost:8001/foo/instances/a35f2518-13bb-4403-bbdf-dd8751077712/queues/t2
    HTTP/1.1 201 CREATED
    Content-Type: application/json
    Date: Mon, 15 Aug 2011 16:06:32 GMT
    Transfer-Encoding: chunked

    {"ok": "ok"}

In the logs for the agent that controls this instance you should now see::

    [2011-08-15 16:06:32,226: WARNING/MainProcess]
        {Supervisor} a35f2518-13bb-4403-bbdf-dd8751077712: node.consume_from: tasks


If the test was successful you can clean up after yourself by,

* Cancelling consuming from the ``tasks`` queue::

    $ curl -X DELETE -i \
        http://localhost:8001/foo/instances/a35f2518-13bb-4403-bbdf-dd875107772/queues/tasks

* Deleting the ``tasks`` queue::

    $ curl -X DELETE -i http://localhost:8001/foo/queues/


* and finally, deleting the worker instance::

    $ curl -X DELETE -i http://localhost:8001/instances/a35f2518-13bb-4403-bbdf-dd8751077712/


The worker instance should now be shutdown by the agents supervisor.


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
