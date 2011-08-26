===============================================
 Introduction
===============================================

.. contents::
    :local:

Synopsis
========

The Cyme agent is a distributed application, where each instance
handles the Celery worker nodes on a machine (physical or virtual).
Distributed as in it reroutes messages to agents that can handle them,
for example if the current agent does not hold the definition of a queue,
it uses broadcast messaging to ask other agents for that information.

Requirements
============

* Python 2.6 or later.

* `cl`_
* `eventlet`_
* `django`_
* `django-celery`_

.. _`cl`: http://github.com/ask/cl
.. _`eventlet`: http://pypi.python.org/pypi/eventlet
.. _`django`: http://djangoproject.com/
.. _`django-celery`: http://pypi.python.org/pypi/django-celery`

.. note::

    An updated list of requirements can always be found
    in the :file:`requirements/` directory of the Cyme distribution.
    This directory contains pip requirements files for different
    scenarios.

Getting started
===============

You can run as many agents as needed: one or multiple.
It is also possible to run multiple agents on the same machine
by specifying a custom HTTP port, and root directory for each
agent.

Start one or more agents::

    $ cyme-agent :8001 -i agent1 -D agent1/

    $ cyme-agent :8002 -i agent2 -D agent2/


The default HTTP port is 8000, and the default root directory
is :file:`instances/`.  The root directory must be writable
by the user the agent is running as.  The logs and pid files of
every worker node will be stored in this directory.

Create a new application named ``foo``::

    $ curl -X POST -i http://localhost:8001/foo/
    HTTP/1.1 201 CREATED
    Content-Type: application/json
    Date: Mon, 15 Aug 2011 22:06:43 GMT
    Transfer-Encoding: chunked

    {"name": "foo", "broker": {"password": "guest",
                               "hostname": "127.0.0.1",
                               "userid": "guest",
                               "port": 5672,
                               "virtual_host": "/"}}


Note that we can edit the broker connection details here
by using a POST request::

    $ curl -X POST -i http://localhost/bar/ -d \
        'hostname=w1&userid=me&password=me&vhost=/'


.. note::

    For convenience and full client support **PUT** can
    be replaced with a **POST** instead, and it will result in the same
    action being performed.

    Also, for **POST**, **PUT** and **DELETE** the query part of the
    URL can be used instead of actual post data.


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
        --workdir=agent1/
        --pidfile=agent1/celeryd@%n.pid
        --logfile=agent1/celeryd@%n.log
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
This queue binds the exchange ``tasks`` with routing key ``tasks``.
(note that the queue name will be used as both exchange name and routing key
if these are not provided).

Create the queue by performing the following request::

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



API Reference
=============


Applications
------------

* Create new named application

::

  [PUT|POST] http://agent:port/<name>/?hostname=str
                                      ?port=int
                                      ?userid=str
                                      ?password=str
                                      ?virtual_host=str

If ``hostname`` is not provided, then any other broker parameters
will be ignored and the default broker will be used.

* List all available applications

::

  GET http://agent:port/

* Get the configuration for app by name

::

  GET http://agent:port/name/


Instances
---------

* Create and start an anonymous instance associated with app

::

    [PUT|POST] http://agent:port/<app>/instances/


This will return the details of the new id,
including the instance name (which for anonymous instances
is an UUID).


* Create and start a named instance associated with app:

::

    [PUT|POST] http://agent:port/<app>/instances/<name>/


* List all available instances associated with an app

::

    GET http://agent:port/<app>/

* Get the details of an instance by name

::

    GET http://agent:port/<app>/instances/<name>/


* Delete an instance by name.

::

    DELETE http://agent:port/<app>/instances/<name>/


Queues
------

* Create a new queue declaration by name

::

    [PUT|POST] http://agent:port/<app>/queues/<name>/?exchange=str
                                                     ?exchange_type=str
                                                     ?routing_key=str
                                                     ?options=json dict

``exchange`` and ``routing_key`` will default to the queue name if not
provided, and ``exchange_type`` will default to ``direct``.
``options`` is a json encoded mapping of additional queue, exchange and
binding options, for a full list of supported options see
:meth:`kombu.compat.entry_to_queue`.


* Get the declaration for a queue by name

::

    GET http://agent:port/<app>/queues/<name>/

* Get a list of available queues

::

    GET http://agent:port/<app>/queues/


Consumers
---------

Every instance can consume from one or more queues.
Queues are referred to by name, and there must exist a full declaration
for that name.


* Tell an instance by name to consume from queue by name

::

    [PUT|POST] http://agent:port/<app>/instances/<instance>/queues/<queue>/


* Tell an instance by name to stop consuming from queue by name

::

    DELETE http://agent:port/<app>/instances/<instance>/queues/<queue>/


Queueing Tasks
--------------

Queueing an URL will result in one of the worker nodes to execute that
request as soon as possible.

::

    [verb] http://agent:port/<app>/queue/<queue>/<url>?get_data
    post_data



The ``verb`` can be any supported HTTP verb, such as
``HEAD``, ``GET``, ``POST``, ``PUT``, ``DELETE``, ``TRACE``,
``OPTIONS``, ``CONNECT``, and ``PATCH``.
The worker will then use the same verb when performing the request.
Any get and post data provided will also be forwarded.


When you queue an URL a unique identifier is returned,
you can use this identifier (called an UUID) to query the status of the task
or collect the return value.  The return value of the task is the HTTP
response of the actual request performed by the worker.


**Examples**::

    GET http://agent:port/<app>/queue/tasks/http://m/import_contacts?user=133


    POST http://agent:port/<app>/queue/tasks/http://m/import_user
    username=George Costanza
    company=Vandelay Industries


Querying Task State
-------------------


* To get the current state of a task

::

    GET http://agent:port/<app>/query/<uuid>/state/


* To get the return value of a task

::

    GET http://agent:port/<app>/query/<uuid>/result/


* To wait for a task to complete, and return its result.

::

    GET http://agent:port/<app>/query/<uuid>/wait/


Instance details and statistics
-------------------------------

To get configuration details and statistics for a particular
instance::

    GET http://agent:port/<app>/instance/<name>/stats/


Autoscale
---------

* To set the max/min concurrency settings of a node

::

    POST http://agent:port/<app>/instance/<name>/autoscale/?max=int
                                                           ?min=int

* To get the max/min concurrency settings of a node

::

    GET http://agent:port/<app>/instance/<name>/autoscale/

Components
==========

Programs
--------

* :mod:`cyme <cyme.management.commands.cyme`.

    This is the management application, speaking HTTP with the clients.
    See ``cyme --help`` for full description and command line arguments.

* :mod:`cyme-agent <cyme.management.commands.cyme_agent>`.

    This runs an agent.
    See ``cyme-agent --help`` for full description and command line arguments.

Models
------

The agent uses an SQLite database to store state,
but this can also be another database system (MySQL, PostgreSQL, Oracle, DB2).

App
~~~
:see: :class:`cyme.models.App`.

Every node belongs to an application, and the application
contains the default broker configuration.

Broker
~~~~~~
:see: :class:`cyme.models.Broker`.

The connection parameters for a specific broker (``hostname``, ``port``,
``userid``, ``password``, ``virtual_host``)

Node
~~~~
:see: :class:`cyme.models.Node`.

This describes a Celery worker node that should be running on this
agent, the queues it should consume from and its max/min concurrency
settings. It also describes what broker instance the node should be
connecting to (which if not specified will default to the broker of the
app the node belongs to).

Queue
~~~~~
:see: :class:`cyme.models.Queue`.

A queue declaration: name, exchange, exchange type, routing key,
and options.  Options is a json encoded mapping of queue, exchange and binding
options supported by :func:`kombu.compat.entry_to_queue`.

Supervisor
==========
:see: :mod:`cyme.supervisor`.

The supervisor wakes up at intervals to monitor for changes in the model.
It can also be requested to perform specific operations, e.g.
restart a node, add queues to node,
and these operations can be either async or sync.

It is responsible for:

* Stopping removed instances.
* Starting new instances.
* Restarting unresponsive/killed instances.
* Making sure the instances consumes from the queues specified in the model,
  sending add_consumer/- cancel_consumer broadcast commands
  to the nodes as it finds inconsistencies.
* Making sure the max/min concurrency setting is as specified in
  the model, sending autoscale broadcast commands to the nodes as it
  finds inconsistencies.

The supervisor is resilient to intermittent connection failures,
and will auto-retry any operation that is dependent on a broker.

Since workers cannot respond to broadcast commands while the broker
is off-line, the supervisor will not restart affected instances
until the instance has had a chance to reconnect
(decided by the wait_after_broker_revived attribute).

Controller
==========
:see: :mod:`cyme.controller`.

The controller is a series of `cl`_ actors to control applications,
nodes and queues.  It is used by the HTTP interface, but can also
be used directly.

HTTP
====

The http server currently serves up an admin instance
where you can add, remove and modify instances.

The http server can be disabled using the :option:`--without-http` option.
