========================
 cyme.models
========================

.. contents::
    :local:
.. currentmodule:: cyme.models

.. automodule:: cyme.models

    .. autoclass:: Broker

        .. attribute:: url

            AMQP (kombu) url.

        .. method:: connection

            Return a new :class:`~kombu.Connection` to this broker.

        .. automethod:: as_dict

        .. attribute:: pool

            A connection pool with connections to this broker.

        .. attribute:: objects

            The manager for this model is :class:`~cyme.managers.BrokerManager`.


    .. autoclass:: Queue

        .. attribute:: name

            Queue name (unique, max length 128)

        .. attribute:: exchange

            Exchange name (max length 128)

        .. attribute:: exchange_type

            Exchange type (max length 128)

        .. attribute:: routing_key

            Routing key/binding key (max length 128).

        .. attribute:: options

            Additional JSON encoded queue/exchange/binding options.
            see :mod:`kombu.compat` for a list of options supported.

        .. attribute:: is_enabled

            Not in use.

        .. attribute:: created_at

            Timestamp created.

        .. automethod:: as_dict

        .. attribute:: objects

            The manager for this model is :class:`~cyme.managers.QueueManager`.


    .. autoclass:: Instance

        .. attribute:: name

            Name of the instance.

        .. attribute:: queues

            Queues this instance should consume from (many to many
            relation to :class:`Queue`).

        .. attribute:: max_concurrency

            Autoscale setting for max concurrency.
            (maximum number of processes/threads/green threads when the
            worker is active).

        .. attribute:: min_concurrency

            Autoscale setting for min concurrency.
            (minimum number of processes/threads/green threads when
            the worker is idle).

        .. attribute:: is_enabled

            Flag set if this instance should be running.

        .. attribute:: created_at

            Timestamp of when this instance was first created.

        .. attribute:: broker

            The broker this instance should connect to.
            (foreign key to :class:`Broker`).

        .. attribute:: Broker

            Broker model class used (default is :class:`Broker`)

        .. attribute:: Queue

            Queue model class used (default is :class:`Queue`)

        .. attribute:: MultiTool

            Class used to start/stop and restart celeryd instances.
            (Default is :class:`celery.bin.celeryd_multi.MultiTool`).

        .. attribute:: objects

            The manager used for this model is
            :class:`~cyme.managers.InstanceManager`.

        .. attribute:: cwd

            Working directory used by all instances.
            (Default is :file:`/var/run/cyme`).


        .. automethod:: as_dict

        .. automethod:: enable

        .. automethod:: disable

        .. automethod:: start

        .. automethod:: stop

        .. automethod:: restart

        .. automethod:: alive

        .. automethod:: stats

        .. automethod:: autoscale

        .. automethod:: responds_to_ping

        .. automethod:: responds_to_signal

        .. automethod:: consuming_from

        .. automethod:: add_queue

        .. automethod:: cancel_queue

        .. automethod:: getpid

        .. automethod:: _action

        .. automethod:: _query
