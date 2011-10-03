"""cyme.tasks

- We have our own version of the webhook task.

- It simply forwards the original request, not depending on any semantics
  present in the query string of the request, nor in the data returned
  in the response.

"""
from __future__ import absolute_import

from celery.task import task
from requests import request

from . import __version__

#: Cyme User Agent string.
UA = "Celery/cyme v%s" % (__version__, )

#: Default HTTP headers to pass to the dispatched request.
DEFAULT_HEADERS = {"User-Agent": UA}


def response_to_dict(r):
    return dict(status_code=r.status_code, url=r.url,
                headers=r.headers, content=r.read())


@task(timeout=60)
def webhook(url, method="GET", params={}, data={}, headers=None, **kwargs):
    kwargs["timeout"] = kwargs.get("timeout", webhook.timeout)
    headers = {} if headers is None else headers
    return response_to_dict(request(method, url, params=params, data=data,
                                    headers=dict(headers, **DEFAULT_HEADERS)))
