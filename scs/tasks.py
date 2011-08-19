from celery.task import task
from requests import request

from . import __version__

#: SCS User Agent string.
UA = "Celery/SCS v%s" % (__version__, )

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
