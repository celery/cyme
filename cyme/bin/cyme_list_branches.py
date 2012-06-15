"""cyme.bin.cyme_list_branches"""
from __future__ import absolute_import

from .base import app

import anyjson


def get_branches(broker=None, limit=None):
    from cyme.branch.controller import Branch
    from celery import current_app as celery
    if limit:
        limit = int(limit)

    args = [broker] if broker else []
    conn = celery.broker_connection(*args)
    return Branch(connection=conn).all(limit=limit)


@app()
def main(env, argv):
    opts = dict(arg.split('=', 1)
                    for arg in argv[1:] if arg.startswith('--'))
    print(anyjson.serialize(get_branches(opts.get('--broker'),
                                         opts.get('--limit'))))


if __name__ == '__main__':
    main()
