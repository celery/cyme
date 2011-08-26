import os
import sys

DB_NAME = os.environ["CYME_DB_NAME"] = "funtest.db"

sys.path.insert(0, os.path.join(os.getcwd(), os.pardir))


def teardown():
    if os.path.exists(DB_NAME):
        os.unlink(DB_NAME)
