# !/usr/bin/env python3

import redis
import os

FILE_PATH = os.path.abspath(os.path.dirname(__file__))


def filter_KLStore(name1, expression):
    """
    This function gets a KL store in Redis named <name1> and a boolean expression and
    applies this expression on each element of each list of <name1>.
    If the return value is true, the element remains in the list, otherwise it is removed.

    :param name1: a KL store in Redis named <name1>
    :param expression: a string representing a valid python boolean expression
    """

    try:

        # The decode_repsonses flag here directs the client to convert the responses from Redis into Python strings
        # using the default encoding utf-8.  This is client specific.
        r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

        keys = r.smembers(name1)
        for key in keys:
            list = r.lrange(key, 0, -1)
            for index, value in enumerate(list):
                val = value.strip()
                expression = expression.replace(" ", "")
                if eval(expression):
                    r.lrem(key, 0, value)

    except Exception as e:
        raise Exception(e)


if __name__ == '__main__':
    filter_KLStore("Sales2_csv", "val == 't4'")
