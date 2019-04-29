import redis
import random
from initializers import initializeKLStore

def byte_to_string(string):
    """
    Function for decoding Redis output
    :param string: Redis output
    :return: decoded Redis output
    """
    return string.decode('utf-8')

def Apply_KLStore(name1, func):
    """
    This function gets a KL store in Redis named <name1> and a python function named <func> - which gets a string and
    returns a string – and applies <func> on each element of a list, for all lists of the KL store <name1>,
    transforming thus the lists of the KL store.
    :param name1: KLStore name
    :param func: Python functions to be applied in each element of every list
    """

    # get all KLStore keys
    keys = r.smembers(name1)

    pipe = r.pipeline()

    for key in keys:
        # new list to be inserted in Redis
        values = []

        # for every value in list
        for i in range(0, int(r.llen(key))):
            # get value and remove it from the list
            value = byte_to_string(r.lpop(key))

            # apply the python function to the value and insert it in the python list
            values.append(func(value))

        for value in values:
            # insert value to Redis list
            pipe.lpush(key, value)

    pipe.execute()

if __name__ == '__main__':

    def random_string_transformer(string):

        """
        Dummy function for testing
        :param string: a string to be transformed
        :return: the transformed string
        """

        return string[:random.randint(1, len(string))]


    initializeKLStore('clients')

    # initialize Redis connection
    r = redis.Redis(host='localhost', port=6379)

    Apply_KLStore('clients', random_string_transformer)