import redis
import random
import sys

def byte_to_string(string):
    """
    Function for decoding Redis output
    :param string: Redis output
    :return: decoded Redis output
    """
    return string.decode('utf-8')

def Aggr_KLStore(name1, aggr, func):
    """
    This function gets a KL store in Redis named <name1> and a string named <aggr> or a python function.
    If you choose to use aggr then you have to put in the 3rd input an empty string and vice versa.
    In the case of using aggr, the aggregator applies to every list and replaces the old list with a new one which includes
    only one item, the result of the aggregation. If a list's item is not an number, it is omitted. If there are no
    numbers in the list then the new list that is created is empty.
    In the case of using func the Python function operates on a each list (which has strings) and updates the list
    with just one string, the result of the function.

    :param name1: KLStore name
    :param aggr: item from “avg/sum/count/min/max” list according to which we perfrom the aggregation
    :param func: python function <func> that operates on a list of strings and returns a string
    """

    allowed_aggr = ['avg', 'sum', 'count', 'min', 'max', '']

    # user puts something not accepted in the 'aggr' input
    if aggr not in allowed_aggr:
        raise ValueError('The aggregator you inserted is not supported. Please try avg, sum, count, min, or max')
    else:

        # user uses both aggr and func
        if aggr != '' and func != '':
            raise ValueError('Please enter 1 of 2 arguments')

        # get all KLStore keys
        keys = r.smembers(name1)

        pipe = r.pipeline()

        if aggr == 'avg':

            for key in keys:

                # sum of values that are numbers
                sum = 0

                # key's list length
                length = r.llen(key)

                # count of values that are numbers
                count = 0

                for i in range(0, length):
                    # get value
                    value = r.lpop(key)

                    # if a value is a number keep going
                    try:
                        value = float(byte_to_string(value))

                    # else continue to the next value
                    except:
                        continue

                    sum += value
                    count += 1

                # if there was at least 1 value that was a number calculate the average
                if count != 0:
                    average = sum / count
                    pipe.lpush(key, average)
                # else leave the list empty (as it is)

        elif aggr == 'sum':

            for key in keys:

                # sum of values that are numbers
                sum = 0

                # key's list length
                length = r.llen(key)

                for i in range(0, length):
                    # get value
                    value = r.lpop(key)

                    # if a value is a number keep going
                    try:
                        value = float(byte_to_string(value))

                    # else continue to the next value
                    except:
                        continue

                    sum += value

                # if there was at least 1 value that was a number put sum in the list
                if sum != 0:
                    pipe.lpush(key, sum)
                # else leave the list empty (as it is)

        elif aggr == 'count':

            for key in keys:

                # key's list length
                length = r.llen(key)

                # count of values that are numbers
                count = 0

                for i in range(0, length):
                    # get value
                    value = r.lpop(key)

                    # if a value is a number keep going
                    try:
                        value = float(byte_to_string(value))

                    # else continue to the next value
                    except:
                        continue

                    count += 1

                # if there was at least 1 value that was a number put the count in the list
                if count != 0:
                    pipe.lpush(key, count)
                # else leave the list empty (as it is)

        elif aggr == 'min':

            for key in keys:

                # minimum value
                min = sys.maxsize

                length = r.llen(key)

                for i in range(0, length):

                    # if a value is a number keep going
                    try:
                        value = float(byte_to_string(value))

                    # else continue to the next value
                    except:
                        continue

                    if value < min:
                        min = value

                # if there was at least 1 value that was a number put the min in the list
                if min != sys.maxsize:
                    pipe.lpush(key, min)
                # else leave the list empty (as it is)

        elif aggr == 'max':

            for key in keys:

                # maximum value
                min = - sys.maxsize -1

                length = r.llen(key)

                for i in range(0, length):

                    # if a value is a number keep going
                    try:
                        value = float(byte_to_string(value))

                    # else continue to the next value
                    except:
                        continue

                    if value > max:
                        max = value

                # if there was at least 1 value that was a number put the max in the list
                if min != - sys.maxsize -1:
                    pipe.lpush(key, max)
                # else leave the list empty (as it is)

        # if the user leaves empty the aggr input and uses the func
        elif aggr == '' and func is not None:

            for key in keys:

                # the list values will be stored temporarily here
                list = []
                length = r.llen(key)

                for i in range(0, length):
                    # trasform value to string
                    value = str(byte_to_string(r.lpop(key)))

                    # put value to Python list
                    list.append(value)

                # put function's result to Redis list
                pipe.lpush(key, func(list))

        # if user doesn't use either an aggregator or a function
        else:
            raise ValueError('Please enter a function or an aggregator')

        pipe.execute()

if __name__ == '__main__':

    def random_string_list_transformer(list):
        """
        Dummy function for testing
        :param list: the input list
        :return: the string that comes from the list editing
        """
        return str(len(list) * random.randint(1, 10))

    # initialize Redis connection
    r = redis.Redis(host='localhost', port=6379)

    lucky_number = random.randint(1, 7)
    if lucky_number == 1:
        Aggr_KLStore('clients', 'min', '')
    elif lucky_number == 2:
        Aggr_KLStore('clients', 'avg', '')
    elif lucky_number == 3:
        Aggr_KLStore('clients', 'max', '')
    elif lucky_number == 4:
        Aggr_KLStore('clients', 'sum', '')
    elif lucky_number == 5:
        Aggr_KLStore('clients', 'count', '')
    elif lucky_number == 6:
        Aggr_KLStore('clients', '', random_string_list_transformer)