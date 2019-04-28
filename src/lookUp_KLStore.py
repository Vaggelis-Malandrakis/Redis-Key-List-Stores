import redis


def lookUp_KLStore(name1, name2):
    """
    This function gets two KL stores and for each element of the list in <name1>,
    performs a lookup in the keys of <name2>, gets the list of the matched key,
    and replaces its value in <name1> KL store.

    :param name1: a KL store in Redis named <name1>
    :param name2: a KL store in Redis named <name2>
    """

    try:
        r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

        keys1 = r.smembers(name1)
        keys2 = r.smembers(name2)

        for key1 in keys1:
            list1 = r.lrange(key1, 0, -1)
            for value1 in list1:
                if value1.strip() in keys2:
                    list2 = r.lrange(value1.strip(), 0, -1)
                    r.lrem(key1, 0, value1)

                    for value2 in list2:
                        r.rpush(key1, value2)

    except Exception as e:
        raise Exception(e)


if __name__ == '__main__':
    lookUp_KLStore("Sales2_csv", "Transactions_excel")