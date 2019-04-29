import redis
import random

r = redis.Redis(host='localhost', port=6379)

def initializeKLStore(name1):
    """
    Creates a simple KLStore with random keys and random values
    :param name1: KLStore name
    """

    pipe = r.pipeline()
    pipe.sadd(name1, 'c12', 'c34', 'c76')
    pipe.lpush('c12', '12', '67')
    pipe.lpush('c34', '87', '12', '98')
    pipe.lpush('c76', '121', '72', '99', '179')

    pipe.execute()


def initializeKLStoreAdvanced(name, k):
    """
    Creates k KLStores with random keys and random values
    :param name: general name of each KLStore (e.g. sales)
    :param k: number of KLStores
    """

    pipe = r.pipeline()

    for i in range(1, k + 1):

        # j: number of keys in the current KLStore
        for j in range(0, random.randint(5, 10)):

            # e.g.  KLName = store1
            #       KLKey = store1:c2
            KLName = str(name) + str(i)
            KLKey = KLName + ':c' + str(random.randint(1, 45))

            # for every KLName create a set with all its keys
            pipe.sadd(KLName, KLKey)

            # m : number of elements in list
            for m in range(0, random.randint(5, 10)):
                # e.g.  key = store2:c7
                #       value = 55
                key = KLKey
                pipe.lpush(key, random.randint(1, 100))

    pipe.execute()


if __name__=="__main__":

    initializeKLStore('clients')

    initializeKLStore('transactions')

    initializeKLStoreAdvanced('store', 3)
