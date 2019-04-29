import random
import re
from initializers import initializeKLStoreAdvanced
import redis

def random_list_element(list):
    """
    Returns a random value from a list
    """
    list_length = int(r.llen(list))
    random_index = random.randint(0, list_length - 1)
    random_element = r.lindex(list, random_index)
    return float(byte_to_string(random_element))

def byte_to_string(string):
    """
    Function for decoding Redis output
    :param string: Redis output
    :return: decoded Redis output
    """
    return string.decode('utf-8')

def ProjSel_KLStore(output_name, pnames, expression):
    """
    The goal of this function is to perform a join on the common keys of some KL stores, creating a new KL
    store having keys the common keys and corresponding list the concatenation of the individual lists in
    nm_1, nm_2,…, nm_n. In other words, if there is a key k in all KL stores nm_1, nm_2,…, nm_n, and Lk1, Lk2,…Lkn
    the corresponding lists, then we insert a key-list pair in the new KL store as (k, concatenation(Lk1, Lk2,…Lkn)).

    In addition, we would like to specify at the same time a filtering condition for this new KL store, based on the
    key and the contents of the lists involved, for example “key <> ‘t22’ and nm_2 > 20”. Such an expression is
    ill-defined because lists are not atoms. However, we would like to keep these semantics for reasons that have to
    do with user experience and understanding at the conceptual level (not discussed here). For the scope of this
    assignment, translate it as “any element of the list, randomly chosen, for example the first one”.

    :param output_name: the name of the KL store that will be created
    :param pnames: a list with the names of the KL stores that will be used for the output, i.e. their lists will be
                   concatenated on common keys
    :param expression: a valid python boolean expression with the following convention: within the expression, key
                       and KL stores involved should be prepended by some special symbol(s)
                       e.g. “##key <> ‘t22’ and ##age > 20”.
    """

    pipe = r.pipeline()

    keys_set = set()

    # create keys set
    # for every KLStore
    for pname in pnames:

        # find KLStore's keys
        keys = r.smembers(pname)

        # for every key in KLStore
        for key in keys:

            # tranform key to string and keep only the relevant part
            # e.g. key = store1:c22 -> key = c22
            key = byte_to_string(key).split(":")[1]

            # add key to keys_set
            keys_set.add(key)

    # create key-values dictionary
    key_values_dic = {}

    for key in keys_set:

        # empty list for adding the key values
        key_values_dic[key]=[]

    # fill key-values dictionary
    for pname in pnames:

        # find KLStore's keys
        keys = r.smembers(pname)

        # for every key in KLStore
        for key in keys:

            # tranform key to string and keep only the relevant part
            # e.g. key = store1:c22 -> key = c22
            str_key = byte_to_string(key).split(":")[1]

            # replace '##key' from the expression to key
            # e.g. '##key != c28' -> 'c25 != c28'
            transformed_expression = expression.replace('##key', '"' + str_key + '"')

            # replace the random attribute to a random element from the key's list
            # e.g. store2:c98 = [28, 78, 96, 47]
            # the expression is '##age > 20' and it is transformed to '28 > 20' because 28 was randomly choosen
            # (we use random_list_element for this purpose) from the key's list
            transformed_expression = re.sub(r'##\w+', str(random_list_element(key)), transformed_expression)

            # if the expression is True
            if eval(transformed_expression) == True:

                # empty list for adding the values of the key
                list=[]

                # for every value of the key
                for i in range(0, r.llen(key)):

                    # put the value to the list
                    list.append(byte_to_string(r.lindex(key, i)))

                # update key-values dicionary with the new values
                key_values_dic[str_key] = key_values_dic[str_key] + list

    # set for keeping only the keys that have at least 1 value
    actual_keys = set()

    # for every key in the key-values dictionary
    for key in key_values_dic:

        # if the key has at least 1 value
        if len(key_values_dic[key]) > 0:

            actual_keys.add(key)

    # for every key in the actual keys set
    for key in actual_keys:

        # add the key to the list of KLStores
        pipe.sadd(output_name, output_name + ':' + key)

    # for every key in the key-values dictionary
    for key in key_values_dic:

        # if the key has at least 1 value
        if len(key_values_dic[key]) > 0:

            # for every value of the key
            for value in key_values_dic[key]:

                # add to the Redis list the value
                pipe.lpush(output_name + ':' + key, value)

    pipe.execute()


if __name__ == '__main__':

    initializeKLStoreAdvanced('store', 3)

    # initialize Redis connection
    r = redis.Redis(host='localhost', port=6379)

    ProjSel_KLStore('agg_clients', ['store1', 'store2', 'store3'], '##key != "c32" and ##age < 50')