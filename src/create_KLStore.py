import redis
import os
import json
import csv
from xlrd import open_workbook
import mysql.connector

FILE_PATH = os.path.abspath(os.path.dirname(__file__))


def create_KLStore_from_csv(r, klStore_name, filename, path, delimeter, position1, position2):
    """
    This function retrieve data from a csv file and creates a Key-list store in redis based on these data

    :param r: redis connection
    :param filename: name of the csv file where data are
    :param path: path of csv file
    :param delimeter: delimeter that is used in csv file
    :param position1: column where key is stored in csv file
    :param position2: column where value is stored in csv file
    """

    with open(os.path.join(FILE_PATH, path + filename)) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=delimeter)

        pipe = r.pipeline()
        while True:
            try:
                for row in csv_reader:
                    pipe.rpush(row[position1], row[position2])
                    pipe.sadd(klStore_name, row[position1])

                pipe.execute()
                break
            except Exception:
                continue


def create_KLStore_from_excel(r, klStore_name, filename, path, query_string, position1, position2):
    """
    This function retrieve data from an excel file and creates a Key-list store in redis based on these data

    :param r: redis connection
    :param filename: name of the excel file where data are
    :param path: path of excel file
    :param query_string: excel sheet where data are stored in excel file
    :param position1: column where key is stored in excel file
    :param position2: column where value is stored in excel file
    """

    reader = open_workbook(os.path.join(FILE_PATH, path + filename), on_demand=True )
    sheet = reader.sheet_by_name(query_string)

    for key, value in zip(sheet.col(position1), sheet.col(position2)):
        r.rpush(key.value, value.value)
        r.sadd(klStore_name, key.value.encode("utf-8"))


def create_KLStore_from_db(r, klStore_name, host, user, pwd, database, query_string, direction):
    """
    This function retrieve data from an relational database and creates a Key-list store in redis based on these data

    :param r: redis connection
    :param host: host of database connection where data are stored
    :param user: user of database connection where data are stored
    :param pwd: pwd database connection where data are stored
    :param database: database name database connection where data are stored
    :param query_string: an SQL statement in the form SELECT col1, col2 WHERE <etc>.
    :param direction: the value 1 or 2, specifying whether KL1(D) or KL2(D) should be implemented
    """
    mydb = mysql.connector.connect(
        host=host,
        user=user,
        passwd=pwd,
        database=database
    )

    my_cursor = mydb.cursor()
    my_cursor.execute(query_string)
    my_result = my_cursor.fetchall()

    pipe = r.pipeline()
    while True:
        try:
            for x in my_result:
                # if direction is equal to 1, key is in position 0 of returned array and
                # value in position 1 (direction%2)
                # if direction is equal to 2, key is in position 1 of returned array and
                # value in position 0 (direction%2)
                pipe.rpush(x[direction - 1], x[direction % 2])
                pipe.sadd(klStore_name, x[direction - 1])

            pipe.execute()
            break
        except Exception:
            continue


def get_datasource(name, data_source, query_string, position1, position2, direction):
    """
    This function

    :param name: name of the datasource
    :param data_source: json file where all information about where data are stored exist
    :param query_string: name of excel sheet or an SQL statement
    :param position1: column where key is stored in excel file
    :param position2: column where value is stored in excel file
    :param direction: the value 1 or 2, specifying whether KL1(D) or KL2(D) should be implemented
    """

    try:

        # The decode_repsonses flag here directs the client to convert the responses from Redis into Python strings
        # using the default encoding utf-8.  This is client specific.
        r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

        with open(os.path.join(FILE_PATH, data_source)) as data_source_file:
            datasources = json.load(data_source_file)['datasources']

            if name not in datasources:
                raise ValueError("No name in given datasources")

            data = datasources[name]

            if data["type"] == "csv":
                create_KLStore_from_csv(r, name, data["filename"], data["path"], data["delimiter"], position1,
                                        position2)

            if data["type"] == "excel":
                create_KLStore_from_excel(r, name, data["filename"], data["path"], query_string, position1, position2)

            if data["type"] == "db":
                create_KLStore_from_db(r, name, data['dbconnect']['host'], data['dbconnect']['username'],
                                       data['dbconnect']['password'], data['dbconnect']['database'], query_string,
                                       direction)

    except Exception as e:
        raise Exception(e)


if __name__ == '__main__':
    # csv file:
    # get_datasource ( "Sales_csv", "datasource.json", "", 0, 1, "" )
    get_datasource("Sales2_csv", "datasource.json", "", 2, 1, "")

    # excel file:
    get_datasource("Sales_excel", "datasource.json", "transactions", 0, 1, "")

    # relational db:
    # get_datasource("Sales_db", "datasource.json", "SELECT trans_id, cust_id FROM redis_bigData_db.Sales "
    #                                               "WHERE cust_id = 'c1';", "", "", 2)
