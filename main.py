import csv
import os
import re
import timeit
from pprint import pprint

import pymysql
from pyhocon import ConfigFactory

from ctane_cp import main as ctane
from tane_cp import main as tane
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
CONF = ConfigFactory.parse_file(ROOT_DIR + '/data_preparation/resources/reference.conf')


def export_prepared_data():
    mydb = pymysql.connect(host=CONF["database_ip"], user=CONF["database_username"],
                           password=CONF["database_password"],
                           charset='utf8',  # charset='utf8mb4',
                           cursorclass=pymysql.cursors.DictCursor)
    mycursor = mydb.cursor()
    mycursor.execute("USE dataprepdedup;")
    mycursor.execute("SHOW TABLES;")
    mycursor.execute("select * from dataprepdedup.classification;")
    res = []
    for row in mycursor:
        print(row)
        # if row['dataset'] == 'restaurant':
        #     res.append(row)
    mycursor.close()
    mydb.close()
    return res


def convert_tsv2csv(old_ds, new_ds):
    with open(old_ds, 'r')as tsvfile:
        with open(new_ds, 'w')as csvfile:
            for line in tsvfile:
                filecontent = re.sub("\t", ",", line)
                csvfile.write(filecontent)


def main():
    res = export_prepared_data()
    # for row in res:
    #     print(row['dataset'])
    old_ds = 'data_preparation/data/restaurants.tsv'
    new_ds = 'testdata/restaurants.csv'
    # convert_tsv2csv(old_ds, new_ds)

    # k = 20
    # start = timeit.default_timer()
    # list_of_cfds_old = ctane('testdata/restaurants_small.csv', k)  # TANE with messy testdata
    # stop = timeit.default_timer()
    # print('Before the data preparation:')
    # print(f'CTANE running time: {stop - start}')
    # print(f'The number of conditional functional dependency: {len(list_of_cfds_old)}')
    # pprint(list_of_cfds_old)
    #
    # list_fd = tane('testdata/restaurants_small.csv')
    # pprint(list_fd)

    # list_of_cfds_gt = ctane(clean_data_path, k)


if __name__ == '__main__':
    main()
