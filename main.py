import csv
import os
import re
import timeit
from pprint import pprint

import pymysql
import sqlalchemy
from pyhocon import ConfigFactory
import pandas as pd
from tqdm import tqdm

from ctane_cp import main as ctane
from tane_cp import main as tane

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
CONF = ConfigFactory.parse_file(ROOT_DIR + '/data_preparation/resources/reference.conf')


# def export_prepared_data():
#     mydb = pymysql.connect(host=CONF["database_ip"], user=CONF["database_username"],
#                            password=CONF["database_password"],
#                            charset='utf8',  # charset='utf8mb4',
#                            cursorclass=pymysql.cursors.DictCursor)
#     mycursor = mydb.cursor()
#     mycursor.execute("USE dataprepdedup;")
#     mycursor.execute("SHOW TABLES;")
#     mycursor.execute("select * from dataprepdedup.classification;")
#     res = []
#     for row in mycursor:
#         print(row)
#         # if row['dataset'] == 'restaurant':
#         #     res.append(row)
#     mycursor.close()
#     mydb.close()
#     return res


def export_prepared_data(df_original, path, ds):
    """
    Exports the prepared data to a csv file.

    :param df: DataFrame, original data
    :param path: String
    :return: None
    """
    db_connection = sqlalchemy.create_engine('mysql+pymysql://{0}:{1}@{2}/{3}'
                                             .format(CONF["database_username"],
                                                     CONF["database_password"],
                                                     CONF["database_ip"] + ":" + CONF["database_connection_port"],
                                                     CONF["database_schemaname"]))
    sql_query = """
                select * from preparation_data_cube
                where `dataset` = '%s' and `xstandard` = 'goldstandard' 
                """ % ds
    df_cube = pd.read_sql_query(sql_query, db_connection)

    for i, row in tqdm(df_cube.iterrows(), total=len(df_cube)):
        df_original.loc[df_original['id'] == int(row['record_id']), row['attribute']] = row['value']

    df_original.to_csv(path, index=False)


def convert_tsv2csv(old_ds, new_ds):
    with open(old_ds, 'r')as tsvfile:
        with open(new_ds, 'w')as csvfile:
            for line in tsvfile:
                filecontent = re.sub("\t", ",", line)
                csvfile.write(filecontent)


def main():

    old_ds = 'data_preparation/data/restaurants.tsv'
    new_ds = 'testdata/restaurants.csv'
    # convert_tsv2csv(old_ds, new_ds)

    k = 20
    start_old_ds = timeit.default_timer()
    list_of_cfds_old = ctane(new_ds, k)  # TANE with messy testdata
    stop_old_ds = timeit.default_timer()
    print('Before the data preparation:')
    print(f'CTANE running time: {stop_old_ds - start_old_ds}')
    print(f'The number of conditional functional dependency: {len(list_of_cfds_old)}')
    pprint(list_of_cfds_old)

    ds = 'restaurants'
    df = pd.read_csv(f'testdata/{ds}.csv')

    # Export prepared data
    prepared_path = f'testdata/{ds}_prepared_data.csv'
    # export_prepared_data(df, prepared_path, ds)  # no need to re-run once created
    start_new_ds = timeit.default_timer()
    list_of_cfds_prep = ctane(prepared_path, k)
    stop_new_ds = timeit.default_timer()
    print('After the data preparation:')
    print(f'CTANE running time: {stop_new_ds - start_new_ds}')
    print(f'The number of conditional functional dependency: {len(list_of_cfds_prep)}')
    pprint(list_of_cfds_prep)

    # FD
    list_fd_old = tane(new_ds)
    pprint(list_fd_old)
    print(len(list_fd_old))

    list_fd_prep = tane(prepared_path)
    pprint(list_fd_prep)
    print(len(list_fd_prep))


if __name__ == '__main__':
    main()
