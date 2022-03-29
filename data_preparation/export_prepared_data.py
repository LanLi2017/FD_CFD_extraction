import os

import pandas as pd
import numpy as np
from tqdm import tqdm

import sqlalchemy
from pyhocon import ConfigFactory

DATASET = 'restaurants'

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
CONF = ConfigFactory.parse_file(ROOT_DIR + '/resources/reference_YL.conf')

db_connection = sqlalchemy.create_engine('mysql+pymysql://{0}:{1}@{2}/{3}'
                                        .format(CONF["database_username"],
                                                CONF["database_password"],
                                                CONF["database_ip"] + ":" + CONF["database_connection_port"],
                                                CONF["database_schemaname"]))

def export_prepared_data(df_original, path):
    """
    Exports the prepared data to a csv file.

    :param df: DataFrame, original data
    :param path: String
    :return: None
    """
    sql_query = """
                select * from preparation_data_cube
                where `dataset` = '%s' and `xstandard` = 'goldstandard' 
                """ % DATASET
    df_cube = pd.read_sql_query(sql_query, db_connection)


    for i, row in tqdm(df_cube.iterrows(), total=len(df_cube)):
        df_original.loc[df_original['id'] == int(row['record_id']), row['attribute']] = row['value']

    df_original.to_csv(path, index=False, sep='\t')



if __name__ == '__main__':
    # Load data
    df = pd.read_csv('data/%s.tsv'%DATASET, sep='\t')

    # Export data
    if not os.path.isdir('output'):
        os.mkdir('output')
    export_prepared_data(df, './output/prepared_data.tsv')
