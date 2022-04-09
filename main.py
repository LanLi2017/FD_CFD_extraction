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
from utils.evaluation import evaluate_FDs
import matplotlib.pyplot as plt

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


def evaluation(list_of_fds_gt, list_of_fds_dirty, fwrite):
    accuracy, precision, recall, f1, c_matches, ic_matches, miss_matches = \
        evaluate_FDs(list_of_fds_gt, list_of_fds_dirty)
    print("-------------------------")
    print("evaluating tane FDs...")
    print("-------------------------")
    print("accuracy: %.3f\nprecision: %.3f\nrecall: %.3f\nf1: %.3f" % \
          (accuracy, precision, recall, f1))
    print("-------------------------")
    print(f'The correct discovered rules: {c_matches}')
    print(f'the wrong discovered rules: {ic_matches}')
    print(f'missing rules: {miss_matches}')

    fwrite.write("------------------------- \n")
    fwrite.write("evaluating tane FDs... \n")
    fwrite.write("accuracy: %.3f\nprecision: %.3f\nrecall: %.3f\nf1: %.3f \n" % \
                 (accuracy, precision, recall, f1))
    fwrite.write("------------------------- \n")
    fwrite.write(f'The correct discovered rules: {c_matches} \n')
    fwrite.write(f'the wrong discovered rules: {ic_matches} \n')
    fwrite.write(f'missing rules: {miss_matches}\n')

    return accuracy, precision, recall, f1


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


def run():
    ds = 'census'
    df = pd.read_csv(f'data_preparation/data/{ds}.tsv', sep='\t')
    # Export prepared data
    prepared_path = f'testdata/{ds}_prepared_data.csv'
    export_prepared_data(df, prepared_path, ds)  # no need to re-run once created


def main1():
    import numpy as np
    list_of_fd_gt = [[("id",), ("phone",)],
                     [("id",), ("merged_values",)],
                     [("id",), ("type",)],
                     [("id",), ("name",)],
                     [("id",), ("city",)],
                     [("id",), ("address",)],
                     [("merged_values",), ("id",)],
                     [("merged_values",), ("phone",)],
                     [("merged_values",), ("type",)],
                     [("merged_values",), ("name",)],
                     [("merged_values",), ("city",)],
                     [("merged_values",), ("address",)],
                     [("address", "city"), ("phone",)],
                     [("address", "city"), ("name",)],
                     [("address", "city"), ("type",)]
                     ]
    # this is to generate thresholds list
    # ori_ds = 'testdata/restaurants.csv'
    # list_fd_old = tane(ori_ds, thres=0.0)
    # prepared_path = f'testdata/restaurants_prepared_data.csv'
    # list_fd_prep = tane(prepared_path, thres=0.0)
    er_tane = list(np.linspace(0, 0.18, num=6))
    with open('log.log', 'a+')as fp:
        for er in er_tane:
            fp.write(f'Current threshold is {er} \n')
            ori_ds = 'testdata/restaurants.csv'
            list_fd_old = tane(ori_ds, thres=er)
            prepared_path = f'testdata/restaurants_prepared_data.csv'
            list_fd_prep = tane(prepared_path, thres=er)
            fp.write('Before preparing the data, evaluate the rules prediction: \n')
            evaluation(list_of_fd_gt, list_fd_old, fp)
            fp.write('After preparing the data, evaluate the rules prediction: \n')
            evaluation(list_of_fd_gt, list_fd_prep, fp)


def main():
    er_tane = [0.0]
    for i in range(1, 21):
        er_tane.append(er_tane[i - 1] + 0.9 / 20)
    print(er_tane)
    # er_tane = list(np.linspace(0, 0.875, num=6))
    # list_of_fd_gt = CONF['rules_gt_restaurant']
    list_of_fd_gt = [[("id",), ("phone",)],
                     [("id",), ("merged_values",)],
                     [("id",), ("type",)],
                     [("id",), ("name",)],
                     [("id",), ("city",)],
                     [("id",), ("address",)],
                     [("merged_values",), ("id",)],
                     [("merged_values",), ("phone",)],
                     [("merged_values",), ("type",)],
                     [("merged_values",), ("name",)],
                     [("merged_values",), ("city",)],
                     [("merged_values",), ("address",)],
                     [("address", "city"), ("phone",)],
                     [("address", "city"), ("name",)],
                     [("address", "city"), ("type",)]
                     ]

    old_ds = 'data_preparation/data/restaurants.tsv'
    new_ds = 'testdata/restaurants.csv'
    # convert_tsv2csv(old_ds, new_ds)

    # k = 20
    # start_old_ds = timeit.default_timer()
    # list_of_cfds_old = ctane(new_ds, k)  # TANE with messy testdata
    # stop_old_ds = timeit.default_timer()
    # print('Before the data preparation:')
    # print(f'CTANE running time: {stop_old_ds - start_old_ds}')
    # print(f'The number of conditional functional dependency: {len(list_of_cfds_old)}')
    # pprint(list_of_cfds_old)

    ds = 'restaurants'
    df = pd.read_csv(f'testdata/{ds}.csv')

    # Export prepared data
    prepared_path = f'testdata/{ds}_prepared_data.csv'
    # export_prepared_data(df, prepared_path, ds)  # no need to re-run once created

    # start_new_ds = timeit.default_timer()
    # list_of_cfds_prep = ctane(prepared_path, k)
    # stop_new_ds = timeit.default_timer()
    # print('After the data preparation:')
    # print(f'CTANE running time: {stop_new_ds - start_new_ds}')
    # print(f'The number of conditional functional dependency: {len(list_of_cfds_prep)}')
    # pprint(list_of_cfds_prep)

    # FD
    old_acc_list = []
    old_prec_list = []
    old_recall_list = []
    old_f1_list = []
    new_acc_list = []
    new_prec_list = []
    new_recall_list = []
    new_f1_list = []
    with open('log.log', 'a+') as fwrite:
        for er in er_tane:
            fwrite.write(f'Current threshold for TANE is : {er} \n')
            fwrite.write(f'The number of ground truth for functional dependencies: {len(list_of_fd_gt)} \n')
            list_fd_old = tane(new_ds, er)
            print(len(list_fd_old))
            fwrite.write(f'The number of functional dependencies based on original data: {len(list_fd_old)} \n')
            list_fd_prep = tane(prepared_path, er)
            print(len(list_fd_prep))
            fwrite.write(f'The number of functional dependencies based on prepared data: {len(list_fd_prep)} \n')

            print("dirty FDs: ")
            pprint(list_fd_old)
            print("ground truth FDs: ")
            pprint(list_of_fd_gt)
            print('FDs for prepared data:')
            pprint(list_fd_prep)
            fwrite.write(f'The ground truth for the rules: {list_of_fd_gt} \n')
            fwrite.write(f'Original Dataset: The discovered FDs: {list_fd_old} \n')
            fwrite.write(f'Prepared Dataset: The discovered FDs: {list_fd_prep} \n')

            fwrite.write('The evaluation results: \n')
            fwrite.write('Before preparing the data, evaluate the rules prediction: \n')
            old_acc, old_prec, old_recall, old_f1 = evaluation(list_of_fd_gt, list_fd_old, fwrite)
            old_acc_list.append(old_acc)
            old_prec_list.append(old_prec)
            old_recall_list.append(old_recall)
            old_f1_list.append(old_f1)
            fwrite.write('After preparing the data, evaluate the rules prediction: \n')
            new_acc, new_prec, new_recall, new_f1 = evaluation(list_of_fd_gt, list_fd_prep, fwrite)
            new_acc_list.append(new_acc)
            new_prec_list.append(new_prec)
            new_recall_list.append(new_recall)
            new_f1_list.append(new_f1)
    plt.figure()
    plt.subplot(221)
    plt.plot(er_tane, old_acc_list, 'r', er_tane, new_acc_list, 'g')
    plt.xlabel('threshold for TANE')
    plt.ylabel('accuracy')

    plt.subplot(222)
    plt.plot(er_tane, old_recall_list, 'r', er_tane, new_recall_list, 'g')
    plt.xlabel('threshold for TANE')
    plt.ylabel('recall')

    plt.subplot(223)
    plt.plot(er_tane, old_prec_list, 'r', er_tane, new_prec_list, 'g')
    plt.xlabel('threshold for TANE')
    plt.ylabel('precision')

    plt.subplot(224)
    plt.plot(er_tane, old_f1_list, 'r', er_tane, new_f1_list, 'g')
    plt.xlabel('threshold for TANE')
    plt.ylabel('f1')
    # plt.show()
    plt.savefig('result.png')


if __name__ == '__main__':
    # main1()
    # run()
    main()
