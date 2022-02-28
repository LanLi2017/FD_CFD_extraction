import argparse
from cProfile import label
import sys
from itertools import product
from pprint import pprint

import numpy as np
import matplotlib.pyplot as plt

from Database.db_conn import process
from ctane_cp import main as ctane
from tane_cp import main as tane

from utils.evaluation import evaluate_FDs


def run(xml_path='BART-master/Bart_Engine/misc/resources/employees/employees-dbms-50-egtask.xml',
        clean_data_path='Database/exp_data/e1/employee_50_egtask_clean',
        dirty_data_path='Database/exp_data/e1/employee_50_egtask_dirty',
        func='tane', k=20, left_max=6, right_max=6):
    # 1.Load clean dataset D0
    # 2.Use BART to generate dirty version dataset
    # 2.1 tune the parameters
    # 3. Use TANE/CTANE to generate rules based on D0 and dirty version dataset from 2
    # 4. do qualitative analysis

    # Tune error rate
    left_error_rate = list(range(left_max))
    right_error_rate = list(range(right_max))
    error_rate_e1 = list(product(left_error_rate, right_error_rate))

    # test e1: n1 == n2 ==> d1 != d2
    for left_er, right_er in error_rate_e1:
        process(e1_er=[left_er, right_er],
                xml_path=xml_path,
                clean_db_path=f'{clean_data_path}_[{left_er,right_er}].csv',
                dirty_db_path=f'{dirty_data_path}_[{left_er, right_er}].csv')


def error_analysis(func='tane',
                   clean_data_path='Database/exp_data/e1/employee_50_egtask_clean_[(0, 0)].csv',
                   dirty_data_path='Database/exp_data/e1/employee_50_egtask_dirty_[(0, 0)].csv',
                   k=20):
    if func == 'tane':
        list_of_fds_dirty = tane(dirty_data_path)  # TANE with messy testdata
        list_of_fds_gt = tane(clean_data_path)  # TANE with clean testdata
        print("dirty FDs: ")
        pprint(list_of_fds_dirty)
        print("ground truth FDs: ")
        pprint(list_of_fds_gt)

        accuracy, precison, recall, f1, c_matches, ic_matches, miss_matches = \
            evaluate_FDs(list_of_fds_gt, list_of_fds_dirty)
        print("-------------------------")
        print("evaluating tane FDs...")
        print("-------------------------")
        print("accuracy: %.3f\nprecison: %.3f\nrecall: %.3f\nf1: %.3f" % \
              (accuracy, precison, recall, f1))
        print("-------------------------")
        print(f'The correct discovered rules: {c_matches}')
        print(f'the wrong discovered rules: {ic_matches}')
        print(f'The correct rules that are not able to be discovered: {miss_matches}')
    elif func == 'ctane':
        list_of_cfds_dirty = ctane(dirty_data_path, k)  # TANE with messy testdata
        list_of_cfds_gt = ctane(clean_data_path, k)  # TANE with clean testdata
    pass

def error_analysis_e1(search_space=[(0, 5), (0, 5)], func='tane', k=20):
    x_list, y_list = [], []
    accuracy_list, precison_list, recall_list, f1_list = [], [], [], []

    for x in range(search_space[0][0], search_space[0][1]+1):
        for y in range(search_space[1][0], search_space[1][1]+1):
            clean_data_path='Database/exp_data/e1/employee_50_egtask_clean_[(%d, %d)].csv'%(x, y)
            dirty_data_path='Database/exp_data/e1/employee_50_egtask_dirty_[(%d, %d)].csv'%(x, y)

            if func == 'tane':
                list_of_fds_dirty = tane(dirty_data_path)  # TANE with messy testdata
                list_of_fds_gt = tane(clean_data_path)  # TANE with clean testdata
                print("dirty FDs: ")
                pprint(list_of_fds_dirty)
                print("ground truth FDs: ")
                pprint(list_of_fds_gt)

                accuracy, precison, recall, f1, c_matches, ic_matches, miss_matches = \
                    evaluate_FDs(list_of_fds_gt, list_of_fds_dirty)
                
                accuracy_list.append(accuracy)
                precison_list.append(precison)
                recall_list.append(recall)
                f1_list.append(f1)
                x_list.append(x)
                y_list.append(y)

                
            elif func == 'ctane':
                list_of_cfds_dirty = ctane(dirty_data_path, k)  # TANE with messy testdata
                list_of_cfds_gt = ctane(clean_data_path, k)  # TANE with clean testdata
            pass
    
    X, Y, ACC, F1 = \
        np.array(x_list), np.array(y_list), np.array(accuracy_list), np.array(f1_list)

    # plot scatter
    ax = plt.axes(projection='3d')
    ax.scatter(X, Y, ACC, cmap='viridis', label='accuracy')
    ax.scatter(X, Y, F1, cmap='viridis', label='f1')

    ax.set_xlabel('Left Error Rate (%)')
    ax.set_ylabel('Right Error Rate (%)')
    ax.set_zlabel('FD Metrics')

    plt.legend()
    plt.show()

    # plot surface
    # ax = plt.axes(projection='3d')
    # ax.plot_surface(X.reshape(-1, 6), Y.reshape(-1, 6), ACC.reshape(-1, 6), cmap='viridis', label='accuracy')
    # ax.plot_surface(X.reshape(-1, 6), Y.reshape(-1, 6), F1.reshape(-1, 6), cmap='viridis', label='f1')
    # plt.show()



if __name__ == '__main__':
    # error_analysis()
    # run()
    error_analysis_e1()
