import argparse
import sys
from ctane_cp import main as ctane
from tane_cp import main as tane

from utils.evaluation import evaluate_FDs

def tune_parameters():
    pass


def run():
    # 1.Load clean dataset D0
    # 2.Use BART to generate dirty version dataset
    # 2.1 tune the parameters
    # 3. Use TANE/CTANE to generate rules based on D0 and dirty version dataset from 2
    # 4. do qualitative analysis
    clean_data_path = 'Database/exp_data/employee_50_egtask_clean.csv'
    dirty_data_path = 'Database/exp_data/employee_50_egtask_dirty.csv' # add 4 dirty cells [e1]
    use_func = ['TANE', 'CTANE']
    list_of_fds_dirty = tane(dirty_data_path)  # TANE with messy testdata
    list_of_fds_gt = tane(clean_data_path) # TANE with clean testdata

    print("dirty FDs: " + str(list_of_fds_dirty))
    print("ground truth FDs: " + str(list_of_fds_gt))

    accuracy, precison, recall, f1 = evaluate_FDs(list_of_fds_gt, list_of_fds_dirty)
    print("-------------------------")
    print("evaluating tane FDs...")
    print("-------------------------")
    print("accuracy: %.3f\nprecison: %.3f\nrecall: %.3f\nf1: %.3f"% \
        (accuracy, precison, recall, f1))
    print("-------------------------")

    # ctane(dirty_data_path, k=2)


if __name__ == '__main__':
    run()
