import csv
import os
from glob import glob

import pandas as pd

from dpd_utils.utils import _get_execution_default_parameters


def export_classification_stats_to_table(classification_stats_total: list, fpath):
    fieldnames = set(classification_stats_total[0].keys())

    with open(fpath, "wt") as fout:
        csvout = csv.DictWriter(fout, fieldnames=sorted(fieldnames), delimiter="\t")
        csvout.writeheader()
        for r in classification_stats_total:
            csvout.writerow(r)


def import_all_classification_stats(dirpath):
    classifiers_stats = {}
    for fpath in set(glob(dirpath + "*")) - set(
            glob(dirpath + "*pairs*") + glob(dirpath + "*.pdf") + glob(dirpath + "*search_space*")):
        classifier = os.path.basename(fpath)[:-4].split("-")[1]

        classification = pd.read_csv(index_col=0, filepath_or_buffer=fpath, sep="\t")  # index_col=None)
        classifiers_stats[classifier] = classification
    return classifiers_stats


def classification_calculate_pr_curve_auc(classifier, dataset, preparations, attributes, xstandard,
                                          is_chain_preparators, classification_style, CONF):
    jar_path = CONF["dataprepdedup_jar"]
    main_class = "deduplication.classification.ClassificationController"
    java_jar_exec_base = "java " + CONF["java_vm_parameters"] + " -cp " + jar_path + " " + main_class

    parameters = _get_execution_default_parameters(dataset, preparations, attributes, xstandard, is_chain_preparators,
                                                   CONF)

    parameters.append(("classifier", classifier))
    parameters.append(("classification_style", classification_style))

    cmd = java_jar_exec_base + " " + " ".join(v[0] + "==" + v[1] for v in parameters)
    print(cmd)
    os.system(cmd)
