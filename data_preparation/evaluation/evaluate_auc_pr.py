import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn import metrics
from sklearn.metrics import precision_recall_curve

from prepare import CONF

workspace_dir = "/data/projects/data_preparation/workspace/"


def generate_auc_charts_for_ratio_experiment():
    df = pd.read_csv(workspace_dir + "dplToNDPLratios.tsv", sep="\t", index_col=None)
    for i, r in df.iterrows():
        plt.clf()
        precisions = [float(x) for x in list(r["precisions"][1:-1].split(","))]
        precision = np.array(precisions)
        recalls = [float(x) for x in list(r["recalls"][1:-1].split(","))]
        recall = np.array(recalls)
        auc_pc = metrics.auc(recall, precision)

        print("hi")

        plt.step(recall, precision, color='b', alpha=0.2, where='post')
        plt.fill_between(recall, precision, alpha=0.2, color='b')

        plt.xlabel('Recall')
        plt.ylabel('Precision')
        plt.ylim([0.0, 1.05])
        plt.xlim([0.0, 1.0])
        plt.title('2-class Precision-Recall curve: AP={0:0.2f}'.format(auc_pc))
        # plt.show()
        plt.savefig(workspace_dir + r["dataset"]  + "_" + r["classifier"] + "_aucpr_" + str(r["ratio"]) + ".pdf", bbox_inches='tight', format="pdf")


def generate_test():
    plt.clf()
    precision, recall, thresholds = precision_recall_curve(CONF["labels"], CONF["predictions"])
    # precisions = CONF["predictions"]
    # precision = np.array(precisions)
    # recalls = [float(x) for x in list(r["recalls"][1:-1].split(","))]
    # recall = np.array(recalls)
    auc_pc = metrics.auc(recall, precision)

    print("hi")

    plt.step(recall, precision, color='b', alpha=0.2, where='post')
    plt.fill_between(recall, precision, alpha=0.2, color='b')

    plt.xlabel('Recall')
    plt.ylabel('Precision')
    plt.ylim([0.0, 1.05])
    plt.xlim([0.0, 1.0])
    plt.title('2-class Precision-Recall curve: AP={0:0.2f}'.format(auc_pc))
    plt.show()


if __name__ == '__main__':
    # calculate_auc_pr()
    # generate_auc_charts_for_ratio_experiment()
    generate_test()