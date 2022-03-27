import sys

import matplotlib.pyplot as plt
import numpy as np
from sklearn import metrics
from sklearn.metrics import precision_recall_curve


def execute_externally_auc_pc():
    print("EXECUTING PYTHON SCRIPT")
    labels = [int(x) for x in sys.argv[1].split("_")]
    print("in_labels: " + str(labels))
    predictions = [float(x) for x in sys.argv[2].split("_")]
    print("in_predictions: " + str(predictions))

    precision, recall, thresholds = precision_recall_curve(labels, predictions)
    auc_pc = metrics.auc(recall, precision)
    print("precisions_" + "_".join([str(x) for x in precision]))
    print("recalls_" + "_".join([str(x) for x in recall]))
    print("thresholds_" +  "_".join([str(x) for x in thresholds]))
    print("aucpr_" + str(auc_pc))


def execute_randomly_auc_pr():
    num_instances = 1000
    p = 0.99
    predictions = np.random.rand(num_instances,1)
    labels = [0] * int(num_instances * p) + [1] *  int(num_instances - (num_instances * p))
    print("hi")

    precision, recall, thresholds = precision_recall_curve(labels, predictions)
    auc_pc = metrics.auc(recall, precision)
    print("precisions_" + "_".join([str(x) for x in precision]))
    print("recalls_" + "_".join([str(x) for x in recall]))
    print("thresholds_" + "_".join([str(x) for x in thresholds]))
    print("aucpr_" + str(auc_pc))
    plt.step(recall, precision, color='b', alpha=0.2, where='post')
    plt.fill_between(recall, precision, alpha=0.2, color='b')

    plt.xlabel('Recall')
    plt.ylabel('Precision')
    plt.ylim([0.0, 1.05])
    plt.xlim([0.0, 1.0])
    plt.title('2-class Precision-Recall curve: AP={0:0.2f}'.format(auc_pc))
    plt.show()

if __name__ == '__main__':
    # execute_externally_auc_pc()
    execute_randomly_auc_pr()