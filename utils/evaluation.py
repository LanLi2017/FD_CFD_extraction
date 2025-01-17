from logging import exception
import numpy as np


class FD(object):
    def __init__(self, input_list: list) -> None:
        # input_list: list of tuples, e.g. [('oid',), ('manager',)]
        self.set_X = set(input_list[0])
        self.set_Y = set(input_list[1])

    def __eq__(self, __o: object) -> bool:
        if hasattr(__o, 'set_X'):
            if __o.set_X == self.set_X and __o.set_Y == self.set_Y:
                return True
        return False

    def __str__(self) -> str:
        return str(self.set_X) + '--->' + str(self.set_Y)


def evaluate_FDs(y_true, y_pred):
    c_matches = [] # correctly discovered rules
    ic_matches = [] # discovered rules not in ground truth
    miss_matches = [] # missing rules [not be discovered]
    tp, fp, fn = 0, 0, 0
    FDs_pred = [FD(l) for l in y_pred]
    FDs_true = [FD(l) for l in y_true]

    for fd in FDs_pred:
        rule = fd.__str__()
        if fd in FDs_true:
            tp += 1
            c_matches.append(rule)
        else:
            fp += 1
            ic_matches.append(rule)

    for fd in FDs_true:
        rule = fd.__str__()
        if fd not in FDs_pred:
            fn += 1
            miss_matches.append(rule)

    accuracy = tp / len(FDs_true)

    precison = tp / (tp + fp)
    recall = tp / (tp + fn)

    f1 = 2 * (precison * recall / (precison + recall))

    return accuracy, precison, recall, f1, c_matches, ic_matches, miss_matches
