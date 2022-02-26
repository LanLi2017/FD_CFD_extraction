"""------------------------------------------------------------------------------------------
TANE Algorithm for discovery of exact conditional functional dependencies
Author: Nabiha Asghar, nasghar@uwaterloo.ca
March 2015
Use for research purposes only.
Please do not re-distribute without written permission from the author
Any commerical uses strictly forbidden.
Code is provided without any guarantees.
----------------------------------------------------------------------------------------------"""
from pprint import pprint

from pandas import *
from collections import defaultdict
import numpy as NP
import itertools
import sys


def replace_element_in_tuple(tup, elementindex, elementval):
    if type(elementval) == tuple:
        elementval = elementval[0]
    newtup = list(tup)
    newtup[elementindex] = elementval
    newtup = tuple(newtup)
    return newtup


def add_element_in_tuple(spxminusa, ca):
    thelist = list(spxminusa)
    thelist.append(ca[0])
    return tuple(thelist)


def validcfd(xminusa, x, a, spxminusa, sp, ca, dictpartitions):
    ept_tu = tuple()
    if xminusa is ept_tu or a is ept_tu:
        return False
    indexofa = x.index(a)
    # newsp0 = add_element_in_tuple(spxminusa, ca)
    newsp1 = replace_element_in_tuple(sp, indexofa, ca)  # this is sp, except that in place of value of a we put ca
    if (x, newsp1) in dictpartitions.keys():
        if len(dictpartitions[(xminusa, spxminusa)]) == len(dictpartitions[(x, newsp1)]):
            # and twodlen(dictpartitions[(xminusa, spxminusa)]) == twodlen(dictpartitions[(x, newsp1)]):
            return True
    return False


# def twodlen(listoflists):
#     summ = 0
#     for item in listoflists:
#         summ = summ + len(item)
#     return summ


def greaterthanorequalto(upxminusa, spxminusa):  # this is actually greaterthan or equal to
    if upxminusa == spxminusa:
        return True
    flag = True
    for index in range(0, len(upxminusa)):
        if not (spxminusa[index] == '--'):
            if not (upxminusa[index] == spxminusa[index]):
                flag = False
    return flag


def compute_dependencies(level, listofcols, dictCplus, finallistofCFDs, dictpartitions):
    for (x, sp) in level:
        for a in x:
            for (att, ca) in dictCplus[(x, sp)]:
                tmp = list(x)
                if att == (a,):
                    newtup = spXminusA(sp, x, a)
                    tmp.remove(a)
                    if not tmp:
                        del_ = ()
                    else:
                        del_ = tuple(tmp)
                    if validcfd(del_, x, a, newtup, sp, ca, dictpartitions) and not (
                            [del_, a, [newtup, ca]] in finallistofCFDs):
                        finallistofCFDs.append([del_, a, [newtup, ca]])
                        for (xx, up) in level:
                            if xx == x:
                                newtup0 = spXminusA(up, x, a)
                                if up[x.index(a)] == ca[0] and greaterthanorequalto(newtup0, newtup):
                                    if ((a,), ca) in dictCplus[(x, up)]:
                                        dictCplus[(x, up)].remove(((a,), ca))
                                    listofcolscopy = listofcols[:]
                                    for j in x:  # this loop computes R\X
                                        if j in listofcolscopy:
                                            listofcolscopy.remove(j)
                                    for b_att in listofcolscopy:  # this loop removes each b in R\X from C+(X,up)
                                        stufftobedeleted = []
                                        for (bbval, sometup) in dictCplus[(x, up)]:
                                            if (b_att,) == bbval:
                                                stufftobedeleted.append((bbval, sometup))
                                        for item in stufftobedeleted:
                                            dictCplus[(x, up)].remove(item)

    return finallistofCFDs, dictCplus


def prune(level, dictCplus):
    stufftobedeleted = []
    for (x, sp) in level:
        if len(dictCplus[(x, sp)]) == 0:
            stufftobedeleted.append((x, sp))
    for item in stufftobedeleted:
        level.remove(item)
    return level


def computeCplus(level, dictCplus):  # for each tuple (x,sp) in the list level, it computes C+(x,sp), which is a list of (attribute, value) tuples)

    for (x, sp) in level:  # sp is a tuple of strings like this: ('aa', 'bb', 'cc') or ('aa', )
        thesets = []
        for b in x:
            spcopy = spXminusA(sp, x, b)  ### tuple(y for y in sp if not sp.index(y)==indx)
            tmp = list(x)
            tmp.remove(b)
            if not tmp:
                del_ = ()
            else:
                del_ = tuple(tmp)
            # if (x.replace(b, ''), spcopy) in dictCplus.keys():
            if (del_, spcopy) in dictCplus.keys():
                # temp = dictCplus[(x.replace(b, ''), spcopy)]
                temp = dictCplus[(del_, spcopy)]
            else:
                temp = []  # is this correct???? should I put [] here?
            thesets.insert(0, set(temp))
        if not list(set.intersection(*thesets)):
            dictCplus[(x, sp)] = []
        else:
            dictCplus[(x, sp)] = list(set.intersection(*thesets))
    return dictCplus


def initial_Cplus(level, dictCplus):
    dictCplus = computeCplus(level, dictCplus)
    for (a, ca) in level:
        stufftobedeleted = []
        for (att, val) in dictCplus[(a, ca)]:
            if att == a and not val == ca:
                stufftobedeleted.append((att, val))
        for item in stufftobedeleted:
            dictCplus[(a, ca)].remove(item)
    return dictCplus


def populateL1(listofcols, k_suppthreshold, data2D, dictpartitions):
    l1 = []
    attributepartitions = computeAttributePartitions(listofcols, data2D)
    for a in listofcols:
        # tu_a = tuple([a])
        tu_a = (a,)
        l1.append((tu_a, ('--',)))
        for eqclass in attributepartitions[tu_a]:
            if len(eqclass) >= k_suppthreshold:
                l1.append((tu_a, (str(data2D.iloc[eqclass[0]][a]),)))
    dictpartitions = computeInitialPartitions(l1, attributepartitions, dictpartitions)
    # populates the dictpartitions with the initial partitions (X,sp) where X is a single attribute
    return l1, dictpartitions


def computeInitialPartitions(level1, attributepartitions, dictpartitions):
    # dictpartitions[(x,sp)] is of the form [[0,1,2]]. So simply a list of lists of indices
    for (a, sp) in level1:
        dictpartitions[(a, sp)] = []
        dictpartitions[(a, sp)] = attributepartitions[a]
    return dictpartitions


def computeAttributePartitions(listofcols, data2D):  # compute partitions for every attribute
    attributepartitions = {}
    for a in listofcols:
        tu_a = (a,)
        attributepartitions[tu_a] = []
        for element in list_duplicates(data2D[
                                           a].tolist()):  # list_duplicates returns 2-tuples, where 1st is a value, and 2nd is a list of indices where that value occurs
            if len(element[1]) > 0:  # if >1, then ignore singleton equivalence classes
                attributepartitions[tu_a].append(element[1])
    return attributepartitions


def list_duplicates(seq):
    tally = defaultdict(list)
    for i, item in enumerate(seq):
        tally[item].append(i)
    return ((key, locs) for key, locs in tally.items()
            if len(locs) > 0)


def sometuplematchesZUP(z, up, dictpartitions, k_suppthreshold):
    sumofmatches = 0
    for eqclass in dictpartitions[(z, up)]:
        sumofmatches = sumofmatches + len(eqclass)
    if sumofmatches >= k_suppthreshold:
        return True
    else:
        return False


def generate_next_level(level, tableT, dictpartitions, k):
    nextlevel = []
    for i in range(0, len(level)):  # pick an element
        for j in range(i + 1, len(level)):  # compare it to every element that comes after it.
            if ((not level[i][0] == level[j][0]) and level[i][0][0:-1] == level[j][0][0:-1] and level[i][1][0:-1] ==
                    level[j][1][0:-1]):
                z = level[i][0] + (level[j][0][-1],)
                # z = tuple(level[i][0]) + tuple(level[j][0][-1])
                up = tuple(list(level[i][1]) + [level[j][1][-1]])
                (z, up) = sortspbasedonx(z, up)
                partition_product((z, up), level[i], level[j], tableT, dictpartitions)
                if sometuplematchesZUP(z, up, dictpartitions, k):
                    flag = True
                    for att in z:
                        # indexofatt = z.index(att)  # where is att located in z
                        up_zminusa = spXminusA(up, z, att)
                        tmp = list(z)
                        tmp.remove(att)
                        if not tmp:
                            del_ = ()
                        else:
                            del_ = tuple(tmp)
                        # zminusa = z.replace(att, '')
                        zminusa = del_
                        if not ((zminusa, up_zminusa) in level):
                            flag = False
                    if flag:
                        nextlevel.append((z, up))
    return nextlevel


def spXminusA(sp, x, a):
    indexofa = x.index(a)
    mylist = []
    for i in range(0, len(sp)):
        if not i == indexofa:
            mylist.append(sp[i])
    return tuple(mylist)


def partition_product(zup, xsp, ytp, tableT, dictpartitions):
    tableS = [''] * len(tableT)
    partitionXSP = dictpartitions[xsp]
    partitionYTP = dictpartitions[ytp]
    partitionZUP = []
    print("x:%s partitionX:%s,y:%s partitionY:%s" % (xsp, partitionXSP, ytp, partitionYTP))
    for i in range(len(partitionXSP)):
        for t in partitionXSP[i]:
            tableT[t] = i
        tableS[i] = ''
    for i in range(len(partitionYTP)):
        for t in partitionYTP[i]:
            if not (tableT[t] == 'NULL'):
                tableS[tableT[t]] = sorted(list(set(tableS[tableT[t]]) | {t}))
        for t in partitionYTP[i]:
            if (not (tableT[t] == 'NULL')) and len(tableS[tableT[t]]) >= 1:
                partitionZUP.append(tableS[tableT[t]])
            if not (tableT[t] == 'NULL'):
                tableS[tableT[t]] = ''
    for i in range(len(partitionXSP)):
        for t in partitionXSP[i]:
            tableT[t] = 'NULL'
    dictpartitions[zup] = partitionZUP
    dictpartitions[zup] = partitionZUP
    print(f'zup={zup},partitionX={partitionZUP}')
    return dictpartitions


def sortspbasedonx(x, sp):
    x = list(x)
    points = zip(x, sp)
    sorted_points = sorted(points)
    new_x = [point[0] for point in sorted_points]
    new_sp = [point[1] for point in sorted_points]
    return tuple(new_x), tuple(new_sp)


# ------------------------------------------------------- START ---------------------------------------------------
# if len(sys.argv) > 1:
#     infile = str(sys.argv[1])
# if len(sys.argv) > 2:
#     k = int(sys.argv[2])
def main(infile, k=30):
    # infile = 'testdata/testdata3.csv'
    data2D = read_csv(infile)

    totaltuples = len(data2D.index)
    listofcolumns = list(data2D.columns.values)  # returns ['A', 'B', 'C', 'D', .....]
    tableT = ['NULL'] * totaltuples  # this is for the table T used in the function partition_product
    k_suppthreshold = k
    L0 = []

    dictpartitions = {}  # maps 'stringslikethis' to a list of lists, each of which contains indices
    finallistofCFDs = []
    L1, dictpartitions = populateL1(listofcolumns[:], k_suppthreshold, data2D,
                    dictpartitions)  # L1 is a list of tuples of the form [ ('A', ('val1') ), ('A', ('val2') ), ..., ('B', ('val3') ), ......]
    dictCplus = {((), ()): L1[:]}
    l = 1
    L = [L0, L1]

    while L[l]:
        if l == 1:
            dictCplus = initial_Cplus(L[l], dictCplus)
        else:
            dictCplus = computeCplus(L[l], dictCplus)
        finallistofCFDs, dictCplus = compute_dependencies(L[l], listofcolumns[:], dictCplus, finallistofCFDs,
                                                          dictpartitions)
        L[l] = prune(L[l], dictCplus)
        temp = generate_next_level(L[l], tableT, dictpartitions, k_suppthreshold)
        L.append(temp)
        l = l + 1


if __name__ == '__main__':
    # main('testdata/employee_old.csv')
    # main('testdata/employee_fullname.csv')
    # main('testdata/exp.csv')
    # main('testdata/emplcsv')
    main('database/exp_data/employee_50_egtask_clean.csv')
