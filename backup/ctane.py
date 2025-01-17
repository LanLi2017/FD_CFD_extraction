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


def validcfd(xminusa, x, a, spxminusa, sp, ca):
    global dictpartitions
    print(f'in valid cfd, x is {x}')
    print(f'in valid cfd, a is {a}')
    if xminusa == '' or a == '':
        return False
    indexofa = x.index(a)
    newsp0 = add_element_in_tuple(spxminusa, ca)
    print(f'old sp: {sp}')
    print(f'ca: {ca}')
    newsp1 = replace_element_in_tuple(sp, indexofa, ca)  # this is sp, except that in place of value of a we put ca
    print(f'new sp: {newsp1}')
    print(f'in valid cfds: x minus a : {xminusa}; sp(x) minus a : {spxminusa}')
    print(f'what is dict partitions: {dictpartitions}')
    print(f'parition for x minus a: {dictpartitions[(xminusa, spxminusa)]}; '
          f'partition for x: {dictpartitions[( x,newsp1)]}')
    if (x, newsp1) in dictpartitions.keys():
        if len(dictpartitions[(xminusa, spxminusa)]) == len(dictpartitions[(
                x,
                newsp1)]):  # and twodlen(dictpartitions[(xminusa, spxminusa)]) == twodlen(dictpartitions[(x, newsp1)]):
            return True
    return False


def twodlen(listoflists):
    summ = 0
    for item in listoflists:
        summ = summ + len(item)
    return summ


def greaterthanorequalto(upxminusa, spxminusa):  # this is actually greaterthan or equal to
    if upxminusa == spxminusa:
        return True
    flag = True
    for index in range(0, len(upxminusa)):
        if not (spxminusa[index] == '--'):
            if not (upxminusa[index] == spxminusa[index]):
                flag = False
    return flag


def doublegreaterthan(upxminusa, spxminusa):
    if upxminusa == spxminusa:
        return False
    flag = True
    for index in range(0, len(upxminusa)):
        if not spxminusa[index] == '--':
            if not (upxminusa[index] == spxminusa[index]):
                flag = False
    return flag


def compute_dependencies(level, listofcols):
    global dictCplus
    global finallistofCFDs
    global listofcolumns
    print('what is dict C plus?')
    pprint(dictCplus)
    print('BUG here? What is the level')
    pprint(level)
    for (x, sp) in level:
        for a in x:
            for (att, ca) in dictCplus[(x, sp)]:
                if att == a:
                    newtup = spXminusA(sp, x,
                                       a)  ### tuple(y for y in sp if not sp.index(y)==x.index(a)) # this is sp[X\A]
                    print(f'after removing {a}: {x} ==> {x.replace(a, "")}')
                    if validcfd(x.replace(a, ''), x, a, newtup, sp, ca) and not (
                            [x.replace(a, ''), a, [newtup, ca]] in finallistofCFDs):
                        print(f"adding cfds: {[x.replace(a, ''), a, [newtup, ca]]}")
                        finallistofCFDs.append([x.replace(a, ''), a, [newtup, ca]])
                        print(f'Current level is {level}')
                        for (xx, up) in level:
                            print(f'xx is {xx}; up is {up}')
                            if xx == x:
                                newtup0 = spXminusA(up, x,
                                                    a)  ### tuple(y for y in up if not up.index(y)==x.index(a)) # this is up[X\A]
                                print(f'what is new tuple? : {newtup0}')
                                if up[x.index(a)] == ca[0] and greaterthanorequalto(newtup0, newtup):
                                    if (a, ca) in dictCplus[(x, up)]:
                                        dictCplus[(x, up)].remove((a, ca))
                                        print(f'what to remove from dict C plus first? : {(a, ca)}')
                                    listofcolscopy = listofcols[:]
                                    for j in x:  # this loop computes R\X
                                        if j in listofcolscopy:
                                            print(f'here you need to remove {j} from {listofcolscopy}')
                                            listofcolscopy.remove(j)
                                    print(f'remove columns : {listofcolscopy}')
                                    for b_att in listofcolscopy:  # this loop removes each b in R\X from C+(X,up)
                                        stufftobedeleted = []
                                        for (bbval, sometup) in dictCplus[(x, up)]:
                                            if b_att == bbval:
                                                stufftobedeleted.append((bbval, sometup))
                                        print(f'what to remove? {stufftobedeleted}')
                                        for item in stufftobedeleted:
                                            dictCplus[(x, up)].remove(item)
                                        print(f'after removing: dict C plus')
                                        pprint(dictCplus)


def prune(level):
    global dictCplus
    stufftobedeleted = []
    for (x, sp) in level:
        if len(dictCplus[(x, sp)]) == 0:
            stufftobedeleted.append((x, sp))
    for item in stufftobedeleted:
        level.remove(item)


def computeCplus(
        level):  # for each tuple (x,sp) in the list level, it computes C+(x,sp), which is a list of (attribute, value) tuples)
    global listofcolumns
    global dictCplus
    listofcols = listofcolumns[:]
    for (x, sp) in level:  # sp is a tuple of strings like this: ('aa', 'bb', 'cc') or ('aa', )
        thesets = []
        for b in x:
            indx = x.index(b)  # the index where b is located in x
            spcopy = spXminusA(sp, x, b)  ### tuple(y for y in sp if not sp.index(y)==indx)
            spcopy2 = sp[:]
            if (x.replace(b, ''), spcopy) in dictCplus.keys():
                temp = dictCplus[(x.replace(b, ''), spcopy)]
            else:
                temp = []  # is this correct???? should I put [] here?
            thesets.insert(0, set(temp))
        if not list(set.intersection(*thesets)):
            dictCplus[(x, sp)] = []
        else:
            dictCplus[(x, sp)] = list(set.intersection(*thesets))


def initial_Cplus(level):
    global listofcolumns
    global dictCplus
    print(f'in initial C plus: dict Cplus-{dictCplus}')
    computeCplus(level)
    for (a, ca) in level:
        stufftobedeleted = []
        for (att, val) in dictCplus[(a, ca)]:
            if att == a and not val == ca:
                stufftobedeleted.append((att, val))
        for item in stufftobedeleted:
            dictCplus[(a, ca)].remove(item)


def populateL1(listofcols):
    global k_suppthreshold
    l1 = []
    attributepartitions = computeAttributePartitions(listofcols)
    print(f'what is attribute partitions: {attributepartitions}')
    for a in listofcols:
        l1.append((a, ('--',)))
        for eqclass in attributepartitions[a]:

            if len(eqclass) >= k_suppthreshold:
                l1.append((a, (str(data2D.iloc[eqclass[0]][a]),)))
    computeInitialPartitions(l1,
                             attributepartitions)  # populates the dictpartitions with the initial partitions (X,sp) where X is a single attribute
    return l1


def computeInitialPartitions(level1, attributepartitions):
    global data2D
    global dictpartitions  # dictpartitions[(x,sp)] is of the form [[0,1,2]]. So simply a list of lists of indices
    for (a, sp) in level1:
        dictpartitions[(a, sp)] = []
        dictpartitions[(a, sp)] = attributepartitions[a]


def old_computeInitialPartitions(level1, attributepartitions):
    global data2D
    global dictpartitions  # dictpartitions[(x,sp)] is of the form [[0,1,2]]. So simply a list of lists of indices
    for (a, sp) in level1:
        dictpartitions[(a, sp)] = []
        if sp[0] == '--':
            dictpartitions[(a, sp)] = attributepartitions[a]
        else:
            for eqclass in attributepartitions[a]:
                if str(data2D.iloc[eqclass[0]][a]) == sp[0]:
                    dictpartitions[(a, sp)].append(eqclass)


def computeAttributePartitions(listofcols):  # compute partitions for every attribute
    global data2D
    attributepartitions = {}

    for a in listofcols:
        attributepartitions[a] = []
        for element in list_duplicates(data2D[
                                           a].tolist()):  # list_duplicates returns 2-tuples,
            print(f"for each element in list_duplicates- {list_duplicates(data2D[a].tolist())}: {element}")
            # where 1st is a value, and 2nd is a list of indices where that value occurs
            if len(element[1]) > 0:  # if >1, then ignore singleton equivalence classes
                attributepartitions[a].append(element[1])
    return attributepartitions


def list_duplicates(seq):
    tally = defaultdict(list)
    for i, item in enumerate(seq):
        tally[item].append(i)
    return ((key, locs) for key, locs in tally.items()
            if len(locs) > 0)


def sometuplematchesZUP(z, up):
    global dictpartitions
    global k_suppthreshold
    sumofmatches = 0
    for eqclass in dictpartitions[(z, up)]:
        sumofmatches = sumofmatches + len(eqclass)
    if sumofmatches >= k_suppthreshold:
        return True
    else:
        return False


def generate_next_level(level):
    nextlevel = []
    for i in range(0, len(level)):  # pick an element
        for j in range(i + 1, len(level)):  # compare it to every element that comes after it.
            if ((not level[i][0] == level[j][0]) and level[i][0][0:-1] == level[j][0][0:-1] and level[i][1][0:-1] ==
                    level[j][1][0:-1]):
                z = level[i][0] + level[j][0][-1]
                up = tuple(list(level[i][1]) + [level[j][1][-1]])
                (z, up) = sortspbasedonx(z, up)
                partition_product((z, up), level[i], level[j])
                if sometuplematchesZUP(z, up):
                    flag = True
                    for att in z:
                        indexofatt = z.index(att)  # where is att located in z
                        up_zminusa = spXminusA(up, z, att)
                        zminusa = z.replace(att, '')
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


def partition_product(zup, xsp, ytp):
    global dictpartitions
    global tableT
    tableS = [''] * len(tableT)
    partitionXSP = dictpartitions[xsp]
    partitionYTP = dictpartitions[ytp]
    partitionZUP = []
    for i in range(len(partitionXSP)):
        for t in partitionXSP[i]:
            tableT[t] = i
        tableS[i] = ''
    for i in range(len(partitionYTP)):
        for t in partitionYTP[i]:
            if not (tableT[t] == 'NULL'):
                tableS[tableT[t]] = sorted(list(set(tableS[tableT[t]]) | set([t])))
        for t in partitionYTP[i]:
            if (not (tableT[t] == 'NULL')) and len(tableS[tableT[t]]) >= 1:
                partitionZUP.append(tableS[tableT[t]])
            if not (tableT[t] == 'NULL'): tableS[tableT[t]] = ''
    for i in range(len(partitionXSP)):
        for t in partitionXSP[i]:
            tableT[t] = 'NULL'
    dictpartitions[zup] = partitionZUP
    dictpartitions[zup] = partitionZUP
    print(f'zup={zup},partitionX={partitionZUP}')


def sortspbasedonx(x, sp):
    x = list(x)
    points = zip(x, sp)
    sorted_points = sorted(points)
    new_x = [point[0] for point in sorted_points]
    new_sp = [point[1] for point in sorted_points]
    return ''.join(new_x), tuple(new_sp)


# ------------------------------------------------------- START ---------------------------------------------------
# if len(sys.argv) > 1:
#     infile = str(sys.argv[1])
# if len(sys.argv) > 2:
#     k = int(sys.argv[2])

# infile = 'testdata/testdata3.csv'
# infile = '../testdata/exp.csv'
# infile = '../Database/exp_data/employee_50_egtask_clean.csv'
infile = '../testdata/employee.csv'
data2D = read_csv(infile)
k = 30

totaltuples = len(data2D.index)
listofcolumns = list(data2D.columns.values)  # returns ['A', 'B', 'C', 'D', .....]
print(listofcolumns)
tableT = ['NULL'] * totaltuples  # this is for the table T used in the function partition_product
k_suppthreshold = k
L0 = []

dictpartitions = {}  # maps 'stringslikethis' to a list of lists, each of which contains indices
finallistofCFDs = []
L1 = populateL1(listofcolumns[
                :])  # L1 is a list of tuples of the form [ ('A', ('val1') ), ('A', ('val2') ), ..., ('B', ('val3') ), ......]
dictCplus = {('', ()): L1[:]}
print(f'initial C plus: {dictCplus}')
l = 1
L = [L0, L1]
print(f'initial level: {L}')
print(f'dict partitions are {dictpartitions}')

while not (L[l] == []):
    print(f'Level {l} is : {L[l]}')
    if l == 1:
        initial_Cplus(L[l])
    else:
        computeCplus(L[l])
    compute_dependencies(L[l], listofcolumns[:])
    print(f'after compute dependencies: dict partitions: {dictpartitions}')
    prune(L[l])
    temp = generate_next_level(L[l])
    L.append(temp)
    l = l + 1
    # print "List of all CFDs: " , finallistofCFDs
    # print "CFDs found: ", len(finallistofCFDs), ", level = ", l-1

print("List of all CFDs: ", finallistofCFDs)
pprint(finallistofCFDs)
print("Total number of CFDs found: ", len(finallistofCFDs))
