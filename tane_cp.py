"""------------------------------------------------------------------------------------------
TANE Algorithm for discovery of exact functional dependencies
Author: Nabiha Asghar, nasghar@uwaterloo.ca
February 2015
Use for research purposes only.
Please do not re-distribute without written permission from the author
Any commerical uses strictly forbidden.
Code is provided without any guarantees.
----------------------------------------------------------------------------------------------"""
from pprint import pprint

from pandas import *
from collections import defaultdict
import numpy as NP
import sys


def list_duplicates(seq):
    tally = defaultdict(list)
    for i, item in enumerate(seq):  # 这两行核心代码实现了合并相同元组： 等价类的划分
        tally[item].append(i)  # 只有item一样才能实现合并
    return ((key, locs) for key, locs in tally.items()
            if len(locs) > 0)


def findCplus(x, dictCplus):  # this computes the Cplus of x as an intersection of smaller Cplus sets
    '''

    :param x: tuple: ('a', 'b', ...)
    :return:
    '''
    thesets = []
    list_x = list(x)
    for a in x:
        list_x.remove(a)
        if not list_x:
            del_x = ()
        else:
            del_x = tuple(list_x)
        # 计算的过程涉及了递归
        if del_x in dictCplus:
            # print(f'{del_x} in {dictCplus.keys()}')
            temp = dictCplus[del_x]
        else:
            temp = findCplus(del_x, dictCplus)  # compute C+(X\{A}) for each A at a time
        # dictCplus[x.replace(a,'')] = temp
        thesets.insert(0, set(temp))
    if not list(set.intersection(*thesets)):
        cplus = []
    else:
        cplus = list(set.intersection(*thesets))  # compute the intersection in line 2 of pseudocode
    return cplus


def compute_dependencies(level, listofcols, dictCplus, finallistofFDs, totaltuples, dictpartitions):
    '''

    :param dictpartitions:
    :param totaltuples:
    :param finallistofFDs: final FD list
    :param dictCplus: RHS
    :param level: list of tuples [('a'), ('b'), ...]
    :param listofcols: list of tuples [('a'), ('b'), ...]
    :return:
    '''
    # 属性-右方集dict
    # FD list
    # global listofcolumns  # 属性集list
    # FUN1: 计算所有X属于Li的右方集Cplus
    # 通过上层结点{A}计算当前层的每个X的Cplus(X)
    # 或者通过computeCplus
    for x in level:
        thesets = []
        for a in x:
            list_x = list(x)
            list_x.remove(a)
            del_x = tuple(list_x)
            if del_x in dictCplus:
                # if x.replace(a, '') in dictCplus.keys():  # 如果Cplus(X\A) 已经在当前右方集List中
                temp = dictCplus[del_x]  # temp存入的是Cplus(X\A) -- 即X\A的右集合
            else:  # 否则，计算右方集
                temp = computeCplus(del_x, listofcols, totaltuples,
                                    dictpartitions)  # compute C+(X\{A}) for each A at a time
                dictCplus[del_x] = temp  # 存入dictCplus中
            thesets.insert(0, set(temp))  # 通过set, 将temp转化成集合，再将该对象插入到列表的第0个位置
        res = set.intersection(*thesets)
        if not list(res):  # set.intersection(set1, set2, ...ect)求并集
            dictCplus[tuple(x)] = []
        else:
            dictCplus[tuple(x)] = list(res)  # compute the intersection in line 2 of pseudocode

    # Fun2: 找到最小函数依赖
    # 并对Cplus进行剪枝（最小性剪枝）： 1.删掉已经成立的。 2去掉必不可能的 留下的是"仍有希望的"
    for x in level:
        for a in x:
            list_x = list(x)
            tu_a = (a,)
            if tu_a in dictCplus[x]:
                list_x.remove(a)
                del_x = tuple(list_x)
                if validfd(del_x, tu_a, totaltuples, dictpartitions):  # line 5 即x\{A} -> A 函数依赖成立
                    finallistofFDs.append([del_x, tu_a])  # line 6
                    dictCplus[x].remove(tu_a)  # line 7
                    listofcols = listofcols[:]  # 为了下面的剪枝作准备
                    for j in x:  # this loop computes R\X
                        tu_j = (j,)
                        if tu_j in listofcols:
                            listofcols.remove(tu_j)
                    for b in listofcols:  # this loop removes each b in R\X from C+(X)
                        # 在C+(X) 删掉所有属于R\X 即不属于X的元素， 即留下的Cplus元素全部属于X
                        if b in dictCplus[x]:
                            dictCplus[x].remove(b)

    return dictCplus, finallistofFDs, dictpartitions


def computeCplus(x, listofcolumns, totaltuples, dictpartitions):
    # this computes the Cplus from the first definition in section 3.2.2 of TANE paper.
    # output should be a list of single attributes
    listofcols = listofcolumns[:]
    if x == '':
        return listofcols  # because C+{phi} = R
    cplus = []
    for a in listofcols:  # A属于R并满足如下条件
        del_a = listofcols
        for b in x:
            del_a.remove(a)
            del_a.remove(b)
            tmp = del_a
            # temp = x.replace(a, '')
            # temp = temp.replace(b, '')
            if not validfd(tmp, b, totaltuples, dictpartitions):
                cplus.append(a)
    return cplus


def validfd(y, z, totaltuples, dictpartitions):
    ept_tu = tuple()
    if y == ept_tu or z == ept_tu:
        return False
    ey = computeE(y, totaltuples, dictpartitions)
    eyz = computeE(y + z, totaltuples, dictpartitions)
    if ey == eyz:
        return True
    else:
        return False


def computeE(x, totaltuples, dictpartitions):  # 属性集为x
    '''

    :param x:
    :param totaltuples:
    :param dictpartitions:
    :return:
    '''
    # 元组数
    # 关于每个属性集的剥离分区
    doublenorm = 0
    sort_x = tuple(sorted(x))
    for i in dictpartitions[sort_x]:
        doublenorm = doublenorm + len(i)
    e = (doublenorm - len(dictpartitions[sort_x])) / float(totaltuples)
    return e


def check_superkey(x, dictpartitions):
    if (dictpartitions[x] == [[]]) or (dictpartitions[x] == []):  # 如果剥离分区为空， 则说明pi_x 只有单例等价类组成
        return True
    else:
        return False


def prune(level, dictCplus, finallistofFDs, dictpartitions):
    # stufftobedeletedfromlevel = []
    for x in level:  # line 1
        '''Angle 1: 右方集修建'''
        if not dictCplus[x]:  # line 2
            level.remove(x)  # line 3
        '''Angle 2: 键修建'''
        if check_superkey(tuple(sorted(x)),
                          dictpartitions):  # line 4   ### should this check for a key, instead of super key??? Not sure.
            temp = dictCplus[x][:]  # 初始化temp 为computes cplus(X)

            # 求C+(X)\X
            for i in list(x):  # this loop computes C+(X) \ X
                # tu_i = tuple(list(i))
                if (i,) in temp:
                    temp.remove((i,))
            for a in temp:  # line 5: for each a 属于 Cplus(X)\X do
                thesets = []
                # 计算Cplus((X+A)\ {B})
                for b in x:
                    new_tu_ = x + a
                    list_new_tu = list(new_tu_)
                    # new_tu = tuple(x for x in new_tu if x != b)
                    list_new_tu.remove(b)
                    new_tu = tuple(sorted(list_new_tu))
                    if not (new_tu in dictCplus.keys()):
                        # ''.join(sorted((x+a).replace(b,''))表示的就是XU{a}\{b}
                        dictCplus[new_tu] = findCplus(new_tu, dictCplus)
                    thesets.insert(0, set(dictCplus[new_tu]))
                # 4. 计算Cplus((X+A)\ {B})交集，判断a是否在其中
                if a in list(set.intersection(*thesets)):  # line 6
                    finallistofFDs.append([x, a])  # line 7
                # print "adding key FD: ", [x,a]
            if x in level:
                level.remove(x)  # 只要x是超键， 就要剪掉x
                # stufftobedeletedfromlevel.append(x)  # line 8
    return dictCplus, finallistofFDs, level


def generate_next_level(level, dictpartitions, tableT):
    # 首先令 L[i+1] 这一层为空集
    nextlevel = []
    for i in range(0, len(level)):  # pick an element
        for j in range(i + 1, len(level)):  # compare it to every element that comes after it.
            # 如果这两个元素属于同一个前缀块，那么就可以合并:只有最后一个属性不同，其余都相同

            if (not level[i] == level[j]) and level[i][0:-1] == level[j][0:-1]:  # i.e. line 2 and 3
                # print(set(level[i] + level[j][-1]))
                x = tuple(sorted(set(level[i] + (level[j][-1],))))
                # x = tuple([level[i]]) + tuple([level[j][-1]])  # line 4
                flag = True
                for a in x:  # this entire for loop is for the 'for all' check in line 5
                    new_tu = tuple(t for t in x if t != a)
                    if not (new_tu in level):
                        flag = False
                if flag:
                    nextlevel.append(x)
                    # 计算新的属性集X上的剥离分区
                    # =pi_y*pi_z（其中y为level[i]，z为level[j]）
                    dictpartitions = stripped_product(x, level[i], level[
                        j], dictpartitions,
                                     tableT)  # compute partition of x as pi_y * pi_z (where y is level[i] and z is level[j])
    return nextlevel, dictpartitions


def stripped_product(x, y, z, dictpartitions, tableT):
    # dictpartitions: 剥离分区
    tableS = [''] * len(tableT)
    # partitionY、partitionZ是属性集Y、Z上的剥离分区，已知！
    # partitionY is a list of lists, each list is an equivalence class
    partitionY = dictpartitions[tuple(sorted(y))]
    partitionZ = dictpartitions[tuple(sorted(z))]
    partitionofx = []  # line 1
    for i in range(len(partitionY)):  # line 2
        for t in partitionY[i]:  # line 3
            tableT[t] = i
        tableS[i] = ''  # line 4
    for i in range(len(partitionZ)):  # line 5
        for t in partitionZ[i]:  # line 6
            if not (tableT[t] == 'NULL'):  # line 7
                tableS[tableT[t]] = sorted(list(set(tableS[tableT[t]]) | {t}))
        for t in partitionZ[i]:  # line 8
            if (not (tableT[t] == 'NULL')) and len(tableS[tableT[t]]) >= 2:  # line 9
                partitionofx.append(tableS[tableT[t]])
            if not (tableT[t] == 'NULL'):
                tableS[tableT[t]] = ''  # line 10
    for i in range(len(partitionY)):  # line 11
        for t in partitionY[i]:  # line 12
            tableT[t] = 'NULL'
    dictpartitions[tuple(sorted(x))] = partitionofx  # 生成属性集X上的剥离分区
    return dictpartitions


# ------------------------------------------------------- START ---------------------------------------------------

# if len(sys.argv) > 1:
#     infile = str(sys.argv[1])  # this would be e.g. "testdata.csv"

# 测试computeSingletonPartitions
# 此时考虑的属性集只有A,B,C,D，在单个属性集上面生成剥离分区
'''测试list_duplicates函数的返回值:返回的是每个属性列表中每个属性的剥离分区'''


def main(file):
    data2D = read_csv(file)

    totaltuples = len(data2D.index)
    columns = list(x for x in data2D.columns.values)
    listofcolumns = list(tuple([x]) for x in columns)  # returns ['A', 'B', 'C', 'D', .....]
    dictpartitions = {}
    dictCplus = {tuple(): listofcolumns}
    for col in columns:  # 为索引列
        col_tu = tuple([col])
        dictpartitions[col_tu] = []
        # for element in list_duplicates(data2D[a].tolist()):
        #     print("element=", element)
        for element in list_duplicates(data2D[
                                           col].tolist()):  # list_duplicates returns 2-tuples, where 1st is a value, and 2nd is a list of indices where that value occurs
            if len(element[1]) > 1:  # ignore singleton equivalence classes
                dictpartitions[col_tu].append(element[1])

    finallistofFDs = []
    # 初始时，L1层包含的属性集为：A,B,C,D...

    L1 = [x for x in listofcolumns]  # L1: [('A',), ('B',),...]
    i = 1
    L0 = []
    L = [L0, L1]
    tableT = ['NULL'] * totaltuples  # this is for the table T used in the function stripped_product
    while L[i]:  # 第i层的包含的属性集不为空
        dictCplus, finallistofFDs, dictpartitions = \
            compute_dependencies(L[i], listofcolumns[:], dictCplus, finallistofFDs, totaltuples,
                                 dictpartitions)  # 计算该层的函数依赖
        dictCplus, finallistofFDs, L[i] = prune(L[i], dictCplus, finallistofFDs, dictpartitions)  # 剪枝，删除Li中的集合，修剪搜索空间
        nextlevel, dictpartitions = generate_next_level(L[i], dictpartitions, tableT)
        L.append(nextlevel)  # 将生成的层追加到L集合中
        i = i + 1
    return finallistofFDs


if __name__ == '__main__':
    fp = 'testdata/exp.csv'
    main(fp)
