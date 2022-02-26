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


def findCplus(x):  # this computes the Cplus of x as an intersection of smaller Cplus sets
    global dictCplus
    thesets = []
    print('Knock knock')
    for a in x:
        # 计算的过程涉及了递归
        if x.replace(a, '') in dictCplus.keys():
            temp = dictCplus[x.replace(a, '')]
        else:
            temp = findCplus(x.replace(a, ''))  # compute C+(X\{A}) for each A at a time
        # dictCplus[x.replace(a,'')] = temp
        thesets.insert(0, set(temp))
    if not list(set.intersection(*thesets)):
        cplus = []
    else:
        cplus = list(set.intersection(*thesets))  # compute the intersection in line 2 of pseudocode
    return cplus


def compute_dependencies(level, listofcols):
    global dictCplus  # 属性-右方集dict
    global finallistofFDs  # FD list
    # global listofcolumns  # 属性集list
    # FUN1: 计算所有X属于Li的右方集Cplus
    # 通过上层结点{A}计算当前层的每个X的Cplus(X)
    # 或者通过computeCplus
    print('============')
    print(f'what is current level: {level}')
    for x in level:
        thesets = []
        for a in x:
            print(f'a is : {a}')
            print(f'what x here: {x}')
            diff = x.replace(a, '')
            print(f'This might be the diff: {diff}')
            if x.replace(a, '') in dictCplus.keys():  # 如果Cplus(X\A) 已经在当前右方集List中
                temp = dictCplus[x.replace(a, '')]  # temp存入的是Cplus(X\A) -- 即X\A的右集合
            else:  # 否则，计算右方集
                temp = computeCplus(x.replace(a, ''))  # compute C+(X\{A}) for each A at a time
                dictCplus[x.replace(a, '')] = temp  # 存入dictCplus中
            print(temp)
            thesets.insert(0, set(temp))  # 通过set, 将temp转化成集合，再将该对象插入到列表的第0个位置
            print('Last call!!!')

        if not list(set.intersection(*thesets)):  # set.intersection(set1, set2, ...ect)求并集
            dictCplus[x] = []
        else:
            print(f'here x is {x}')
            dictCplus[x] = list(set.intersection(*thesets))  # compute the intersection in line 2 of pseudocode
        pprint(dictCplus)

    # Fun2: 找到最小函数依赖
    # 并对Cplus进行剪枝（最小性剪枝）： 1.删掉已经成立的。 2去掉必不可能的 留下的是"仍有希望的"
    for x in level:
        print(f'for each x in level: {x}')
        for a in x:
            if a in dictCplus[x]:
                print(f'current a: {a} ')
                # if x=='BCJ': print "dictCplus['BCJ'] = ", dictCplus[x]
                print(f'current x: {x}')
                print(f'delete element a: {x.replace(a, "")}')
                if validfd(x.replace(a, ''), a):  # line 5 即x\{A} -> A 函数依赖成立
                    finallistofFDs.append([x.replace(a, ''), a])  # line 6
                    print(f'compute_dependencies: level{level} adding key FD: {[x.replace(a, ""), a]}')
                    dictCplus[x].remove(a)  # line 7
                    pprint(dictCplus)

                    listofcols = listofcols[:]  # 为了下面的剪枝作准备
                    for j in x:  # this loop computes R\X
                        if j in listofcols:
                            listofcols.remove(j)
                    print(listofcols)
                    for b in listofcols:  # this loop removes each b in R\X from C+(X)
                        print(f'b is {b}')
                        print(f'dict Cplus : {dictCplus}')
                        # 在C+(X) 删掉所有属于R\X 即不属于X的元素， 即留下的Cplus元素全部属于X
                        if b in dictCplus[x]:
                            dictCplus[x].remove(b)
                        print(f'current dict Cplus: {dictCplus}')
    print('New Information!')
    print(finallistofFDs)


def computeCplus(x):
    # this computes the Cplus from the first definition in section 3.2.2 of TANE paper.
    # output should be a list of single attributes
    global listofcolumns
    listofcols = listofcolumns[:]
    if x == '':
        return listofcols  # because C+{phi} = R
    cplus = []
    for a in listofcols:  # A属于R并满足如下条件
        for b in x:
            print('Pay attention!')
            print(f'before remove: {x}')
            temp = x.replace(a, '')
            temp = temp.replace(b, '')
            print(temp)
            if not validfd(temp, b):
                cplus.append(a)
    return cplus


def validfd(y, z):
    print('Look at this!')
    if y == '' or z == '':
        print(f'{y} == " "')
        return False
    ey = computeE(y)
    eyz = computeE(y + z)

    if ey == eyz:
        print(f'{y}, {y + z} ==> {ey} == {eyz}')
        return True
    else:
        print(f'{y}, {y + z} ==> {ey} != {eyz}')
        return False


def computeE(x):  # 属性集为x
    global totaltuples  # 元组数
    global dictpartitions  # 关于每个属性集的剥离分区
    doublenorm = 0
    for i in dictpartitions[''.join(sorted(x))]:
        print(f'for i : {i} in dictpartitions {x}')
        doublenorm = doublenorm + len(i)
    e = (doublenorm - len(dictpartitions[''.join(sorted(x))])) / float(totaltuples)
    # print(doublenorm)
    # print(len(dictpartitions[''.join(sorted(x))]))
    # print(totaltuples)
    # print(e)
    return e


def check_superkey(x):
    global dictpartitions
    print(dictpartitions)
    if (dictpartitions[x] == [[]]) or (dictpartitions[x] == []):  # 如果剥离分区为空， 则说明pi_x 只有单例等价类组成
        return True
    else:
        return False


def prune(level):
    global dictCplus
    global finallistofFDs
    # stufftobedeletedfromlevel = []
    print(f'before remove: level {level}')
    for x in level:  # line 1
        '''Angle 1: 右方集修建'''
        print(f'what is x: {x}')
        print(f'{dictCplus[x]}')
        if not dictCplus[x]:  # line 2
            print(f'x should be {x}')
            level.remove(x)  # line 3
        '''Angle 2: 键修建'''
        print(f'current level: {level}')
        if check_superkey(''.join(sorted(x))):  # line 4   ### should this check for a key, instead of super key??? Not sure.
            temp = dictCplus[x][:]  # 初始化temp 为computes cplus(X)
            print(f'begin..... temp: {temp}')

            # 求C+(X)\X
            for i in x:  # this loop computes C+(X) \ X
                if i in temp:
                    temp.remove(i)
            print(f'after remove: {temp}')
            for a in temp:  # line 5: for each a 属于 Cplus(X)\X do
                thesets = []
                # 计算Cplus((X+A)\ {B})
                for b in x:
                    if not (''.join(sorted((x + a).replace(b, ''))) in dictCplus.keys()):
                        # ''.join(sorted((x+a).replace(b,''))表示的就是XU{a}\{b}
                        dictCplus[''.join(sorted((x + a).replace(b, '')))] = findCplus(
                            ''.join(sorted((x + a).replace(b, ''))))
                    thesets.insert(0, set(dictCplus[''.join(sorted((x + a).replace(b, '')))]))
                # 4. 计算Cplus((X+A)\ {B})交集，判断a是否在其中
                if a in list(set.intersection(*thesets)):  # line 6
                    finallistofFDs.append([x, a])  # line 7
                # print "adding key FD: ", [x,a]
            if x in level:
                level.remove(x)  # 只要x是超键， 就要剪掉x
                # stufftobedeletedfromlevel.append(x)  # line 8
    print('Last call')
    pprint(dictCplus)
    print(finallistofFDs)
    # for item in stufftobedeletedfromlevel:
    #     level.remove(item)


def generate_next_level(level):
    # 首先令 L[i+1] 这一层为空集
    nextlevel = []
    for i in range(0, len(level)):  # pick an element
        for j in range(i + 1, len(level)):  # compare it to every element that comes after it.
            # 如果这两个元素属于同一个前缀块，那么就可以合并:只有最后一个属性不同，其余都相同
            print(f'current level: {level}')
            print(f'last element: {level[i]} ==> {level[j]}')
            print(f'previous element: {level[i][0:-1]} ==> {level[j][0:-1]}')
            if (not level[i] == level[j]) and level[i][0:-1] == level[j][0:-1]:  # i.e. line 2 and 3
                x = level[i] + level[j][-1]  # line 4
                print(f'Attention! Level : {level[i] }; {level[j][-1]};')
                print(f'x in current level: {x}')
                flag = True
                for a in x:  # this entire for loop is for the 'for all' check in line 5
                    if not (x.replace(a, '') in level):
                        flag = False
                if flag:
                    nextlevel.append(x)
                    # 计算新的属性集X上的剥离分区
                    # =pi_y*pi_z（其中y为level[i]，z为level[j]）
                    stripped_product(x, level[i], level[
                        j])  # compute partition of x as pi_y * pi_z (where y is level[i] and z is level[j])
    return nextlevel


def stripped_product(x, y, z):
    global dictpartitions  # 剥离分区
    global tableT
    tableS = [''] * len(tableT)
    # partitionY、partitionZ是属性集Y、Z上的剥离分区，已知！
    # partitionY is a list of lists, each list is an equivalence class
    partitionY = dictpartitions[''.join(sorted(y))]
    partitionZ = dictpartitions[''.join(sorted(z))]
    print("y:%s partitionY:%s,z:%s partitionZ%s" % (y, partitionY, z, partitionZ))
    partitionofx = []  # line 1
    for i in range(len(partitionY)):  # line 2
        for t in partitionY[i]:  # line 3
            tableT[t] = i
        tableS[i] = ''  # line 4
    for i in range(len(partitionZ)):  # line 5
        for t in partitionZ[i]:  # line 6
            if not (tableT[t] == 'NULL'):  # line 7
                tableS[tableT[t]] = sorted(list(set(tableS[tableT[t]]) | set([t])))
        for t in partitionZ[i]:  # line 8
            if (not (tableT[t] == 'NULL')) and len(tableS[tableT[t]]) >= 2:  # line 9
                partitionofx.append(tableS[tableT[t]])
            if not (tableT[t] == 'NULL'): tableS[tableT[t]] = ''  # line 10
    for i in range(len(partitionY)):  # line 11
        for t in partitionY[i]:  # line 12
            tableT[t] = 'NULL'
    dictpartitions[''.join(sorted(x))] = partitionofx  # 生成属性集X上的剥离分区
    print(f'x={x},partitionX={partitionofx}')


def computeSingletonPartitions(listofcols):
    global data2D
    global dictpartitions
    for a in listofcols:
        dictpartitions[a] = []
        for element in list_duplicates(data2D[
                                           a].tolist()):  # list_duplicates returns 2-tuples, where 1st is a value, and 2nd is a list of indices where that value occurs
            if len(element[1]) > 1:  # ignore singleton equivalence classes
                dictpartitions[a].append(element[1])


# ------------------------------------------------------- START ---------------------------------------------------

# if len(sys.argv) > 1:
#     infile = str(sys.argv[1])  # this would be e.g. "testdata.csv"

# 测试computeSingletonPartitions
# 此时考虑的属性集只有A,B,C,D，在单个属性集上面生成剥离分区
'''测试list_duplicates函数的返回值:返回的是每个属性列表中每个属性的剥离分区'''
#
data2D = read_csv('testdata/employee.csv')

totaltuples = len(data2D.index)
listofcolumns = list(data2D.columns.values)  # returns ['A', 'B', 'C', 'D', .....]
dictpartitions = {}
dictCplus = {'NULL': listofcolumns}
print(data2D)
for a in listofcolumns:  # 为索引列
    print("a=", a)
    dictpartitions[a] = []
    print(a, data2D[a].tolist())
    # for element in list_duplicates(data2D[a].tolist()):
    #     print("element=", element)
    for element in list_duplicates(data2D[
                                       a].tolist()):  # list_duplicates returns 2-tuples, where 1st is a value, and 2nd is a list of indices where that value occurs
        if len(element[1]) > 1:  # ignore singleton equivalence classes
            dictpartitions[a].append(element[1])
print(f'dict partitions: {dictpartitions}')  # 存放的是每个属性集上的剥离分区
print(f'C plus: {dictCplus}')

finallistofFDs = []
# print dictCplus['NULL']
# 初始时，L1层包含的属性集为：A,B,C,D...

L1 = listofcolumns[:]  # L1 is a copy of listofcolumns

i = 1
L0 = []
L = [L0, L1]
print(f'initial level: {L}')
tableT = ['NULL'] * totaltuples  # this is for the table T used in the function stripped_product
print(f'tableT: {tableT}')
while not (L[i] == []):  # 第i层的包含的属性集不为空
    compute_dependencies(L[i], listofcolumns[:])  # 计算该层的函数依赖
    prune(L[i])  # 剪枝，删除Li中的集合，修剪搜索空间
    temp = generate_next_level(L[i])
    L.append(temp)  # 将生成的层追加到L集合中
    i = i + 1

print("List of all FDs: ", finallistofFDs)
#  correct result
#  List of all FDs:  [['C', 'D'], ['C', 'A'], ['C', 'B'], ['AD', 'B'], ['AD', 'C']]
# Total number of FDs found:  5
print("Total number of FDs found: ", len(finallistofFDs))
print(L)
pprint(dictpartitions)
pprint(dictCplus)
