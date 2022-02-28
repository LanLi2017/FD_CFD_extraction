#!/usr/bin/python
# https://www.postgresqltutorial.com/postgresql-python/connect/
import csv
from collections import Counter
from pprint import pprint

import psycopg2
import xml.etree.ElementTree as ET


def tune_parameters(e2_er=[], e3_er=[], e4_er=[], e5_er=[],
                    e1_er=[],
                    error_pc_index=4,
                    xml_path='../BART-master/Bart_Engine/misc/resources/employees/employees-dbms-50-egtask.xml',
                    e1=False, e2=False, e3=False, e4=False, e5=False):
    """
    This function is to change the error percentage
    or add new elements with new constraints
    ==> if the constraint already exists, you can not add a same constraint with the different parameters.
    separately;
    :param e5: add element e5
    :param e4: add element e4
    :param e3: add element e3
    :param e2: add element e2
    :param e1: add element e1
    :param e1_er: error rate for e1: [left_error_rate, right_error_rate]
    :param e2_er: error rate for e2
    :param e3_er: error rate for e3
    :param e4_er: error rate for e4
    :param e5_er: error rate for e5
    :param error_pc_index: error percentage block index from the xml file
    :param xml_path: xml file path
    :param add_elm: add denial constraints
    :return:
    """
    # this function is to edit the error generation rate in xml
    # passing the path of xml document to enable the parsing process
    tree = ET.parse(xml_path)
    root = tree.getroot()

    config = root[4]
    print(config)
    error_pec = config[error_pc_index]
    print(error_pec)
    vio_generation = error_pec[1]
    print(vio_generation)
    # what are the current constraints?
    rules = []
    for elem in vio_generation.findall('vioGenQuery'):
        # for subelem in elem.findall('vioGenQuery'):
        # if we know the name of the attribute, access it directly
        print(elem.get('id'))
        rules.append(elem.get('id'))
    print(rules)
    count_elem = len(rules)

    ''' change error rate '''
    if e1_er:
        left_error_pct = e1_er[0]
        right_error_pct = e1_er[1]
        res = []
        for elem in vio_generation:
            id_elem = elem.get('id')
            if id_elem == 'e1':
                res.append(elem)

        for i, rule in enumerate(res):
            if i == 0:
                for subelem in rule.iter('percentage'):
                    subelem.text = str(left_error_pct)
            elif i == 1:
                for subelem in rule.iter('percentage'):
                    subelem.text = str(right_error_pct)

    '''create XML sub-elements adding an element to the vio_generation'''
    if e1 and ('e1' not in rules):
        attrib = {'id': 'e1'}
        element = vio_generation.makeelement('vioGenQuery', attrib)
        vio_generation.append(element)

        # adding an element to the left
        attrib_c = {}
        ET.SubElement(vio_generation[count_elem], 'comparison', attrib_c)
        vio_generation[count_elem][0].text = '(n1 == n2)'

        ET.SubElement(vio_generation[count_elem], 'percentage', attrib_c)
        vio_generation[count_elem][1].text = '5'

        # add element on the right
        attrib = {'id': 'e1'}
        element = vio_generation.makeelement('vioGenQuery', attrib)
        vio_generation.append(element)
        ET.SubElement(vio_generation[count_elem + 1], 'comparison', attrib_c)
        vio_generation[count_elem + 1][0].text = '(d1 != d2)'

        ET.SubElement(vio_generation[count_elem + 1], 'percentage', attrib_c)
        vio_generation[count_elem + 1][1].text = '5'

    if e2 and 'e2' not in rules:
        pass

    if e3 and 'e3' not in rules:
        pass

    if e4 and 'e4' not in rules:
        pass

    if e5:
        attrib = {'id': 'e5'}
        element = vio_generation.makeelement('vioGenQuery', attrib)
        vio_generation.append(element)

        # adding an element to the seconditem node
        attrib_c = {}
        ET.SubElement(vio_generation[count_elem], 'comparison', attrib_c)
        vio_generation[count_elem][0].text = '(m1 == n2)'

        attrib_p = {}
        ET.SubElement(vio_generation[count_elem], 'percentage', attrib_p)
        vio_generation[count_elem][1].text = '2.0'

    tree.write(xml_path)


def run_xml():
    pass


def connect(db='bart_employees_50'):
    """ Connect to the PostgreSQL database server """
    conn = None
    clean_db = []
    dirty_db = []
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(host='localhost', user='postgres', password='root',
                                database=db)

        # create a cursor
        cur = conn.cursor()

        # execute a statement
        print('PostgreSQL database version:')
        cur.execute('SELECT * from target.emp;')
        rows = cur.fetchall()
        # print(f'the number of rows in target dataset: {cur.rowcount}')
        for row in rows:
            # print(row)
            clean_db.append(row)

        cur.execute('SELECT * from target_dirty.emp;')
        rows = cur.fetchall()
        # print(f'the number of rows in dirty version: {cur.rowcount}')
        for row in rows:
            # print(row)
            dirty_db.append(row)

        # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')
    return clean_db, dirty_db


def save_data(csv_path, input_db):
    with open(csv_path, 'w') as out:
        csv_out = csv.writer(out)
        csv_out.writerow(['oid', 'name', 'dept', 'salary', 'manager'])
        for row in input_db:
            csv_out.writerow(row)


def get_col_list(col_id, db):
    collection_list = []
    for tu in db:
        collection_list.append(tu[col_id])
    return collection_list


def get_duplicates(collection_list):
    indices = {}
    for idx, item in enumerate(collection_list):
        indices.setdefault(item, []).append(idx)
    dup_id_list = []
    dup_value_list = []
    for key, value in indices.items():
        if len(value) > 1:
            dup_value_list.append(key)
            dup_id_list.extend(value)
            print(f'value: {key} ==> freq: {len(value)}; the index: {value}')
    print(f'The duplicated value list: {dup_value_list}')
    print(f'duplicated name values index list: {dup_id_list}')
    return indices, dup_value_list, dup_id_list


def get_rules(to_collection_list, dup_value_list, dup_id_list, from_collection_list):
    """<vioGenQuery id="e1">
                    <comparison>(n1 == n2)</comparison>
                    <percentage>10</percentage>
                </vioGenQuery>
                <vioGenQuery id="e1">
                    <comparison>(d1 != d2)</comparison>
                    <percentage>10</percentage>
                </vioGenQuery> """
    rule_dict = {k: [] for k in dup_value_list}
    # dep_value_list = []
    for row_id in dup_id_list:
        # print(f'Duplicated row id: {row_id}; the value is {from_collection_list[row_id]}')
        dep_value = to_collection_list[row_id]
        rule_dict[from_collection_list[row_id]].append(dep_value)
    print('the name-> department:')
    pprint(rule_dict)
    return rule_dict


def exe_stats(dirty_db, clean_db):
    print("The first test is on duplication parameter:")
    # e1: n1==n2;
    print(f'the value counts distribution from dirty dataset:')
    name_collection_list = get_col_list(1, dirty_db)
    name_indices, dup_name_value_list, dup_name_id_list = get_duplicates(name_collection_list)
    print(f'The number of duplicated rows in column name : {len(dup_name_id_list)}')

    # what's the value distribution from the clean dataset?
    print(f'the value counts distribution from clean dataset:')
    name_collect_clean = get_col_list(1, clean_db)
    name_clean_indices, dup_name_v_clean_list, dup_name_id_clean_list = get_duplicates(name_collect_clean)
    intersection_id = set(dup_name_id_list) & set(dup_name_id_clean_list)
    union_id = set(dup_name_id_list).union(dup_name_id_clean_list)
    new_name_id_generated = union_id - intersection_id
    print(f'The row index of new generated errors on column name: {new_name_id_generated}')
    print(f'The number of new generated duplicated errors on column name: {len(new_name_id_generated)}')
    # <comparison>(n1 == n2)</comparison>
    # <percentage>10</percentage>
    assert len(new_name_id_generated) == int(0.05 * len(dirty_db))

    # if n1==n2; ==> d1 != d2
    print("The second test is on rule-based violation parameter:")
    print('Dirty testdata: Data distribution in column department ')
    dep_collection_list = get_col_list(2, dirty_db)
    name_dep_dict = get_rules(dep_collection_list, dup_name_value_list, dup_name_id_list, name_collection_list)

    print('Clean testdata: Data distribution in column department:')
    dep_collection_clean_list = get_col_list(2, clean_db)
    name_dep_clean_dict = get_rules(dep_collection_clean_list, dup_name_v_clean_list, dup_name_id_clean_list,
                                    name_collect_clean)


def main(clean_db_path='exp_data/employee_50_egtask_clean.csv',
         dirty_db_path='exp_data/employee_50_egtask_dirty.csv'
         ):
    clean_db, dirty_db = connect()
    save_data(clean_db_path, clean_db)
    save_data(dirty_db_path, dirty_db)
    print(f'the length of clean dataset: {len(clean_db)}')
    print(f'the length of dirty dataset: {len(dirty_db)}')

    error_c = 0
    for i, tu_clean in enumerate(clean_db):
        tu_dirty = dirty_db[i]
        # temp = (element in list(tu_dirty) for element in list(tu_clean))
        # print(f'in row {i}: the overlap: {temp}')
        count = len(tu_clean) - sum(element in list(tu_dirty) for element in list(tu_clean))
        # if count != 0:
        #     print(f'the dirty row: {i}')
        error_c += count
    print(f'the error count : {error_c}')
    exe_stats(dirty_db, clean_db)


if __name__ == '__main__':
    tune_parameters(e1_er=[5, 5])
    # main(clean_db_path='exp_data/employee_50_egtask_clean.csv',
    #      dirty_db_path='exp_data/employee_50_egtask_dirty.csv')
