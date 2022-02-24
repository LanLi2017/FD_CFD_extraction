#!/usr/bin/python
# https://www.postgresqltutorial.com/postgresql-python/connect/
import csv
from collections import Counter
from pprint import pprint

import psycopg2
from config import config


def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    clean_db = []
    dirty_db = []
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

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
    print('Dirty data: Data distribution in column department ')
    dep_collection_list = get_col_list(2, dirty_db)
    name_dep_dict = get_rules(dep_collection_list, dup_name_value_list, dup_name_id_list, name_collection_list)

    print('Clean data: Data distribution in column department:')
    dep_collection_clean_list = get_col_list(2, clean_db)
    name_dep_clean_dict = get_rules(dep_collection_clean_list, dup_name_v_clean_list, dup_name_id_clean_list,
                                    name_collect_clean)


def main():
    clean_db, dirty_db = connect()
    clean_db_path = 'exp_data/employee_50_egtask_clean.csv'
    dirty_db_path = 'exp_data/employee_50_egtask_dirty.csv'
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
    main()
