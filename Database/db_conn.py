#!/usr/bin/python
# https://www.postgresqltutorial.com/postgresql-python/connect/
import csv

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
        print(f'the number of rows in target dataset: {cur.rowcount}')
        for row in rows:
            print(row)
            clean_db.append(row)

        cur.execute('SELECT * from target_dirty.emp;')
        rows = cur.fetchall()
        print(f'the number of rows in dirty version: {cur.rowcount}')
        for row in rows:
            print(row)
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


def main():
    clean_db, dirty_db = connect()
    clean_db_path = 'exp_data/employee_50_egtask_clean.csv'
    dirty_db_path = 'exp_data/employee_50_egtask_dirty.csv'
    save_data(clean_db_path, clean_db)
    save_data(dirty_db_path, dirty_db)
    print(len(clean_db))
    print(len(dirty_db))

    error_c = 0
    for i, tu_clean in enumerate(clean_db):
        tu_dirty = dirty_db[i]
        # temp = (element in list(tu_dirty) for element in list(tu_clean))
        # print(f'in row {i}: the overlap: {temp}')
        count = len(tu_clean) - sum(element in list(tu_dirty) for element in list(tu_clean))
        if count != 0:
            print(f'the dirty row: {i}')
        error_c += count
    print(f'the error count : {error_c}')


if __name__ == '__main__':
    main()
