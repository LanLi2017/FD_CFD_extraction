import sqlite3

from .uf_handler import UnionFindHandler


class DBUnionFindHandler(UnionFindHandler):
    def __init__(self, sql_file, reset):
        super().__init__(sql_file, reset)

        self.conn = sqlite3.connect(database=sql_file, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.crsr = self.conn.cursor()

        if reset:
            # It's better to keep this order, since there is a foreign key relationship and SQLite complains otherwise.
            self._drop_union_find_relation()
            self._create_union_find_relation()

    def _root(self, rid):
        id = rid

        record = self._get_records({'id': [id]})[0]

        while record['id'] != record['parent_id']:
            record = self._get_records({'id': [id]})[0]
        return record

    def union(self, p, q):
        i = self._root(p)
        j = self._root(q)
        smaller, larger = (i, j) if int(i['size']) < int(j['size']) else (j, i)
        update_sql = 'UPDATE UnionFind SET parent_id=' + str(larger['id']) + ' WHERE id=' + str(smaller['id'])
        self.crsr.execute(update_sql)
        update_sql = 'UPDATE UnionFind SET size=' + str(int(i['size']) + int(j['size'])) + ' WHERE id=' + str(
            larger['id'])
        self.crsr.execute(update_sql)

    def find(self, p, q):
        return self._root(p) == self._root(q)

    def delete_rid(self, rid):
        print("ERROR. TODO: Implement this method. delete_rid")
        exit(0)

    def insert_rid(self, rid):
        self.insert_uf_record({'id': rid, 'parent_id': rid, 'size': 1})

    def insert_uf_record(self, uf_record):
        insert_sql = 'INSERT INTO UnionFind (id, parent_id, size) VALUES (?, ?, ?)'
        self.crsr.execute(insert_sql, [uf_record['id'], uf_record['parent_id'], uf_record['size']])

    def close(self):
        self.conn.commit()
        self.conn.close()

    def _get_records(self, conditions):
        select_sql = 'SELECT * FROM UnionFind'
        if len(conditions) >= 1:
            select_sql += ' WHERE ' + ' AND '.join(
                [attr + (' IN (' + ', '.join(values) + ')' if len(values) > 1 else '=' + str(values[0]))
                 for attr, values in conditions.items()])
        self.crsr.execute(select_sql)
        results = self.crsr.fetchall()
        records = [dict(zip(r.keys(), r)) for r in results]
        return records

    def _get_size(self, id):
        select_sql = 'SELECT size FROM UnionFind WHERE id=' + str(id)
        self.crsr.execute(select_sql)
        size = self.crsr.fetchone()
        return size

    def _drop_union_find_relation(self):
        drop_table_sql = 'DROP TABLE IF EXISTS UnionFind;'
        self.crsr.execute(drop_table_sql)

    def _create_union_find_relation(self):
        create_table_sql = 'CREATE TABLE IF NOT EXISTS UnionFind (id INTEGER PRIMARY KEY, parent_id INTEGER, size INTEGER)'
        self.crsr.execute(create_table_sql)

    def _path_to_root(self, id):
        met = set()

        j = id
        record = self._get_records({'id': [j]})[0]

        while record['id'] != record['parent_id']:
            met.add(record['id'])

            record = self._get_records({'id': [j]})[0]
            j = record['parent_id']
        met.add(record['id'])
        return record['id'], met

    def _get_subtree_nodes(self, root_rid):
        met = set()
        records = self._get_records({})
        # for i in range(1, self.cur_size + 1):
        for record in records:
            if record['id'] in met:
                continue
            root_path, met_path = self._path_to_root(record['id'])
            if root_path == root_rid:
                met.update(met_path)
        return met

    def expand_record_ids(self, record_ids):
        root_rids = set()
        for rid in record_ids:
            root_rids.add(rid)
        expanded_rids = []
        for root_rid in root_rids:
            met = self._get_subtree_nodes(root_rid)
            if len(met) > 0:
                expanded_rids.append(met)
            else:
                print("Empty set while getting subtree of nodes!")
                print(str(root_rid))

        return expanded_rids
