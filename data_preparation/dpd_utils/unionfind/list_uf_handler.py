import math
import os

import numpy as np
import progressbar

from .db_uf_handler import DBUnionFindHandler
from .uf_handler import UnionFindHandler


class ListUnionFindHandler(UnionFindHandler):
    def __init__(self, sql_file, reset):
        super().__init__(sql_file, reset)

        if reset or not os.path.exists(sql_file):
            if not os.path.exists(os.path.dirname(sql_file)):
                os.makedirs(os.path.dirname(sql_file))

            self.cur_alloc_size = 1024
            self.cur_size = 0
            self._id = np.full(self.cur_alloc_size, fill_value=-1, dtype=int)
            self._sz = np.full(self.cur_alloc_size, fill_value=0, dtype=int)
            # self._actv = np.full(self.init_size, False, dtype=bool)
        else:
            dbuf = DBUnionFindHandler(sql_file, False)
            records_l = dbuf._get_records({})
            dbuf.close()

            self.cur_size = len(records_l)
            # +1, because we leave the first position empty, so that we can index by the id
            self.cur_alloc_size = int(1024 * math.pow(2.0, math.ceil(math.log2((self.cur_size + 1)/1024.0))))

            self._id = np.full(self.cur_alloc_size, fill_value=-1, dtype=int)
            self._sz = np.full(self.cur_alloc_size, fill_value=0, dtype=int)
            for r in records_l:
                self._id[r['id']] = r['parent_id']
                self._sz[r['id']] = r['size']

    def _root(self, rid):
        j = rid
        while j != self._id[j]:
            self._id[j] = self._id[self._id[j]]
            j = self._id[j]
        return j

    def union(self, p, q):
        i = self._root(p)
        j = self._root(q)
        if i == j:
            return

        sml, lrg = (i, j) if self._sz[i] < self._sz[j] else (j, i)

        self._id[sml] = lrg
        # print(str(self._sz[lrg]) + '-->' + str(self._sz[lrg] + self._sz[sml]))
        self._sz[lrg] += self._sz[sml]

    def find(self, p, q):
        return self._root(p) == self._root(q)

    def delete_rid(self, rid):
        print("ERROR. TODO: Implement this method. delete_rid")
        exit(0)

    def insert_rid(self, rid):
        self._insert_uf_record({'id': rid, 'parent_id': rid, 'size': 1})

    def _insert_uf_record(self, uf_record):
        # new_alloc_size = int(1024 * math.pow(2.0, math.ceil(math.log2((self.cur_size + 1 + 1) / 1024.0))))
        new_alloc_size = int(1024 * math.pow(2.0, math.ceil(math.log2((uf_record["id"] + 1) / 1024.0))))
        if new_alloc_size > self.cur_alloc_size:
            print("Resizing: " + str(self.cur_alloc_size) + " --> " + str(new_alloc_size))
            self._id = np.resize(self._id, new_alloc_size)
            self._sz = np.resize(self._sz, new_alloc_size)
            for i in range(self.cur_alloc_size, new_alloc_size):
                self._id[i] = -1
                self._sz[i] = 0
            self.cur_alloc_size = new_alloc_size

        self._id[uf_record['id']] = uf_record['parent_id']
        self._sz[uf_record['id']] = uf_record['size']

        self.cur_size += 1

    def close(self):
        dbuf = DBUnionFindHandler(self.sql_file, True)

        if len(self._id) > 1:
            for i in range(1, len(self._id)):
                dbuf.insert_uf_record({'id': str(i), 'parent_id': str(self._id[i]), 'size': str(self._sz[i])})
        dbuf.close()

    def _get_records(self, conditions):
        pass

    def _get_size(self, id):
        return self.cur_size

    def _path_to_root(self, id):
        met = set()
        j = id
        while j != self._id[j]:
            met.add(j)
            #self._id[j] = self._id[self._id[j]]
            j = self._id[j]
        met.add(j)
        return j, met

    def get_components(self):
        root_rids = set()
        print("[UF] Collecting roots")
        with progressbar.ProgressBar(max_value=self.cur_alloc_size + 1) as progress:
            for i in range(1, self.cur_alloc_size):
                if self._id[i] == -1:
                    continue

                root = self._root(i)
                root_rids.add(root)
                progress.update(i)

        components = []
        print("[UF] Collecting components")

        root_components = {}
        for root_rid in root_rids:
            root_components[root_rid] = set()

        with progressbar.ProgressBar(max_value=self.cur_alloc_size + 1) as progress:
            count = 0
            for i in range(1, self.cur_alloc_size):
                if self._id[i] == -1:
                    continue

                root = self._root(i)
                root_components[root].add(i)

                count += 1
                progress.update(count)

            for root_rid in root_components:
                components.append(list(root_components[root_rid]))

        return components

    def _get_subtree_nodes(self, root_rid):
        met = set()
        for i in range(1, self.cur_size + 1):
            if i in met:
                continue
            root_path, met_path = self._path_to_root(i)
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