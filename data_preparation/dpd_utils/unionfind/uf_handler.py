import inspect


class UnionFindHandler:
    def __init__(self, sql_file, reset):
        self.sql_file = sql_file
        self.reset = reset


    def close(self):
        raise NotImplementedError(str(inspect.stack()[0][3]) + " is not implemented.")

    def _root(self, rid):
        raise NotImplementedError(str(inspect.stack()[0][3]) + " is not implemented.")

    def find(self, p , q):
        raise NotImplementedError(str(inspect.stack()[0][3]) + " is not implemented.")

    def union(self, p, q):
        raise NotImplementedError(str(inspect.stack()[0][3]) + " is not implemented.")

    def insert_rid(self, rid):
        raise NotImplementedError(str(inspect.stack()[0][3]) + " is not implemented.")

    def insert_uf_record(self, uf_record):
        raise NotImplementedError(str(inspect.stack()[0][3]) + " is not implemented.")

    def delete_rid(self, rid):
        raise NotImplementedError(str(inspect.stack()[0][3]) + " is not implemented.")

    def _get_records(self, conditions):
        raise NotImplementedError(str(inspect.stack()[0][3]) + " is not implemented.")

    def _get_size(self, id):
        raise NotImplementedError(str(inspect.stack()[0][3]) + " is not implemented.")

    def _get_subtree_nodes(self, root_id):
        raise NotImplementedError(str(inspect.stack()[0][3]) + " is not implemented.")

    def _path_to_root(self, id):
        raise NotImplementedError(str(inspect.stack()[0][3]) + " is not implemented.")

    def expand_record_ids(self, record_ids):
        raise NotImplementedError(str(inspect.stack()[0][3]) + " is not implemented.")


    # def _root(self, i):
    #     j = i
    #     while j != self._id[j]:
    #         self._id[j] = self._id[self._id[j]]
    #         j = self._id[j]
    #     return j
    #
    # def _root_branch_set(self, i):
    #     met = set()
    #     j = i
    #     while j != self._id[j]:
    #         met.add(j)
    #         self._id[j] = self._id[self._id[j]]
    #         j = self._id[j]
    #     met.add(j)
    #     return j, met
    #
    # def find(self, p, q):
    #     if not (self._actv[p] and self._actv[q]):
    #         return False
    #     return self._root(p) == self._root(q)
    #
    # def union(self, p, q):
    #     i = self._root(p)
    #     j = self._root(q)
    #     if self._sz[i] < self._sz[j]:
    #         self._id[i] = j
    #         self._sz[j] += self._sz[i]
    #     else:
    #         self._id[j] = i
    #         self._sz[i] += self._sz[j]
    #
    # def insert_id(self, rid):
    #     if rid <= self.cur_alloc_size - 1:
    #         if self._actv[rid]:
    #             raise Exception("ID Already exists in the data structure")
    #         self._actv[rid] = True
    #         self._id[rid] = rid
    #         self._sz[rid] = 1
    #     else:
    #         expansion = math.ceil(rid / self.cur_alloc_size)
    #         new_alloc_size = (1 + expansion) * self.cur_alloc_size
    #
    #         self._actv = np.resize(self._actv, new_alloc_size)
    #         self._id = np.resize(self._id, new_alloc_size)
    #         self._sz = np.resize(self._sz, new_alloc_size)
    #         # self._actv.resize(new_alloc_size)
    #         # self._id.resize(new_alloc_size)
    #         # self._sz.resize(new_alloc_size)
    #         for i in range(self.cur_alloc_size, new_alloc_size):
    #             self._actv[i] = False
    #             self._id[i] = -1
    #             self._sz[i] = 0
    #
    #         self._actv[rid] = True
    #         self._id[rid] = rid
    #         self._sz[rid] = 1
    #
    #         self.cur_alloc_size = new_alloc_size
    #
    #     self.cur_size += 1
    #
    # def delete_id(self, rid):
    #     if rid <= self.cur_alloc_size - 1:
    #         max_size = -1
    #         max_size_id = -1
    #         ids_to_repoint = []
    #         for i in range(self.cur_alloc_size):
    #             if i == rid:
    #                 continue
    #
    #             if self._id[i] == rid:
    #                 ids_to_repoint.append(i)
    #                 if self._sz[i] > max_size:
    #                     max_size = self._sz[i]
    #                     max_size_id = i
    #
    #         for i in ids_to_repoint:
    #             self._id[i] = max_size_id
    #             self._sz[max_size_id] += self._sz[i]
    #
    #         self._actv[rid] = False
    #         self._sz[rid] = 0
    #         self._id[rid] = -1
    #
    #         self.cur_size -= 1
    #     else:
    #         raise Exception("ID does NOT exist in the data structure")



    # def merge_to_duplicate_sets(self, ids_to_merge):
    #     root_to_id = {}
    #     met = set()
    #     for i in ids_to_merge:
    #         j, _met = self._root_branch_set(i)
    #         met.update(_met)
    #         if j not in root_to_id:
    #             root_to_id[j] = set()
    #         root_to_id[j].add(i)
    #         root_to_id[j].update(_met)
    #
    #     return met, root_to_id
    #
    # def merge_and_expand_to_duplicates_sets(self, ids_to_expand):
    #     met, root_to_id = self.merge_to_duplicate_sets(ids_to_expand)
    #
    #     for i in range(self.cur_alloc_size):
    #         if self._actv[i] and i not in met:
    #             j, _met = self._root_branch_set(i)
    #             if j not in root_to_id:
    #                 continue
    #             met.update(_met)
    #             root_to_id[j].update(_met)
    #
    #     return root_to_id