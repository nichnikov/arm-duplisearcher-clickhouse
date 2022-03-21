import logging
import pandas as pd
from collections import namedtuple
from itertools import chain, groupby
from scipy.sparse import hstack, vstack
from sklearn.metrics.pairwise import cosine_similarity
from storage import QueriesMatrix, QueriesStorage

# how to get certain rows from sparse matrix:
# https://cmdlinetips.com/2019/07/how-to-slice-rows-and-columns-of-sparse-matrix-in-python/
# logging.basicConfig(filename="std.log", format='%(asctime)s %(message)s', filemode='w')

logger = logging.getLogger("seacher")
logger.setLevel(logging.INFO)


class MainSearcher:
    """"""

    def __init__(self):
        self.ids = []
        self.matrix = None

    def update(self, ids_list, matrix_list):
        """"""
        self.ids = []
        self.matrix = None
        for id_l in ids_list:
            self.ids += id_l
        self.matrix = vstack(matrix_list)

    def search(self, queries: [()], score: float):
        """tuples must be like (query_id, query_vector)"""
        searched_ids, vectors = zip(*queries)
        searched_matrix = hstack(vectors).T
        try:
            matrix_scores = cosine_similarity(searched_matrix, self.matrix, dense_output=False)
            ResultItem = namedtuple("ResultItem", "SearchedAnswerId, FoundAnswerId, Score")
            search_results = [[ResultItem(q_id, self.ids[i], sc) for i, sc in zip(scores.indices, scores.data)
                               if sc >= score] for scores, q_id in zip(matrix_scores, searched_ids)]
            logger.info('File successfully uploaded to ')
            return [x for x in chain(*search_results) if x]
        except Exception as e:
            logger.error('Failed queries search in MainSearcher.search: ' + str(e))
            return []


class Main:
    def __init__(self):
        self.queries_matrix_list = [QueriesMatrix()]
        self.queries_storage_list = [QueriesStorage()]
        self.main_searcher = MainSearcher()
        self.max_size = 20000

    def del_all(self):
        self.queries_matrix_list = [QueriesMatrix()]
        self.queries_storage_list = [QueriesStorage()]
        self.main_searcher = MainSearcher()

    def add(self, queries_vectors, data):
        """"""
        flag = True
        for q_m, q_s in zip(self.queries_matrix_list, self.queries_storage_list):
            if len(q_m.queries) < self.max_size:
                q_m.add(queries_vectors)
                q_s.add(pd.DataFrame(data, columns=["queryId", "answerId", "moduleId", "cluster", "pubIds", "tokens"]))
                flag = False
        if flag:
            """adding new queries_matrix"""
            q_m = QueriesMatrix()
            q_s = QueriesStorage()
            q_m.add(queries_vectors)
            q_s.add(pd.DataFrame(data, columns=["queryId", "answerId", "moduleId", "cluster", "pubIds", "tokens"]))
            self.queries_matrix_list.append(q_m)
            self.queries_storage_list.append(q_s)

        self.main_searcher.update([qm.ids for qm in self.queries_matrix_list],
                                  [qm.matrix for qm in self.queries_matrix_list])

    def queries_delete(self, q_ids: []):
        """"""
        for queries_matrix, queries_storage in zip(self.queries_matrix_list, self.queries_storage_list):
            if set(tuple(q_ids)) & set(tuple(queries_matrix.ids)):
                queries_matrix.delete(q_ids)
                queries_storage.delete(list(set(q_ids)))
        self.main_searcher.update([qm.ids for qm in self.queries_matrix_list],
                                  [qm.matrix for qm in self.queries_matrix_list])

    def answers_delete(self, a_i):
        """"""
        for queries_matrix, queries_storage in zip(self.queries_matrix_list, self.queries_storage_list):
            if set(tuple(a_i)) & set(queries_storage.answer_ids):
                q_del_df = queries_storage.search(list(set(a_i)), what="answers")
                queries_matrix.delete(list(q_del_df["queryId"]))
                queries_storage.delete(list(q_del_df["queryId"]))
        self.main_searcher.update([qm.ids for qm in self.queries_matrix_list],
                                  [qm.matrix for qm in self.queries_matrix_list])

    def update(self, a_i, queries_vectors, data):
        """"""
        for queries_matrix, queries_storage in zip(self.queries_matrix_list, self.queries_storage_list):
            if set(tuple(a_i)) & set(queries_storage.answer_ids):
                q_del_df = queries_storage.search(list(set(a_i)), what="answers")
                queries_matrix.delete(list(q_del_df["queryId"]))
                queries_storage.delete(list(q_del_df["queryId"]))
        self.add(queries_vectors, data)
        self.main_searcher.update([qm.ids for qm in self.queries_matrix_list],
                                  [qm.matrix for qm in self.queries_matrix_list])

    def search(self, queries_vectors, min_score=0.99):
        """"""
        result_tuples = self.main_searcher.search(queries_vectors, min_score)
        if result_tuples:
            searched_ids, found_ids, scores = zip(*result_tuples)
            found_dicts_l = [obj.search(found_ids).to_dict(orient="records") for obj in
                             self.queries_storage_list if set(found_ids) & set(obj.query_ids)]
            return [result_tuples, found_dicts_l]
        else:
            return []
