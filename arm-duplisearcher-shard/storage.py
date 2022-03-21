import pandas as pd
from scipy.sparse import hstack, vstack


class QueriesMatrix:
    """"""
    def __init__(self):
        self.ids = []
        self.queries = []
        self.matrix = None

    def add(self, queries: [()]):
        """tuples must be like (query_id, query_vector)"""
        if self.queries is None:
            self.queries = queries
            self.matrix = hstack([v for i, v in queries]).T
            self.ids = [i for i, v in self.queries]
        else:
            self.queries += queries
            self.matrix = vstack((self.matrix, hstack([v for i, v in queries]).T))
            self.ids = [i for i, v in self.queries]

    def delete(self, queries_ids: []):
        """tuples must be like (query_id, query_vector)"""
        self.queries = [(i, v) for i, v in self.queries if i not in queries_ids]
        self.ids = [i for i, v in self.queries]
        if self.queries:
            self.matrix = hstack([v for i, v in self.queries]).T
        else:
            self.queries = []
            self.matrix = None


class QueriesStorage:
    """"""
    def __init__(self):
        self.queries = pd.DataFrame({}, columns=["queryId", "answerId", "moduleId", "cluster", "pubIds", "tokens"])
        self.query_ids = []
        self.answer_ids = []

    def add(self, input_queries: pd.DataFrame):
        """dictionary must include all attributes"""
        self.queries = pd.concat([self.queries, input_queries])
        self.query_ids = list(self.queries["queryId"])
        self.answer_ids = list(set(self.queries["answerId"]))

    def delete(self, items: [{}], what="queries"):
        """dictionary must include all attributes"""
        if what == "queries":
            self.queries = self.queries[~self.queries["queryId"].isin(items)]
        else:
            self.queries = self.queries[~self.queries["answerId"].isin(items)]
        self.query_ids = list(self.queries["queryId"])
        self.answer_ids = list(set(self.queries["answerId"]))

    def search(self, item_ids: [], what="queries"):
        """Возвращает текст вопроса с метаданными по входящему списку query_ids"""
        if what == "queries":
            return self.queries[self.queries["queryId"].isin(item_ids)]
        else:
            return self.queries[self.queries["answerId"].isin(item_ids)]
