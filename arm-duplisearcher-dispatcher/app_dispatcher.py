import os
import logging
import requests
from uuid import uuid4
from waitress import serve
from dotenv import load_dotenv
from flask import Flask, request
from flask_restplus import Api, Resource, fields
from multiprocessing import Pool
from texts_processing import TextsTokenizer
from collections import namedtuple
from itertools import groupby, chain

# how to numpy array to json: https://pynative.com/python-serialize-numpy-ndarray-into-json/
"""
# writing to file:
logging.basicConfig(filename="test.log", filemode="a",
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S')"""

# writing to terminal:
logging.basicConfig(format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S')

logger = logging.getLogger("app_dispatcher")
logger.setLevel(logging.INFO)


def chunks_split(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def add_tokens(tokenizer: TextsTokenizer, queries: [()]):
    """"""
    q_i, a_i, m_i, cls, p_i = zip(*queries)
    tokens = tokenizer(cls)
    return list(zip(q_i, a_i, m_i, cls, p_i, tokens))


def sender(url_data: ()):
    """"""
    response = requests.post(url_data[0], json=url_data[1])
    return response.json()


def resulting_report(searched_data, result_tuples, found_dicts_l):
    """"""

    def grouping(similarity_items, searched_queries, searched_answers_moduls):
        """"""
        return [{"id": k1, "moduleId": searched_answers_moduls[k1], "clustersWithDuplicate":
            [{"cluster": searched_queries[k2]["cluster"], "duplicates":
                [{"cluster": x2.FoundText, "id": x2.FoundAnswerId, "moduleId": x2.FoundModuleId,
                  "pubId": x2.FoundPubIds} for x2 in v2]}
             for k2, v2 in
             groupby(sorted([x1 for x1 in v1], key=lambda c: c.SearchedQueryId), lambda d: d.SearchedQueryId)]}
                for k1, v1 in
                groupby(sorted(similarity_items, key=lambda a: a.SearchedAnswerId), lambda b: b.SearchedAnswerId)]

    ResultItem = namedtuple("ResultItem", "SearchedAnswerId, "
                                          "SearchedText, "
                                          "SearchedQueryId, "
                                          "SearchedModuleId, "
                                          "SearchedPubIds, "
                                          "FoundAnswerId, "
                                          "FoundText, "
                                          "FoundQueryId, "
                                          "FoundModuleId, "
                                          "FoundPubIds")

    searched_dict = {q_i: {"answerId": a_i,
                           "moduleId": m_i,
                           "cluster": cl,
                           "pubIds": p_i} for q_i, a_i, m_i, cl, p_i, vcs in searched_data}

    searched_answers_moduls = {a_i: m_i for q_i, a_i, m_i, cl, p_i, vcs in searched_data}

    found_dict = {d["queryId"]: d for d in chain(*found_dicts_l)}
    similarity_items = [ResultItem(searched_dict[sa_i]["answerId"],
                                   searched_dict[sa_i]["cluster"],
                                   sa_i,
                                   searched_dict[sa_i]["moduleId"],
                                   searched_dict[sa_i]["pubIds"],
                                   found_dict[fa_i]["answerId"],
                                   found_dict[fa_i]["cluster"],
                                   fa_i,
                                   found_dict[fa_i]["moduleId"],
                                   found_dict[fa_i]["pubIds"]) for sa_i, fa_i, sc in result_tuples]

    return grouping(similarity_items, searched_dict, searched_answers_moduls)


def result_aggregate(respons):
    result_tuples_list = []
    result_dicts_list = []
    for x in respons:
        if x:
            result_tuples_list += x[0]
            result_dicts_list += x[1]
    return result_tuples_list, result_dicts_list


app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False
api = Api(app)

name_space = api.namespace('api', 'На вход поступает JSON, возвращает JSON')

answer = name_space.model("One Answer",
                          {"id": fields.Integer(description="query's Id", required=True),
                           "clusters": fields.List(fields.String(description="query's text", required=True)),
                           "moduleId": fields.Integer,
                           "pubIds": fields.List(fields.Integer)})

input_data = name_space.model("Input JSONs",
                              {"score": fields.Float(description="The similarity coefficient", required=True),
                               "data": fields.List(fields.Nested(answer)),
                               "operation": fields.String(description="add/update/delete/search/del_all",
                                                          required=True)})

load_dotenv(".env")
URLS = os.environ.get("URLS")
# URLS = "url1,url1"
if URLS is None:
    raise Exception('Env var URLS not defined')

urls = URLS.split(",")
tokenizer = TextsTokenizer()


@name_space.route('/')
class ShardsHandling(Resource):
    """Service searches duplicates and adding and delete data in collection."""

    @name_space.expect(input_data)
    def post(self):
        """POST method on input JSON file with scores, operation type and lists of fast answers."""
        json_data = request.json
        if json_data["data"]:
            pool = Pool()
            queries = []
            for d in json_data["data"]:
                queries += [(str(uuid4()), d["id"], d["moduleId"], tx, d["pubIds"]) for tx in d["clusters"]]

            if json_data["operation"] == "add":
                try:
                    queries_with_tokens = add_tokens(tokenizer, queries)
                    chunk_size = len(queries) // len(urls) + 1
                    urls_data = [(url, {"data": chunk, "operation": "add"}) for
                                 url, chunk in zip(urls, chunks_split(queries_with_tokens, chunk_size))]
                    respons = pool.map(sender, urls_data)
                    pool.close()
                    pool.join()
                    logger.info(str(respons))
                    return True
                except:
                    logger.exception("add data")
                    return False

            elif json_data["operation"] == "delete":
                try:
                    queries_with_tokens = add_tokens(tokenizer, queries)
                    urls_data = [(url, {"data": queries_with_tokens, "operation": "delete"}) for url in urls]
                    respons = pool.map(sender, urls_data)
                    pool.close()
                    pool.join()
                    logger.info(str(respons))
                    return True
                except:
                    logger.exception("delete")
                    return False

            elif json_data["operation"] == "update":

                try:
                    queries_with_tokens = add_tokens(tokenizer, queries)
                    urls_data = [(url, {"data": queries_with_tokens, "operation": "update"}) for url in urls]
                    respons = pool.map(sender, urls_data)
                    pool.close()
                    pool.join()
                    logger.info(str(respons))
                    return True
                except:
                    logger.exception("update")
                    return False
            elif json_data["operation"] == "search":
                try:
                    if "score" in json_data:
                        queries_with_tokens = add_tokens(tokenizer, queries)
                        urls_data = [(url, {"data": queries_with_tokens, "operation": "search",
                                            "score": json_data["score"]}) for url in urls]
                    else:
                        queries_with_tokens = add_tokens(tokenizer, queries)
                        urls_data = [(url, {"data": queries_with_tokens, "operation": "search"}) for url in urls]
                    respons = pool.map(sender, urls_data)
                    pool.close()
                    pool.join()
                    all_tuples, main_dict = result_aggregate(respons)
                    if all_tuples:
                        return resulting_report(queries_with_tokens, all_tuples, main_dict)
                    else:
                        return []
                except:
                    logger.exception("search")
                    return []
        else:
            return None


if __name__ == "__main__":
    serve(app, host="0.0.0.0", port=7000)
    # app.run(host='0.0.0.0', port=7000)
