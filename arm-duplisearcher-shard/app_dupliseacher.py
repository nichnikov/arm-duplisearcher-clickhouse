from flask import Flask, jsonify, request
from flask_restplus import Api, Resource, fields
from texts_processing import QueriesVectors
from seacher import Main
from waitress import serve
import logging


logger = logging.getLogger("app_duplisearcher")
logger.setLevel(logging.DEBUG)


app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False
api = Api(app)

name_space = api.namespace('api', 'На вход поступает JSON, возвращает JSON')

query = name_space.model("One Query",
                         {"id": fields.String(description="query's Id", required=True),
                          "cluster": fields.String(description="query's text", required=True)})

input_data = name_space.model("Input JSONs",
                              {"score": fields.Float(description="The similarity coefficient", required=True),
                               "data": fields.List(fields.Nested(query)),
                               "operation": fields.String(description="add/update/delete/search/del_all",
                                                          required=True)})

main = Main()
"""max size of queries_matrix"""

vectorizer = QueriesVectors(32000)

@name_space.route('/')
class CollectionHandling(Resource):
    """Service searches duplicates and adding and delete data in collection."""

    @name_space.expect(input_data)
    def post(self):
        """POST method on input JSON file with scores, operation type and lists of fast answers."""
        json_data = request.json
        if json_data["data"]:

            if "operation" in json_data:
                if json_data["operation"] == "add":
                    q_i, a_i, m_i, cls, p_i, tkns = zip(*json_data["data"])
                    main.add(list(zip(q_i, vectorizer(tkns))), json_data["data"])
                    return jsonify({"quantity": main.main_searcher.matrix.shape[0]})

                elif json_data["operation"] == "delete":
                    q_i, a_i, m_i, cls, p_i, tkns = zip(*json_data["data"])
                    main.answers_delete(list(set(a_i)))
                    return jsonify({"quantity": main.main_searcher.matrix.shape[0]})

                elif json_data["operation"] == "update":
                    q_i, a_i, m_i, cls, p_i, tkns = zip(*json_data["data"])
                    main.update(a_i, list(zip(q_i, vectorizer(tkns))), json_data["data"])
                    return jsonify({"quantity": main.main_searcher.matrix.shape[0]})

                elif json_data["operation"] == "search":
                    q_i, a_i, m_i, cls, p_i, tkns = zip(*json_data["data"])
                    if "score" in json_data:
                        search_results = main.search(list(zip(q_i, vectorizer(tkns))), json_data["score"])
                    else:
                        search_results = main.search(list(zip(q_i, vectorizer(tkns))))
                    return jsonify(search_results)


if __name__ == "__main__":
    serve(app, host="0.0.0.0", port=8080)
    # app.run(host='0.0.0.0', port=8080)
