from flask import Flask, request, redirect, jsonify, abort, make_response, url_for
import os
import json
from bson import json_util
from bson.objectid import ObjectId
import pymongo


def connect(database_name, collection_name):

    try:
        connection = pymongo.MongoClient('localhost', 27017)
    except pymongo.errors.ConnectionFailure, e:
        mongoerror = "Could not connect to MongoDB: "+ e
        print mongoerror
        return make_response(jsonify({'error': mongoerror}), 402)
    db = connection[database_name]  # database name
    collection = db[collection_name]  # collection name
    return collection


def tojson(data):
    # type: (dictonary) -> json
    # Convert Mongo object(s) to JSON
    return json.dumps(data, default=json_util.default)


# Main Program starts here
app = Flask(__name__)
# app = Flask(__name__, static_url_path = "")

menu_items = connect("restbiz", "menu_items")  # database name, collection name


@app.after_request
def after_request(response):
  response.headers.add('Access-Control-Allow-Origin', 'http://localhost:3000')
  response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
  response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE')
  return response


@app.route('/menus/', methods=['GET'])
@app.route('/menus/', methods=['POST'])
@app.route('/menus/', methods=['DELETE'])
def menus():
    """Return a list of all menu items
    ex) GET /menus/?limit=10&offset=0
    """

    if request.method == 'GET':
        lim = int(request.args.get('limit', 10))
        off = int(request.args.get('offset',0))
        results = menu_items.find().skip(off).limit(lim)
        json_results = []
        for result in results:
            json_results.append(result)
        return tojson(json_results)

    if request.method == 'POST':
        menu_id = menu_items.insert(request.get_json())
        result = menu_items.find_one({'_id': ObjectId(menu_id)})
        return tojson(result)

    if request.method == 'DELETE':
        rowcount = menu_items.count()
        menu_items.drop()
        return jsonify({'Rows Deleted': rowcount})


@app.route('/menus/<menu_id>', methods=['GET'])
@app.route('/menus/<menu_id>', methods=['PUT'])
@app.route('/menus/<menu_id>', methods=['DELETE'])
def menu(menu_id):
    """Return specific menu_id
    ex) GET /menus/123456
    """
    if request.method == 'GET':
        results = menu_items.find({'_id': ObjectId(menu_id)})
        json_results = []
        for result in results:
            json_results.append(result)
        return tojson(json_results)

    if request.method == 'PUT':
        newbody = request.get_json()
        rowcount = menu_items.update({'_id': ObjectId(menu_id)}, {"$set": newbody}, upsert=False)
        return jsonify({'Rows Updated': rowcount})

    if request.method == 'DELETE':
        rowcount = menu_items.remove({'_id': ObjectId(menu_id)})
        return jsonify({'Rows Deleted': rowcount})


@app.errorhandler(400)
def not_found(error):
    return make_response(jsonify({'error': 'Bad request'}), 400)


@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)



# Remove the "debug=True" for production
if __name__ == '__main__':
    # Bind to PORT if defined, otherwise default to 5000.
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)