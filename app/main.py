import json
import base64
import logging
from threading import Lock
from datetime import datetime, timedelta

import boto3
import requests
from botocore.exceptions import ClientError
from flask import Flask, abort, jsonify, request
from flask_cors import CORS
from flask_marshmallow import Marshmallow
from blackfynn import Blackfynn
from app.config import Config

from blackfynn import Blackfynn
from app.process_kb_results import process_kb_results_recursive
# from pymongo import MongoClient
import schedule
import threading
import time
from timeit import default_timer as timer

app = Flask(__name__)
# set environment variable
app.config["ENV"] = Config.DEPLOY_ENV

CORS(app)
times = []


def schedule_check():
    while True:
        schedule.run_pending()
        time.sleep(1)

@app.route("/search/", defaults={'query': ''})
@app.route("/search/<query>")
def kb_search(query):
    try:
        response = requests.get(f'https://scicrunch.org/api/1/elastic/SPARC_Datasets_new/_search?q={query}&api_key={Config.KNOWLEDGEBASE_KEY}')
        return process_kb_results_recursive(response.json())
    except requests.exceptions.HTTPError as err:
        logging.error(err)
        return json.dumps({'error': err})

def heart_query():
    start = timer()
    resp = kb_search('heart')
    global times
    try:
        number_hits = json.loads(resp)['numberOfHits']
    except:
        number_hits = 'Error in query'
    times.append({'time run': time.strftime('%X %x %Z'), 'time elapsed': timer() - start, 'results': number_hits })


schedule.every().minute.do(heart_query)
heart_query()
x = threading.Thread(target=schedule_check, daemon=True)
x.start()

@app.errorhandler(404)
def resource_not_found(e):
    return jsonify(error=str(e)), 404



@app.before_first_request
def connect_to_blackfynn():
    global bf
    bf = Blackfynn(
        api_token=Config.BLACKFYNN_API_TOKEN,
        api_secret=Config.BLACKFYNN_API_SECRET,
        env_override=False,
    )


# @app.before_first_request
# def connect_to_mongodb():
#     global mongo
#     mongo = MongoClient(Config.MONGODB_URI)

@app.route("/scicrunch-test")
def scitest():
    global times
    return jsonify({"Tests": times}), 200


@app.route("/health")
def health():
    return json.dumps({"status": "healthy"})


# Download a file from S3
@app.route("/download")
def create_presigned_url(expiration=3600):
    bucket_name = "blackfynn-discover-use1"
    key = request.args.get("key")
    response = s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket_name, "Key": key, "RequestPayer": "requester"},
        ExpiresIn=expiration,
    )

    return response


# Reverse proxy for objects from S3, a simple get object
# operation. This is used by scaffoldvuer and its 
# important to keep the relative <path> for accessing
# other required files.
@app.route("/s3-resource/<path:path>")
def direct_download_url(path):
    bucket_name = "blackfynn-discover-use1"

    head_response = s3.head_object(
        Bucket=bucket_name,
        Key=path,
        RequestPayer="requester",
    )

    content_length = head_response.get('ContentLength', None)
    if content_length and content_length > 20971520:  # 20 MB
        return abort(413, description=f"File too big to download: {content_length}")

    response = s3.get_object(
        Bucket=bucket_name,
        Key=path,
        RequestPayer="requester",
    )
    resource = response["Body"].read()
    return resource




@app.route("/filter-search/", defaults={'query': ''})
@app.route("/filter-search/<query>/")
def filter_search(query):
    term = request.args.get('term')
    facet = request.args.get('facet')
    size = request.args.get('size')
    start = request.args.get('start')
    if size is None or start is None:
        size = 20
        start = 0
    print('term', term)
    print('facet', facet)
    type_map = {
        'species': ['organisms.subject.species.name', 'organisms.sample.species.name'],
        'gender': ['attributes.subject.sex.value', 'attributes.sample.sex.value'],
        'genotype': ['anatomy.organ.name.aggregate']
    }
    data = {
      "size": size,
      "from": start,
      "query": {
          "bool": {
              "must": [],
              "should": [],
              "filter":
                {
                    'term': {}
                }
          }
      }
    }
    results = []
    if term is not None and facet is not None:
        data['query']['bool']['filter']['term'] = {f'{type_map[term][0]}': f'{facet}'}
    else:
        data['query']['bool']['filter'] = []
    params = {}
    if query is not '':
        if term is None:
            params = {'q': query}
        else:
            data['query']['bool']['must'] = {
              "query_string": {
                "query": f"{query}",
                "default_operator": "and",
                "lenient": "true",
                "type": "best_fields"
              }
            }
    try:
        print(data)
        response = requests.get(
            f'https://scicrunch.org/api/1/elastic/SPARC_Datasets_new/_search?api_key={Config.KNOWLEDGEBASE_KEY}',
            params=params,
            json=data)
        results = process_kb_results_recursive(response.json())
    except requests.exceptions.HTTPError as err:
        logging.error(err)
        return json.dumps({'error': err})
    return results

@app.route("/get-facets/<type>")
def get_facets(type):
    type_map = {
        'species': ['organisms.subject.species.name.aggregate', 'organisms.sample.species.name.aggregate'],
        'gender': ['attributes.subject.sex.value'],
        'genotype': ['anatomy.organ.name.aggregate']
    }

    data = {
        "from": 0,
        "size": 0,
        "aggregations": {
            f"{type}": {
                "terms": {
                    "field": "",
                    "size": 200,
                    "order": [
                        {
                            "_count": "desc"
                        },
                        {
                            "_key": "asc"
                        }
                    ]
                }
            }
        }
    }
    results = []
    for path in type_map[type]:
        data['aggregations'][f'{type}']['terms']['field'] = path
        response = requests.get(
            f'https://scicrunch.org/api/1/elastic/SPARC_Datasets_new/_search?api_key={Config.KNOWLEDGEBASE_KEY}',
            json=data)
        results.append(response.json())

    terms = []
    for result in results:
        terms += result['aggregations'][f'{type}']['buckets']

    return json.dumps(terms)


@app.route("/banner/<dataset_id>")
def get_banner(dataset_id):
    try:
        params = {
            'includePublishedDataset': True,
            'api_key': bf._api.token
        }
        response = requests.get(f'https://api.blackfynn.io/datasets/{dataset_id}', params=params)
        discover_id = response.json()['publication']['publishedDataset']['id']
        response = requests.get(f'{Config.DISCOVER_API_HOST}/datasets/{discover_id}')
        return response.json()
    except requests.exceptions.HTTPError as err:
        logging.error(err)
        return json.dumps({'error': err})