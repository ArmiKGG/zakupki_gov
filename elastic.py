import os
import uuid
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
import urllib
import pandas as pd
load_dotenv()

def gen_query(source_id):
    return {
        "bool": {
            "must": [
                {
                    "match": {
                        "sourceID": source_id
                    }
                }
            ]
        }
    }


def connect_elasticsearch():
    _es = None
    _es = Elasticsearch(
        ["https://c-c9qbvn9fqt1e60l616u1.rw.mdb.yandexcloud.net:9200"],
        basic_auth=(os.getenv("ES_USER"), os.getenv("ES_PASS")),
        verify_certs=False
    )
    if _es.ping():
        print("ES connected")
    else:
        print("Could not connect to ES!")
    return _es


def is_exists(es, source_id):
    resp = es.search(index="products", query=gen_query(source_id))
    if resp["hits"]["total"]["value"] == 0:
        return False
    else:
        return True


def insert_org(es, org_id, org_name="Unknown"):
    response = es.index(
        index='zakupki_ids',
        id=uuid.uuid4(),
        body={"org_id": org_id,
              "name": org_name}
    )
    return response


def insert_product(es, body):
    response = es.index(
        index='products',
        id=uuid.uuid4(),
        body=body
    )
    return response


def update_org(es, org_num, number, org_id):
    es_id = es.search(index="zakupki_ids", query={
        "bool": {
            "must": [
                {
                    "match": {
                        "org_id": org_id
                    }
                }
            ]
        }
    })["hits"]["hits"][0]["_id"]
    response = es.update(index='zakupki_ids', id=es_id,
                         body={"doc": {"statistics": {f"{org_num}": number}}})

    return response


def match_all_orgs(es):
    all_org = es.search(index="zakupki_ids", query={"match_all": {}}, scroll="10h", size=10000)
    return all_org