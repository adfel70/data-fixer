import ast
from collections import defaultdict

from bson import ObjectId

from drivers.mongo_driver import mongo_driver
from model.Movie import attr_list


def filter_attr(raw_docs):
    docs = defaultdict(dict)
    for doc in raw_docs:
        doc_id = doc["id"]
        for key in doc:
            if key in attr_list:
                docs[doc_id][key] = doc[key]
    return docs


def fix_doc_vals(coll_type, doc):
    is_valid = True
    for key in doc:
        if key == "_id" or (
                coll_type == "KEYWORDS" and key == "id" and isinstance(doc[key], int)):
            continue
        if not isinstance(doc[key], str):
            is_valid = False
            break
        if key == "title":
            continue
        try:
            doc[key] = ast.literal_eval(doc[key])
        except:
            is_valid = False
            break
    return is_valid


def fix_docs(docs, coll_type):
    fixed_docs = []
    bad_docs = []
    for doc_id in docs:  # todo ask google how todo it clean
        doc = docs[doc_id]
        is_valid = fix_doc_vals(coll_type, doc)
        if is_valid:
            fixed_docs.append(doc)
        else:
            bad_docs.append(doc)
    return fixed_docs, bad_docs


def insert_docs(end_coll, fixed_docs: list, bad_docs: list):
    if fixed_docs:
        end_coll.insert_many(fixed_docs)
    if bad_docs:
        mongo_driver.bad_docs_coll.insert_many(bad_docs)


def fix_db(body):
    obj_list = [ObjectId(doc_id) for doc_id in body[0]]
    coll_type = body[1]
    end_coll, src_coll = get_coll(coll_type)
    raw_docs = list(src_coll.find({"_id": {"$in": obj_list}}))
    docs = filter_attr(raw_docs)
    fixed_docs, bad_docs = fix_docs(docs, coll_type)
    insert_docs(end_coll, fixed_docs, bad_docs)


def get_coll(coll_type):
    if coll_type == "MOVIES":
        src_coll = mongo_driver.movie_details_coll_raw
        end_coll = mongo_driver.movie_details_coll
    else:
        src_coll = mongo_driver.movie_keywords_coll_raw
        end_coll = mongo_driver.movie_keywords_coll
    return end_coll, src_coll
