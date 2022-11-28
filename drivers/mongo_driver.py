import pymongo


class MongoDriver:
    def __init__(self):
        myclient = pymongo.MongoClient("mongodb://mongo:27017/")
        mydb = myclient["movies"]
        self.movie_details_coll = mydb["movie_details"]
        self.movie_details_coll.drop()
        self.movie_details_coll_raw = mydb["movie_details_raw"]
        self.movie_keywords_coll = mydb["movie_keywords"]
        self.movie_keywords_coll.drop()
        self.movie_keywords_coll_raw = mydb["movie_keywords_raw"]
        self.bad_docs_coll = mydb["bad_docs"]
        self.bad_docs_coll.drop()


mongo_driver = MongoDriver()
