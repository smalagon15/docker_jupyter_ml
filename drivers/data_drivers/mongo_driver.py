from pymongo import MongoClient
import pandas as pd

# Database Names
MODEL = 'Model'
DATA = 'Data'

# Mongo Client Information
CONTAINER_NAME = 'ml_mongo'
PORT = 27017

# This class handles all data persistence and retrieval 
class MongoDriver:
    def __init__(self):
        # ml_mongo is the name of the mongo container and 27017 is the default port 
        # this connects to mongodb://ml_mongo:27017/
        self.client = MongoClient(CONTAINER_NAME, PORT)

    # These two methods insert and retrive dataframs from the Data DB
    def insert_data(self, collection, data_frame):
        df = data_frame.to_dict(orient="records")
        self.insert_collection(DATA, collection, df, "subject_id")
    def get_data_frame(self, collection):
        data =self.get_collection(DATA, collection)
        mongo_df = pd.DataFrame.from_records(data)
        return mongo_df

    # These two methods insert and retrive model metas from the Model DB
    def insert_model_meta(self, collection, model_meta, override_field='model_type'):
        self.insert_collection(MODEL, collection, [model_meta], override_field)

    def get_model_meta(self, collection, find={}, exclude ={}):
        data = self.get_collection(MODEL, collection, find, exclude)
        return data

    def get_collections(self):
        model_collections = self.client[MODEL].list_collection_names()
        data_collections = self.client[DATA].list_collection_names()
        return {'model_collections':model_collections, 'data_collections':data_collections}


    # A collection is the Mongo equivilant of a table
    # This uses the name of a collection to grab values
    # database = String which database to look in
    # collection = String which collection to look in
    def get_collection(self, database, collection, find = {}, exclude = {}):
        exclude['_id']=0
        db = self.client[database]
        table = list(db[collection].find(find, exclude))
        return table

    # A collection is the Mongo equivilant of a table
    # This points to a collection and inserst one or many data points
    # database = String which database to look in
    # collection = String which collection to look in
    # data = array of objects to insert into the collection.
    def insert_collection(self, database, collection, data, override_field):
        db = self.client[database]
        table = db[collection]
        result = []
        for insert in data:
            filter = { override_field : insert[override_field] }
            upsert =  True 
            push = { "$set": insert}
            update = table.update_one(filter, push, upsert)
            result.append(update)
        return result