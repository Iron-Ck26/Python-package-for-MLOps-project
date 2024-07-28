from typing import Any
import os
import pandas as pd
from pymongo.mongo_client import MongoClient
import json
from ensure import ensure_annotations

class mongo_operation:      # class name - mongo_operation
    __collection = None  # Here I have created a private/protected variable
    __database = None
    
    def __init__(self, client_url: str, database_name: str, collection_name: str = None):
        self.client_url = client_url
        self.database_name = database_name
        self.collection_name = collection_name
       
    def create_mongo_client(self):
        client = MongoClient(self.client_url)
        return client
    
    def create_database(self):
        if mongo_operation.__database is None:
            client = self.create_mongo_client()
            mongo_operation.__database = client[self.database_name]
        return mongo_operation.__database
    
    def create_collection(self, collection_name=None):
        if collection_name is None:
            collection_name = self.collection_name
        if mongo_operation.__collection is None or mongo_operation.__collection.name != collection_name:
            database = self.create_database()
            mongo_operation.__collection = database[collection_name]
        return mongo_operation.__collection
    
    def insert_record(self, record: dict, collection_name: str = None) -> Any:
        collection = self.create_collection(collection_name)
        if isinstance(record, list):
            for data in record:
                if not isinstance(data, dict):
                    raise TypeError("All records in the list must be dictionaries")
            result = collection.insert_many(record)
        elif isinstance(record, dict):
            result = collection.insert_one(record)
        else:
            raise TypeError("Record must be a dictionary or list of dictionaries")
        return result
    
    def bulk_insert(self, datafile: str, collection_name: str = None):
        if not os.path.isfile(datafile):
            raise FileNotFoundError(f"No such file: '{datafile}'")
        
        if datafile.endswith('.csv'):
            dataframe = pd.read_csv(datafile, encoding='utf-8')
        elif datafile.endswith(".xlsx"):
            dataframe = pd.read_excel(datafile, encoding='utf-8')
        else:
            raise ValueError("Unsupported file format. Please provide a '.csv' or '.xlsx' file.")
            
        datajson = json.loads(dataframe.to_json(orient='records'))
        collection = self.create_collection(collection_name)
        result = collection.insert_many(datajson)
        return result
