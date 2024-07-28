"""
from typing import Any
import os
import pandas as pd
import pymongo
import json
from ensure import ensure_annotations


import os
import pandas as pd
from pymongo.mongo_client import MongoClient
from ensure import ensure_annotations


class mongo_operation:
    __collection=None # here i have created a private/protected variable
    __database=None
    
    def __init__(self,client_url: str, database_name: str, collection_name: str=None):
        self.client_url=client_url
        self.database_name=database_name
        self.collection_name=collection_name
       
    def create_mongo_client(self,collection=None):
        client=MongoClient(self.client_url)
        return client
    
    def create_database(self,collection=None):
        if mongo_operation.__database==None:
            client=self.create_mongo_client(collection)
            self.database=client[self.database_name]
        return self.database 
    
    def create_collection(self,collection=None):
        if mongo_operation.__collection==None:
            database=self.create_database(collection)
            self.collection=database[self.collection_name]
            mongo_operation.__collection=collection
        
        if mongo_operation.__collection!=collection:
            database=self.create_database(collection)
            self.collection=database[self.collection_name]
            mongo_operation.__collection=collection
            
        return self.collection
    
    def insert_record(self,record: dict, collection_name: str) -> Any:
        if type(record) == list:
            for data in record:
                if type(data) != dict:
                    raise TypeError("record must be in the dict")    
            collection=self.create_collection(collection_name)
            collection.insert_many(record)
        elif type(record)==dict:
            collection=self.create_collection(collection_name)
            collection.insert_one(record)
    
    def bulk_insert(self,datafile,collection_name:str=None):
        self.path=datafile
        
        if self.path.endswith('.csv'):
            pd.read.csv(self.path,encoding='utf-8')
            
        elif self.path.endswith(".xlsx"):
            dataframe=pd.read_excel(self.path,encoding='utf-8')
            
        datajson=json.loads(dataframe.to_json(orient='record'))
        collection=self.create_collection()
        collection.insert_many(datajson)
        """
        
from typing import Any
import pandas as pd
import json
from pymongo.mongo_client import MongoClient
from ensure import ensure_annotations


class MongoOperation:
    __collection = None  # here I have created a private/protected variable
    __database = None
    
    def __init__(self, client_url: str, database_name: str, collection_name: str = None):
        self.client_url = client_url
        self.database_name = database_name
        self.collection_name = collection_name

    def create_mongo_client(self):
        client = MongoClient(self.client_url)
        return client

    def create_database(self):
        if MongoOperation.__database is None:
            client = self.create_mongo_client()
            self.database = client[self.database_name]
            MongoOperation.__database = self.database
        return self.database

    def create_collection(self, collection=None):
        if MongoOperation.__collection is None:
            database = self.create_database()
            self.collection = database[self.collection_name]
            MongoOperation.__collection = self.collection

        if MongoOperation.__collection != collection:
            database = self.create_database()
            self.collection = database[self.collection_name]
            MongoOperation.__collection = collection

        return self.collection

    def insert_record(self, record: dict, collection_name: str) -> Any:
        if isinstance(record, list):
            for data in record:
                if not isinstance(data, dict):
                    raise TypeError("record must be in dict format")    
            collection = self.create_collection(collection_name)
            collection.insert_many(record)
        elif isinstance(record, dict):
            collection = self.create_collection(collection_name)
            collection.insert_one(record)

    def bulk_insert(self, datafile, collection_name: str = None):
        self.path = datafile
        
        if self.path.endswith('.csv'):
            dataframe = pd.read_csv(self.path, encoding='utf-8')
        elif self.path.endswith('.xlsx'):
            dataframe = pd.read_excel(self.path, encoding='utf-8')
        else:
            raise ValueError("File must be a .csv or .xlsx")

        datajson = json.loads(dataframe.to_json(orient='records'))
        collection = self.create_collection(collection_name)
        collection.insert_many(datajson)


# MongoDB connection information
client_url = "mongodb+srv://Iron_Ck:55252555_Ck@mlops-mongodb-cluster1.etxtatl.mongodb.net/?retryWrites=true&w=majority&appName=MLOps-MongoDB-Cluster1"
database = "mynewdatabase"
collection_name = "mynewcolumn"
