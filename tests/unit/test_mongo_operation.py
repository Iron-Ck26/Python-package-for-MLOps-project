import pytest
from mongodb_connect.mongo_crud import MongoOperation

def test_mongo_operation_init():
    client_url = "mongodb+srv://Iron_Ck:55252555_Ck@mlops-mongodb-cluster1.etxtatl.mongodb.net/?retryWrites=true&w=majority&appName=MLOps-MongoDB-Cluster1"
    database_name = "mynewdatabase1"
    collection_name = "mynewcolumn1"
    
    mo = MongoOperation(client_url, database_name, collection_name)
    
    assert mo.client_url == client_url
    assert mo.database_name == database_name
    assert mo.collection_name == collection_name

def test_create_mongo_client():
    client_url = "mongodb+srv://Iron_Ck:55252555_Ck@mlops-mongodb-cluster1.etxtatl.mongodb.net/?retryWrites=true&w=majority&appName=MLOps-MongoDB-Cluster1"
    database_name = "mynewdatabase1"
    collection_name = "mynewcolumn1"
    
    mongo_op = MongoOperation(client_url, database_name, collection_name)
    client = mongo_op.create_mongo_client()
    
    assert client is not None
    # You might want to add more specific assertions about the client

# Add more tests as needed