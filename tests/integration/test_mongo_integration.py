import pytest
from mongodb_connect.mongo_crud import MongoOperation

def test_mongo_connection():
    # Replace with your actual MongoDB connection string
    client_url = "mongodb+srv://Iron_Ck:55252555_Ck@mlops-mongodb-cluster1.etxtatl.mongodb.net/?retryWrites=true&w=majority&appName=MLOps-MongoDB-Cluster1"
    database_name = "mynewdatabase1"
    collection_name = "mynewcolumn1"
    
    mongo_op = MongoOperation(client_url, database_name, collection_name)
    client = mongo_op.create_mongo_client()
    
    # Test if we can list database names (this operation doesn't require auth)
    assert client.list_database_names() is not None

# Add more integration tests as needed