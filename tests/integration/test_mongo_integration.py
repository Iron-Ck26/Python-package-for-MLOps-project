import pytest
from mongodb_connect.mongo_crud import MongoOperation
from pymongo.errors import ConnectionFailure

def test_mongo_connection():
    # Replace with your actual MongoDB connection string
    client_url = "mongodb+srv://Iron_Ck:55252555_Ck@mlops-mongodb-cluster1.etxtatl.mongodb.net/?retryWrites=true&w=majority&appName=MLOps-MongoDB-Cluster1"
    database_name = "mynewdatabase1"
    collection_name = "mynewcolumn1"
    
    mongo_op = MongoOperation(client_url, database_name, collection_name)
    client = mongo_op.create_mongo_client()
    
    try:
        # The ismaster command is cheap and does not require auth.
        client.admin.command('ismaster')
        assert True  # If we get here, the connection was successful
    except ConnectionFailure:
        pytest.fail("Server not available")

# Add more integration tests as needed
