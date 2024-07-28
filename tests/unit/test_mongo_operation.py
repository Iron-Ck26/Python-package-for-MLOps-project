import pytest
from mongodb_connect.mongo_crud import MongoOperation

# It's better to load these from environment variables or a secure config file
client_url = "mongodb+srv://Iron_Ck:55252555_Ck@mlops-mongodb-cluster1.etxtatl.mongodb.net/?retryWrites=true&w=majority&appName=MLOps-MongoDB-Cluster1"
database = "mynewdatabase"
collection_name = "mynewcolumn"

# Create an instance of MongoOperation
mongo_op = MongoOperation(client_url, database, collection_name)

# Now you can use mongo_op to perform operations on your MongoDB