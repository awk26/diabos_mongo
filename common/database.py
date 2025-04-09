
import os
import sys
from pymongo import MongoClient
from dotenv import load_dotenv
# from common.logs import log
# Load environment variables from .env file
load_dotenv()

class Database:
    __client = None
    __db = None

    def __init__(self):
        try:
           
            mongo_db = os.getenv("MONGO_DB")
            mongo_uri = os.getenv("MONGO_URL")
            Database.__client = MongoClient(mongo_uri)
            Database.__db = Database.__client[mongo_db]
            print("MongoDB connection established.")
        except Exception as e:
            print(f"Error initializing MongoDB connection: {e}")
            sys.exit(1)

    def get_collection(self, collection_name):
        try:
            return Database.__db[collection_name]
        except Exception as e:
            print(f"Error getting collection: {e}")
            return None

    def find(self, collection_name, query={}, projection=None):
        try:
            collection = self.get_collection(collection_name)
            results = list(collection.find(query, projection))
            print(f"Query result: {results}")
            return results
        except Exception as e:
            print(f"Exception in find(): {e}")
            return []

    def find_one(self, collection_name, query={}, projection=None):
        try:
            collection = self.get_collection(collection_name)
            result = collection.find_one(query, projection)
            print(f"Single query result: {result}")
            return result
        except Exception as e:
            print(f"Exception in find_one(): {e}")
            return None

if __name__ == '__main__':
    db=Database()
    data=db.find_one("portcalls")
    print(data)