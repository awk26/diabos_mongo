import pymongo
from pymongo import MongoClient
from datetime import datetime, timedelta

# Replace with your MongoDB connection string
client = MongoClient("mongodb+srv://Diabos_read_only:Pass_123@production-cluster.1iv42.mongodb.net?")  

try:
    # Check if the connection was successful before proceeding
    client.server_info()  # This will raise an exception if the connection fails

    # Replace with your database and collection names
    db = client["diabos-production"]
    collection = db["portcalls"]

   

   

    six_months_ago = (datetime.utcnow() - timedelta(days=180)).isoformat() + 'Z'
    now = datetime.utcnow().isoformat() + 'Z'

    pipeline = [
    {
        "$addFields": {
            "monthYear": {
                "$substr": ["$createdOn", 0, 7]  # Extract "YYYY-MM"
            }
        }
    },
    {
        "$addFields": {
            "createdDate": {
                "$toDate": "$createdOn"
            }
        }
    },
    {
        "$addFields": {
            "sixMonthsAgo": {
                "$dateSubtract": {
                    "startDate": "$$NOW",
                    "unit": "month",
                    "amount": 6
                }
            }
        }
    },
    {
        "$match": {
            "$expr": {
                "$and": [
                    { "$gte": ["$createdDate", "$sixMonthsAgo"] },
                    { "$lte": ["$createdDate", "$$NOW"] }
                ]
            }
        }
    },
    {
        "$group": {
            "_id": "$monthYear",
            "count": { "$sum": 1 }
        }
    },
    {
        "$sort": { "_id": 1 }
    }
]


    # print(collection.find_one())
    results=list(collection.aggregate(pipeline))
    print(results)
    for result in results:
        print(result)

except pymongo.errors.ConnectionFailure as e:
    print(f"Could not connect to MongoDB: {e}")
except pymongo.errors.PyMongoError as e:
    print(f"An error occurred during the aggregation: {e}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
finally:
    client.close()