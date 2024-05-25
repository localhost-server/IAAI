import pymongo
from dotenv import load_dotenv
import os

load_dotenv()

# Create a MongoClient
client = pymongo.MongoClient(os.getenv("MONGOAUTH"))

# Select the database
db = client["PortalAuction"]

# Run the aggregation pipeline
pipeline = [
    {
        "$lookup": {
            "from": "Cars",
            "localField": "carLink",
            "foreignField": "carLink",
            "as": "Integrated"
        }
    },
    {
        "$unwind": "$Integrated"
    },
    {
        "$project": {
            "_id": 0,
            "carLink": 1,
            "price": 1,
            "Info": "$Integrated.Info"
        }
    },
    {
        "$out": "IntegratedData"
    }
]

db["CarsPrice"].aggregate(pipeline)
