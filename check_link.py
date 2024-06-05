import pymongo
import argparse
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor
from motor.motor_asyncio import AsyncIOMotorClient
import os

load_dotenv()

def process_link(link):
    if "https://www" in link:
        carLink = link
    else:
        carLink = "https://www.iaai.com" + link
        
    if collection.find_one({"carLink": carLink}):
        # print(f"Car {carLink} already in the database")
        # pass
    else:
        collection.insert_one({"carLink": carLink, "Info": "None"})
        # print(f"Car {carLink} added to the database with NONE")

# Create a new client
client = pymongo.MongoClient(os.getenv("MONGOAUTH"))
#client = AsyncIOMotorClient("mongodb+srv://usernone:exvijNFNWZvPNmwq@atlascluster.0cnf1lh.mongodb.net/")

# Get a reference to the database
db = client['PortalAuction']

# Get a reference to the collection
collection = db['Cars']

def process_links(carlinks, num_threads=100):
    # Create a ThreadPoolExecutor with the specified number of threads
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        # Submit tasks for each link to be processed in parallel
        executor.map(process_link, carlinks)

# Parse command-line arguments
parser = argparse.ArgumentParser(description='Process some car links.')
parser.add_argument('carlinks', type=str, help='a space-separated string of car links')
args = parser.parse_args()

# Split the string back into a list
carlinks = args.carlinks.split()

# Process the links
process_links(carlinks)
