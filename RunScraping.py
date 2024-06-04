import pymongo
import subprocess
import time
import pytz

# Create a new client
client = pymongo.MongoClient("mongodb+srv://usernone:exvijNFNWZvPNmwq@atlascluster.0cnf1lh.mongodb.net/")

# Get a reference to the database
db = client['PortalAuction']

# Get a reference to the collection
collection = db['Cars']

from datetime import datetime

weekdays = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
cdt=pytz.timezone('America/Chicago')

process=None

while True:

    # Get the current time
    now = datetime.now(cdt)

    # Format the time to hours and minutes
    time_string = now.strftime("%H:%M")

    # Get the day of the week
    day_of_week = now.strftime("%A")

    # print("Current Time =", time_string)
    # print("Day of the Week =", day_of_week)
    
    # Checking the count of Cars with None Info
    count = collection.count_documents({"Info": "None"})
    # if count>=500:
    #     print("Found Cars with None Info In Database \nStarting scraping them")
    #     process = subprocess.Popen(["python3", "ProductScraping.py"])
    #     process.wait()  # Wait for the process to complete
    #     del process  # Delete the process
    
    if count and (time_string>="14:00" and (day_of_week in weekdays)):
        print("Time to run the script")
        # if not process:
        process = subprocess.Popen(["python3", "ProductScraping.py"])
        process.wait()
        process.terminate()  # Terminate the process
        process.communicate()  # Cleanup the process

        del process

        # Aggregation
        aggregation_process = subprocess.Popen(["python3", "aggregation.py"])
        aggregation_process.wait()
        aggregation_process.terminate()
        aggregation_process.communicate()

        del aggregation_process

    time.sleep(3600)  # Sleep for an hour before checking again
