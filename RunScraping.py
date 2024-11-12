import pymongo
import subprocess
import time
import pytz
import os
from dotenv import load_dotenv
load_dotenv()

# Create a new client
client = pymongo.MongoClient(os.getenv("MONGOAUTH"))

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
    print("Current Time =", time_string)

    # Get the day of the week
    day_of_week = now.strftime("%A")

    if time_string<="16:00" and time_string>="07:00" and (day_of_week in weekdays):
        print("Time is less than 16:00")
        time.sleep(3600) 
        continue

    # print("Current Time =", time_string)
    # print("Day of the Week =", day_of_week)
    
    # Checking the count of Cars with None Info
    count = collection.count_documents({"Info": "None"})
    # if count>=500:
    #     print("Found Cars with None Info In Database \nStarting scraping them")
    #     process = subprocess.Popen(["python3", "ProductScraping.py"])
    #     process.wait()  # Wait for the process to complete
    #     del process  # Delete the process
    
    if count :#and (time_string>="16:00"):# and (day_of_week in weekdays)):
        print("Time to run the script")
        # if not process:
        process = subprocess.Popen(["python3", "ProductScraping.py"])
        time.sleep(300)
        process1 = subprocess.Popen(["python3", "ProductScraping.py"])
        time.sleep(300)
        process2 = subprocess.Popen(["python3", "ProductScraping.py"])
        process.wait()
        process1.wait()
        process2.wait()
        process.terminate()      
        process1.terminate()     
        process2.terminate()     
        process.communicate()    
        process1.communicate()   
        process2.communicate()   


        del process
        del process1
        del process2

        # Aggregation
        aggregation_process = subprocess.Popen(["python3", "aggregation.py"])
        aggregation_process.wait()
        aggregation_process.terminate()
        aggregation_process.communicate()
        del aggregation_process

        break

    # time.sleep(3600)  # Sleep for an hour before checking again
