from datetime import datetime
import subprocess
import time
import pytz

# Define the weekdays on which to run the scripts
weekdays = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
# Setting CDT timezone
cdt=pytz.timezone('America/Chicago')

# Main loop
while True:
    # Get the current time
    now = datetime.now(cdt)

    # Get the day of the week
    day_of_week = now.strftime("%A")
    nowtime = now.strftime('%H:%M')

    # Check if it's a weekday
    if day_of_week in weekdays and nowtime <= "14:00":
        # Print time in hours minutes
        print(f'Today is Auction - Day of the week: {day_of_week} - Time: {nowtime}')
        # Check if it's time to run the scripts
        if nowtime >= "07:45" :
            print('Checking for scripts execution...')

            # Initialize process flags
            process_initialized = False

            try:
                
                # If processes are not initialized for the current day, initialize them
                if not process_initialized:
                    print("Initializing processes for the day...")
                    process1 = subprocess.Popen(["python3", "AuctionLinksScraping.py"])
                    
                    # DB Merging
                    merging_process = subprocess.Popen(["python3", "mergedDB.py"])
                    merging_process.wait()
                    merging_process.terminate()
                    merging_process.communicate()
                    del merging_process
        
                    time.sleep(300)  # Wait for 5 minutes before initializing the next process
                    # process2 = subprocess.Popen(["python3", "RunAuctions.py"])
                    # time.sleep(300)  # Wait for 5 minutes before initializing the next process
                    # process3 = subprocess.Popen(["python3", "RunScraping.py"])
                    process_initialized = True
                
                    # Wait for each process to complete
                    process1.wait()
                    # process2.wait()
                    # process3.wait()
                    
                    # Terminate the processes to clean up resources
                    process1.terminate()
                    # process2.terminate()
                    # process3.terminate()
                    
                    # Wait for the processes to terminate completely
                    process1.communicate()
                    # process2.communicate()
                    # process3.communicate()
                
        
            except Exception as e:
                print(f"Error: {e}")
                # Terminate the processes if an error occurs
                if process1:
                    process1.terminate()
                    process1.communicate()
               
                # if process3:
                #     process3.terminate()
                #     process3.communicate()
        else:
            # Sleep for the remaining part of the hour
            time.sleep(3600 - now.minute * 60)
    else:
        # Reset the process flag for the next day
        process_initialized = False
        
        # Sleep for 6 hours before checking again
        time.sleep(3600 * 6)
