import requests
import time
import os
import schedule

# Configuration
BOT_TOKEN = "8309028785:AAHGUIPKNNB9xOa0VMrzkyAZForGr8vDQ-I"
CHANNEL_ID = "-1002775048629"
FILE_PATH = "/home/co/uploader.json"

def upload_file():
    try:
        # Check if file exists
        if not os.path.exists(FILE_PATH):
            print(f"Error: File {FILE_PATH} not found")
            return
        
        # API URL for sending document
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument"
        
        # Prepare the file for upload
        files = {'document': open(FILE_PATH, 'rb')}
        data = {'chat_id': CHANNEL_ID, 'caption': f"uploader.json - {time.strftime('%Y-%m-%d %H:%M:%S')}"}
        
        # Send the request
        response = requests.post(url, files=files, data=data)
        
        # Check response
        if response.status_code == 200:
            print(f"File uploaded successfully at {time.strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            print(f"Failed to upload file. Status code: {response.status_code}")
            print(f"Response: {response.text}")
    
    except Exception as e:
        print(f"Error uploading file: {str(e)}")

# Schedule the task to run every 5 minutes
schedule.every(5).minutes.do(upload_file)

# Run the task immediately once
upload_file()

# Keep the script running
while True:
    schedule.run_pending()
    time.sleep(1)
