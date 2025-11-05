import json
from pymongo import MongoClient

# Step 1: Read credentials.json (Make sure this file is in your working directory)
CREDENTIALS_FILE_PATH = r"C:\Users\JOBIAK\PycharmProjects\PythonProject\revenue\credentials.json"

with open(CREDENTIALS_FILE_PATH, 'r') as file:
    credentials = json.load(file)

# Step 2: Connect to MongoDB
REMOTE_MONGO_URI = "mongodb+srv://saidixit:nsaidixit@saidixit.hcevow5.mongodb.net/admin"
client = MongoClient(REMOTE_MONGO_URI)

# Step 3: Select the database and collection
google_creds_db = client["google_creds"]
google_creds_col = google_creds_db["creds"]

# Step 4: Create the document to insert
# Assuming you want to insert the entire credentials JSON as a field called "content"
credential_doc = {
    "name": "google_credentials",  # You can use any unique name for the document
    "content": credentials  # This will insert the full credentials JSON into the "content" field
}

# Step 5: Insert the document into MongoDB
google_creds_col.insert_one(credential_doc)

print("Credentials successfully inserted into MongoDB.")
