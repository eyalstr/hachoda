from pymongo import MongoClient
from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()

# MongoDB connection string
mongo_connection_string = (
    "mongodb://eCourtUser:eCourtUserQA@10.100.216.21:27017,"
    "10.100.216.25:27017,10.100.216.27:27017/CaseManagement"
    "?retryWrites=true&loadBalanced=false&readPreference=secondary"
    "&connectTimeoutMS=10000&authSource=CaseManagement&authMechanism=SCRAM-SHA-1"
)

def fetch_process_ids_by_case_id(case_id, mongo_connection=mongo_connection_string, db_name="CaseManagement", collection_name="Case"):
    """Fetch Process IDs from MongoDB for a given Case ID (_id)."""
    print("Connecting to MongoDB...")
    process_ids = set()  # Use a set to ensure unique Process IDs

    try:
        # Connect to MongoDB
        mongo_client = MongoClient(mongo_connection)
        db = mongo_client[db_name]
        collection = db[collection_name]

        # Fetch the case document by _id
        document = collection.find_one(
            {"_id": case_id},
            {"Requests.Processes.ProcessId": 1, "_id": 1}  # Fetch only the relevant fields
        )

        if not document:
            print(f"No document found for Case ID {case_id}.")
            return []

        print(f"Found document for Case ID {case_id}.")

        # Iterate over all Requests[].Processes[]
        requests = document.get("Requests", [])
        for request in requests:
            processes = request.get("Processes", [])
            for process in processes:
                process_id = process.get("ProcessId")
                if process_id:
                    process_ids.add(process_id)  # Add ProcessId to the set

        # Convert set to list for output
        process_ids = list(process_ids)
        print(f"Extracted Process IDs for Case ID {case_id}: {process_ids}")
        return process_ids

    except Exception as e:
        print(f"Error querying MongoDB: {e}")
        return []

    finally:
        if 'mongo_client' in locals():
            mongo_client.close()
            print("MongoDB connection closed.")

# Test Execution
if __name__ == "__main__":
    case_id = int(input("Enter Case ID (_id): "))
    process_ids = fetch_process_ids_by_case_id(case_id)
    print(f"Fetched Process IDs: {process_ids}")
