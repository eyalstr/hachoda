from pymongo import MongoClient
from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()
# MongoDB connection string from environment variables
mongo_connection_string = os.getenv("MONGO_CONNECTION_STRING", "")

def fetch_process_ids_by_case_id_sorted(case_id, mongo_connection=mongo_connection_string, db_name="CaseManagement", collection_name="Case"):
    """Fetch Process IDs from MongoDB for a given Case ID (_id), sorted by LastPublishDate."""
    print("Connecting to MongoDB...")
    process_list = []  # List to store tuples of (LastPublishDate, ProcessId)

    try:
        # Connect to MongoDB
        mongo_client = MongoClient(mongo_connection)
        db = mongo_client[db_name]
        collection = db[collection_name]

        # Fetch the case document by _id
        document = collection.find_one(
            {"_id": case_id},
            {"Requests.Processes.ProcessId": 1, "Requests.Processes.LastPublishDate": 1, "_id": 1}
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
                last_publish_date = process.get("LastPublishDate")
                if process_id and last_publish_date:
                    # Append tuple of (LastPublishDate, ProcessId)
                    process_list.append((last_publish_date, process_id))

        # Sort the list by LastPublishDate (ascending order)
        process_list.sort(key=lambda x: x[0])

        # Extract only ProcessId values from the sorted list
        sorted_process_ids = [process[1] for process in process_list]

        print(f"Sorted Process IDs for Case ID {case_id}: {sorted_process_ids}")
        return sorted_process_ids

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
    process_ids = fetch_process_ids_by_case_id_sorted(case_id)
    print(f"Fetched and Sorted Process IDs: {process_ids}")
