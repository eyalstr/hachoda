from pymongo import MongoClient
from dotenv import load_dotenv
from pprint import pprint
from bidi.algorithm import get_display
import unicodedata
import os
from colorama import init, Fore, Style

# Initialize colorama for Windows console ANSI escape code support
init(autoreset=True)

# Load environment variables
load_dotenv()
mongo_connection_string = os.getenv("MONGO_CONNECTION_STRING", "")

# ANSI escape codes for color and formatting
BOLD_YELLOW = Style.BRIGHT + Fore.YELLOW
BOLD_GREEN = Style.BRIGHT + Fore.GREEN
BOLD_RED = Style.BRIGHT + Fore.RED
RESET = Style.RESET_ALL

def normalize_hebrew(text):
    """Normalize and format Hebrew text for proper RTL display."""
    if not text:
        return text
    return get_display(unicodedata.normalize("NFKC", text.strip()))

def display_document_with_highlights(doc):
    """
    Display document fields with special handling for Hebrew text in specific keys.
    Highlights certain fields with ANSI colors.
    """
    print("\n" + BOLD_RED + "Document Found:" + RESET)
    for key, value in doc.items():
        # Check for Hebrew text in FileName and apply normalization
        if key == "FileName" and isinstance(value, str):
            normalized_value = normalize_hebrew(value)
            print(f"{BOLD_YELLOW}{key}{RESET} = {BOLD_GREEN}{normalized_value}{RESET}")
        # Handle nested fields (optional formatting for clarity)
        elif isinstance(value, list) or isinstance(value, dict):
            print(f"{BOLD_YELLOW}{key}{RESET} =")
            pprint(value)  # Pretty print for nested values
        else:
            print(f"{BOLD_YELLOW}{key}{RESET} = {value}")

def fetch_documents_by_case_id(case_id, mongo_connection=mongo_connection_string, db_name="CaseManagement", collection_name="Document"):
    """
    Fetch all documents from MongoDB where Entities array contains an object 
    with EntityTypeId=1 and EntityValue=case_id.
    """
    print("Connecting to MongoDB...")
    try:
        # Connect to MongoDB
        mongo_client = MongoClient(mongo_connection)
        db = mongo_client[db_name]
        collection = db[collection_name]

        # Query to match documents with EntityTypeId=1 and EntityValue=case_id
        query = {
            "Entities": {
                "$elemMatch": {
                    "EntityTypeId": 1,
                    "EntityValue": case_id
                }
            }
        }

        # Fetch all matching documents
        print(f"Querying documents for EntityValue (case_id): {case_id}")
        documents = collection.find(query)

        matching_documents = list(documents)  # Convert cursor to list

        if not matching_documents:
            print(f"No documents found matching the case_id: {case_id}")
        else:
            print(f"\nFound {len(matching_documents)} matching documents:")
            for document in matching_documents:
                display_document_with_highlights(document)

        return matching_documents

    except Exception as e:
        print(f"Error querying MongoDB: {e}")
        return []

    finally:
        if 'mongo_client' in locals():
            mongo_client.close()
            print("MongoDB connection closed.")


# Main Execution
if __name__ == "__main__":
    try:
        # Prompt the user for input
        case_id = int(input("Enter Case ID (EntityValue): "))
        fetched_documents = fetch_documents_by_case_id(case_id)
        
        if fetched_documents:
            print("\nSuccessfully fetched documents.")
        else:
            print("\nNo matching documents found.")
    except ValueError:
        print("Invalid input. Please enter a numeric Case ID.")