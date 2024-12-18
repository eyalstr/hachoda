from pymongo import MongoClient
from dotenv import load_dotenv
from pprint import pprint
from bidi.algorithm import get_display
import unicodedata
import os
from colorama import init, Fore, Style
from document_type_mapping import DOCUMENT_TYPE_MAPPING  # Import the mapping table
from document_category_mapping import DOCUMENT_CATEGORY_MAPPING

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

def display_document_with_highlights(doc, number):
    """
    Display document fields with numbering and special handling for Hebrew text in specific keys.
    Highlights certain fields with ANSI colors and applies proper RTL normalization.
    """
    print(f"\n{BOLD_RED}Document #{number} Found:{RESET}")
    for key, value in doc.items():
        # Special case for DocumentTypeId: Lookup description and normalize Hebrew
        if key == "DocumentTypeId" and isinstance(value, int):
            description = DOCUMENT_TYPE_MAPPING.get(value, f"Unknown ({value})")
            print(f"{BOLD_YELLOW}{key}{RESET} = {BOLD_GREEN}{description}({value}){RESET}")

        # Special case for DocumentCategoryId
        elif key == "DocumentCategoryId" and isinstance(value, int):
            description = DOCUMENT_CATEGORY_MAPPING.get(value, f"Unknown")
            print(f"{BOLD_YELLOW}{key}{RESET} = {BOLD_GREEN}{description}({value}){RESET}")

        # Check for Hebrew text in FileName
        elif key == "FileName" and isinstance(value, str):
            normalized_value = normalize_hebrew(value)
            print(f"{BOLD_YELLOW}{key}{RESET} = {BOLD_GREEN}{normalized_value}{RESET}")

        # Handle nested fields (optional formatting for clarity)
        elif isinstance(value, list) or isinstance(value, dict):
            print(f"{BOLD_YELLOW}{key}{RESET} =")
            pprint(value)
        else:
            print(f"{BOLD_YELLOW}{key}{RESET} = {value}")

def fetch_documents_by_case_id(case_id, mongo_connection=mongo_connection_string, db_name="CaseManagement", collection_name="Document"):
    """
    Fetch all documents from MongoDB where Entities array contains an object 
    with EntityTypeId=1 and EntityValue=case_id. Results are sorted by DocumentReceiptTime in descending order.
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

        # Fetch all matching documents, sorted by DocumentReceiptTime in descending order
        print(f"Querying documents for EntityValue (case_id): {case_id}")
        documents = collection.find(query).sort("DocumentReceiptTime", -1)  # Sort descending

        matching_documents = list(documents)  # Convert cursor to list

        if not matching_documents:
            print(f"No documents found matching the case_id: {case_id}")
        else:
            print(f"\nFound {len(matching_documents)} matching documents:")
            for index, document in enumerate(matching_documents, start=1):  # Add numbering
                display_document_with_highlights(document, index)

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
