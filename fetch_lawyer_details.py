import requests
from requests_ntlm import HttpNtlmAuth
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed

# Function to normalize Hebrew text for RTL display
def normalize_hebrew_text(text):
    if text:
        return unicodedata.normalize("NFKC", text)
    return text

# Configuration
base_url = "http://esbcaqa:8080/RepositoriesLawyers/GetLawyer"
headers = {
    "Moj-Application-Id": "MOJ-VALUE",  # Replace with actual value
    "Content-Type": "application/json",
}
username = "eyalst"
password = "subaruB40!"

# Target first and last names (normalized)
target_first_name = normalize_hebrew_text("הראל")
target_last_name = normalize_hebrew_text("מונדני")

# Function to process a single license
def process_license(license_number):
    url = f"{base_url}?license={license_number}"
    try:
        response = requests.get(url, headers=headers, auth=HttpNtlmAuth(username, password))
        if response.status_code == 200:
            data = response.json()
            lawyer_data = data.get("data", {}).get("data", {})
            if lawyer_data:
                first_name = normalize_hebrew_text(lawyer_data.get("firstName"))
                last_name = normalize_hebrew_text(lawyer_data.get("lastName"))
                if first_name == target_first_name and last_name == target_last_name:
                    return license_number, lawyer_data
    except Exception as e:
        print(f"Exception for License {license_number}: {e}")
    return None, None

# Parallel execution using ThreadPoolExecutor
def find_matching_lawyer(start, end):
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(process_license, license_number): license_number for license_number in range(start, end + 1)}
        for future in as_completed(futures):
            license_number = futures[future]
            try:
                result_license, result_data = future.result()
                if result_license and result_data:
                    print(f"Match found for License {result_license}:")
                    print(result_data)
                    return  # Stop after finding the first match
            except Exception as e:
                print(f"Error processing License {license_number}: {e}")

# Main execution
find_matching_lawyer(40000, 70000)
