from extract_process_ids import fetch_process_ids_by_case_id
import pyodbc
from bidi.algorithm import get_display
import unicodedata
import sys
from dotenv import load_dotenv
import os

# Ensure UTF-8 encoding for stdout
sys.stdout.reconfigure(encoding='utf-8')

# Load environment variables from the .env file
load_dotenv()

# SQL Server connection parameters
server_name = os.getenv("DB_SERVER")
database_name = os.getenv("DB_NAME")
user_name = os.getenv("DB_USER")
password = os.getenv("DB_PASS")

# ANSI escape codes for bold and colored formatting
BOLD_YELLOW = '\033[1;33m'
BOLD_GREEN = '\033[1;32m'
BOLD_RED = '\033[1;31m'
RESET = '\033[0m'

def normalize_hebrew(text):
    """Normalize and format Hebrew text for proper RTL display."""
    if not text:
        return text
    return get_display(unicodedata.normalize("NFKC", text.strip()))

def execute_sql_queries(process_ids):
    """Execute SQL queries for each Process ID."""
    if not process_ids:
        print("No Process IDs provided. Exiting.")
        return

    try:
        print("\nConnecting to SQL Server...")
        connection = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server_name};"
            f"DATABASE={database_name};"
            f"UID={user_name};"
            f"PWD={password};"
            f"Trusted_Connection=yes;"
        )
        cursor = connection.cursor()
        print("Connection to SQL Server established successfully.")

        for process_id in process_ids:
            print(f"\nQuerying SQL for ProcessId: {process_id}")

            # SQL Query 1
            sql_query_1 = """
            SELECT TOP (1000) p.[ProcessID],
                   pt.[ProcessTypeName],
                   p.[LdapLeafID]
            FROM [BPM].[dbo].[Processes] AS p
            JOIN [BPM].[dbo].[ProcessTypes] AS pt
                ON pt.[ProcessTypeID] = p.[ProcessTypeID]
            WHERE p.[ProcessID] = ?;
            """
            cursor.execute(sql_query_1, process_id)
            rows_1 = cursor.fetchall()

            if not rows_1:
                print("No results found for the first query.")
                continue
            else:
                print(f"Results from the first query (Fetched {len(rows_1)} rows):")
                for row in rows_1:
                    print(f"  ProcessID = {row[0]}")
                    print(f"  ProcessTypeName = {BOLD_YELLOW}{normalize_hebrew(row[1])}{RESET}")
                    print(f"  LdapLeafID = {row[2]}")

            # SQL Query 2
            sql_query_2 = """
            SELECT TOP (1000) ps.[ProcessStepID],
                   ps.[ProcessID],
                   pt.[ProcessTypeName],
                   at.[ActivityTypeName],
                   ps.[ProcessTypeGatewayID],
                   ps.[DateForBPETreatment],
                   ps.[TaskID],
                   ps.[SubProcessID],
                   ps.[ContentData],
                   ps.[EventTypeID]
            FROM [BPM].[dbo].[ProcessSteps] AS ps
            JOIN [BPM].[dbo].[ProcessTypeActivities] AS pta
                ON ps.[ProcessTypeActivityID] = pta.[ProcessTypeActivityID]
            JOIN [BPM].[dbo].[ProcessTypes] AS pt
                ON pt.[ProcessTypeID] = pta.[ProcessTypeID]
            JOIN [BPM].[dbo].[ActivityTypes] AS at
                ON at.[ActivityTypeID] = pta.[ActivityTypeID]
            WHERE ps.[ProcessID] = ?;
            """
            cursor.execute(sql_query_2, process_id)
            rows_2 = cursor.fetchall()
           
            if not rows_2:
                print(f"No results found for the second query for ProcessID {process_id}.")
                continue

            print(f"Results from the second query (Fetched {len(rows_2)} rows):")
            for row in rows_2:
                try:
                    process_step_id = row[0]  # Ensure this is numeric
                    print(f"  ProcessStepID = {row[0]}")
                    print(f"  ProcessID = {row[1]}")
                    print(f"  ProcessTypeName = {BOLD_GREEN}{normalize_hebrew(row[2])}{RESET}")
                    print(f"  ActivityTypeName = {BOLD_GREEN}{normalize_hebrew(row[3])}{RESET}")
                    print(f"  ProcessTypeGatewayID = {row[4]}")
                    print(f"  DateForBPETreatment = {row[5]}")
                    print(f"  TaskID = {row[6]}")
                    print(f"  SubProcessID = {row[7]}")
                    print(f"  ContentData = {row[8]}")
                    print(f"  EventTypeID = {row[9]}")
                    print("-" * 50)

                    # SQL Query 3 for each ProcessStepID
                    print(f"Running SQL Query 3 for ProcessStepID {process_step_id}...")
                    sql_query_3 = """
                    SELECT TOP (1000) p.[ProcessStepStatusID],
                           p.[ProcessStepID],
                           s.[Description_Heb]
                    FROM [BPM].[dbo].[ProcessStepStatuses] AS p
                    JOIN [BPM].[dbo].[StatusTypes] AS s
                        ON p.[StatusTypeID] = s.[StatusTypeID]
                    WHERE p.[ProcessStepID] = ?;
                    """
                    cursor.execute(sql_query_3, process_step_id)
                    rows_3 = cursor.fetchall()

                    if not rows_3:
                        print(f"No results found for ProcessStepID {process_step_id}.")
                    else:
                        print(f"Results for ProcessStepID {process_step_id} (Fetched {len(rows_3)} rows):")
                        for row in rows_3:
                            print(f"  ProcessStepStatusID = {row[0]}")
                            print(f"  ProcessStepID = {row[1]}")
                            print(f"  Description_Heb = {BOLD_RED}{normalize_hebrew(row[2])}{RESET}")
                            print("-" * 50)

                except Exception as e:
                    print(f"Error processing ProcessStepID {row[0]}: {e}")

    except Exception as e:
        print(f"Error querying SQL Server: {e}")

    finally:
        if 'connection' in locals():
            connection.close()
            print("SQL Server connection closed.")

# Main Execution
if __name__ == "__main__":
    # User input
    case_id = int(input("Enter case id: "))

    # Fetch process IDs and execute SQL queries
    process_ids = fetch_process_ids_by_case_id(case_id)
    execute_sql_queries(process_ids)
