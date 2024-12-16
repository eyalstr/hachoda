import logging
from extract_process_ids import fetch_process_ids_by_case_id_sorted
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

# Configure logging
# Configure logging without prefixes
logging.basicConfig(
    filename='process_analyzer.log',
    filemode='w',
    level=logging.INFO,
    format='%(message)s',  # Only log the message itself
    encoding='utf-8'
)

logger = logging.getLogger()

def log_and_print(message, level="info", ansi_format=None, is_hebrew=False):
    """
    Log a message and print it with optional ANSI formatting.
    If the message contains Hebrew, apply RTL normalization for console output only.
    """
    # Normalize Hebrew text for console, but keep original for log
    if is_hebrew:
        console_message = normalize_hebrew(message)
        log_message = message  # Original logical order for logging
    else:
        console_message = message
        log_message = message

    # Apply ANSI formatting to the console output
    if ansi_format:
        console_message = f"{ansi_format}{console_message}{RESET}"

    # Print to the console
    print(console_message)

    # Log to the file without ANSI formatting
    if level.lower() == "info":
        logger.info(log_message)
    elif level.lower() == "warning":
        logger.warning(log_message)
    elif level.lower() == "error":
        logger.error(log_message)
    elif level.lower() == "debug":
        logger.debug(log_message)

def normalize_hebrew(text):
    """Normalize and format Hebrew text for proper RTL display."""
    if not text:
        return text
    return get_display(unicodedata.normalize("NFKC", text.strip()))

def execute_sql_queries(process_ids):
    """Execute SQL queries for each Process ID."""
    if not process_ids:
        log_and_print("No Process IDs provided. Exiting.", "warning")
        return

    try:
        log_and_print("Connecting to SQL Server...", "info")
        connection = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server_name};"
            f"DATABASE={database_name};"
            f"UID={user_name};"
            f"PWD={password};"
            f"Trusted_Connection=yes;"
        )
        cursor = connection.cursor()
        log_and_print("Connection to SQL Server established successfully.", "info", BOLD_GREEN)

        query_2_counter = 0  # Counter for the second query

        for process_id in process_ids:
            log_and_print(f"\n  Querying SQL for ProcessId: {process_id}", "info", BOLD_YELLOW)

            # SQL Query 1
            sql_query_1 = """
            SELECT TOP (1000) p.[ProcessID],
                   pt.[ProcessTypeName]
            FROM [BPM].[dbo].[Processes] AS p
            JOIN [BPM].[dbo].[ProcessTypes] AS pt
                ON pt.[ProcessTypeID] = p.[ProcessTypeID]
            WHERE p.[ProcessID] = ?;
            """
            cursor.execute(sql_query_1, process_id)
            rows_1 = cursor.fetchall()

            if not rows_1:
                log_and_print("No results found for the first query.", "warning")
                continue
            else:
                #log_and_print(f"Results from the first query (Fetched {len(rows_1)} rows):", "info", BOLD_GREEN)
                for row in rows_1:
                    log_and_print(f"  ProcessID = {row[0]}")
                    log_and_print(f"  ProcessTypeName = {row[1]}", "info", BOLD_YELLOW, is_hebrew=True)

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
                log_and_print(f"No results found for ProcessID {process_id}.", "warning")
                continue

            log_and_print(f"  Results from query (Fetched {len(rows_2)} rows):", "info", BOLD_GREEN)
            for row in rows_2:
                query_2_counter += 1   

                log_and_print(f"\n********* Step={query_2_counter} *************\n", "info", BOLD_GREEN, is_hebrew=True)            
                #log_and_print(f"*******************************  {query_2_counter} : {normalize_hebrew('שלב')}", "info", BOLD_RED,is_hebrew=True)
                #log_and_print(f"\n/***********************************   {query_2_counter}   **********************************/", "info", BOLD_RED)
                #log_and_print(f"\n/*****************************************************************************/", "info", BOLD_RED)
             
                try:
                    process_step_id = row[0]
                    log_and_print(f"  ProcessStepID = {row[0]}")
                    log_and_print(f"  ProcessID = {row[1]}")
                    log_and_print(f"  ProcessTypeName = {row[2]}", "info", BOLD_GREEN, is_hebrew=True)
                    log_and_print(f"  ActivityTypeName = {row[3]}", "info", BOLD_GREEN, is_hebrew=True)

                    # SQL Query 3
                    log_and_print(f"  Information for ProcessStepID {process_step_id}...", "info", BOLD_YELLOW)
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
                        log_and_print(f"No results found for ProcessStepID {process_step_id}.", "warning")
                    else:
                        log_and_print(f"  Results for ProcessStepID {process_step_id} (Fetched {len(rows_3)} rows):", "info", BOLD_GREEN)
                        for row in rows_3:
                            log_and_print(f"  ProcessStepStatusID = {row[0]}")
                            #log_and_print(f"  ProcessStepID = {row[1]}")
                            log_and_print(f"  Description_Heb = {row[2]}", "info", BOLD_RED, is_hebrew=True)

                except Exception as e:
                    log_and_print(f"Error processing ProcessStepID {row[0]}: {e}", "error", BOLD_RED)

    except Exception as e:
        log_and_print(f"Error querying SQL Server: {e}", "error", BOLD_RED)

    finally:
        if 'connection' in locals():
            connection.close()
            log_and_print("\n")  
            log_and_print("  SQL Server connection closed.", "info", BOLD_GREEN)

# Main Execution
if __name__ == "__main__":
    # User input
    case_id = int(input("Enter case id: "))
    process_ids = fetch_process_ids_by_case_id_sorted(case_id)
    execute_sql_queries(process_ids)
    log_and_print("  Execution completed. Press Enter to exit.", "info", BOLD_GREEN)
    input()  # Wait for user input before closing
