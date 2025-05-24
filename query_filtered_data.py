import sqlite3
import json # Not strictly for output here, but good for consistency in main if returning dict
import logging

# --- Configuration ---
DB_PATH = "./upstox_data_v2.db"
# This table name should match what was created in Subtask 18/19
FILTERED_TABLE_NAME = "filtered_2025_05_23" 

# Logging Setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s')

def query_and_display_filtered_stocks():
    """
    Connects to SQLite, queries the filtered stocks table, and prints the results.
    """
    conn = None
    status_summary = {"table_queried": FILTERED_TABLE_NAME, "stocks_found": 0, "status": "pending"}

    try:
        logging.info(f"Connecting to database at: {DB_PATH}")
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        logging.info(f"Successfully connected. Querying table: {FILTERED_TABLE_NAME}")

        query_sql = f"SELECT stock_symbol, percent_change FROM {FILTERED_TABLE_NAME} ORDER BY stock_symbol;"
        
        try:
            cursor.execute(query_sql)
            results = cursor.fetchall()
        except sqlite3.OperationalError as e:
            if "no such table" in str(e).lower():
                logging.warning(f"Table {FILTERED_TABLE_NAME} does not exist.")
                print(f"\nNo stocks met the filter criteria (table {FILTERED_TABLE_NAME} not found).")
                status_summary["status"] = "table_not_found"
                status_summary["error_message"] = str(e)
                return status_summary # Exit early if table not found
            else:
                raise # Re-raise other operational errors

        if results:
            print(f"\n--- Filtered Stocks (from {FILTERED_TABLE_NAME}) ---")
            for row in results:
                stock_symbol, percent_change = row
                # Ensure percent_change is formatted to two decimal places
                print(f"Stock: {stock_symbol}, Change: {percent_change:.2f}%")
                status_summary["stocks_found"] += 1
            status_summary["status"] = "success"
        else:
            logging.info(f"No data found in table {FILTERED_TABLE_NAME}.")
            print(f"\nNo stocks met the filter criteria in table {FILTERED_TABLE_NAME} (table was empty or all values were NULL).")
            status_summary["status"] = "no_data_found"
            
    except sqlite3.Error as e:
        error_msg = f"SQLite error: {e}"
        logging.error(error_msg)
        print(f"\nAn error occurred while querying the database: {e}")
        status_summary["status"] = "error"
        status_summary["error_message"] = error_msg
    except Exception as e:
        error_msg = f"An unexpected error occurred: {e}"
        logging.error(error_msg)
        print(f"\nAn unexpected error occurred: {e}")
        status_summary["status"] = "error"
        status_summary["error_message"] = error_msg
    finally:
        if conn:
            logging.info("Closing database connection.")
            conn.close()
            
    return status_summary

if __name__ == "__main__":
    logging.info("Starting process to query and display filtered stock data.")
    query_summary = query_and_display_filtered_stocks()
    
    print("\n--- Query Process Summary ---")
    print(json.dumps(query_summary, indent=4))
