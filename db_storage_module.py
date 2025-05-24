import sqlite3
import json
from datetime import datetime, timezone
import logging

# --- Configuration & Initialization ---
DB_PATH = "./upstox_data_v2.db"
TARGET_DATE_STR_FOR_TABLES = "2025-05-23" # Date for table naming

RAW_DATA_TABLE_NAME = f"data_{TARGET_DATE_STR_FOR_TABLES.replace('-', '_')}"
FILTERED_DATA_TABLE_NAME = f"filtered_{TARGET_DATE_STR_FOR_TABLES.replace('-', '_')}"

# Logging Setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s')

# --- Simulated Input Data (from fno_stock_processor.py) ---
SIMULATED_ALL_STOCKS_RAW_DATA = [
    {'stock_symbol': 'RELIANCE', 'prev_day_equity_close': 1456.4, 'prev_day_equity_oi': 0, 'equity_920_price': 1460.0, 'equity_920_oi': 0},
    {'stock_symbol': 'GRASIM', 'prev_day_equity_close': 1200.0, 'prev_day_equity_oi': 0, 'equity_920_price': 1230.0, 'equity_920_oi': 0},
    {'stock_symbol': 'TRENT', 'prev_day_equity_close': 1500.0, 'prev_day_equity_oi': 0, 'equity_920_price': 1450.0, 'equity_920_oi': 0},
    {'stock_symbol': 'INFY', 'prev_day_equity_close': None, 'prev_day_equity_oi': None, 'equity_920_price': None, 'equity_920_oi': None, 'error': 'Instrument key not found'} # Example with error/None
]

SIMULATED_FILTERED_STOCKS_PERCENT_CHANGE = [
    ('GRASIM', 2.5), 
    ('TRENT', -3.33)
]

def store_processed_data(all_stocks_data, filtered_stocks_data):
    """
    Stores raw processed stock data and filtered stock list into SQLite database.
    """
    conn = None
    summary = {
        "status": "pending",
        "raw_data_table": RAW_DATA_TABLE_NAME,
        "filtered_data_table": FILTERED_DATA_TABLE_NAME,
        "raw_rows_inserted": 0,
        "filtered_rows_inserted": 0,
        "errors": []
    }

    try:
        logging.info(f"Connecting to database at: {DB_PATH}")
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # --- 1. Store Raw Data ---
        logging.info(f"Preparing to store raw data into table: {RAW_DATA_TABLE_NAME}")
        cursor.execute(f"DROP TABLE IF EXISTS {RAW_DATA_TABLE_NAME}")
        create_raw_table_sql = f"""
        CREATE TABLE {RAW_DATA_TABLE_NAME} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stock_symbol TEXT NOT NULL,
            prev_day_equity_close REAL,
            prev_day_equity_oi INTEGER,
            equity_920_price REAL,
            equity_920_oi INTEGER,
            record_timestamp TEXT NOT NULL
        );
        """
        cursor.execute(create_raw_table_sql)
        logging.info(f"Table {RAW_DATA_TABLE_NAME} created (or recreated).")

        raw_rows_inserted_count = 0
        for record in all_stocks_data:
            current_ts = datetime.now(timezone.utc).isoformat()
            # Handle cases where data might be None, ensure it's correctly inserted as NULL
            insert_sql = f"""
            INSERT INTO {RAW_DATA_TABLE_NAME} (
                stock_symbol, prev_day_equity_close, prev_day_equity_oi,
                equity_920_price, equity_920_oi, record_timestamp
            ) VALUES (?, ?, ?, ?, ?, ?);
            """
            # Extract values safely, defaulting to None if key is missing (though data structure is fixed here)
            data_tuple = (
                record.get('stock_symbol'),
                record.get('prev_day_equity_close'),
                record.get('prev_day_equity_oi'),
                record.get('equity_920_price'),
                record.get('equity_920_oi'),
                current_ts
            )
            cursor.execute(insert_sql, data_tuple)
            raw_rows_inserted_count += 1
        conn.commit()
        summary["raw_rows_inserted"] = raw_rows_inserted_count
        logging.info(f"Successfully inserted {raw_rows_inserted_count} rows into {RAW_DATA_TABLE_NAME}.")

        # --- 2. Store Filtered Data ---
        logging.info(f"Preparing to store filtered data into table: {FILTERED_DATA_TABLE_NAME}")
        cursor.execute(f"DROP TABLE IF EXISTS {FILTERED_DATA_TABLE_NAME}")
        create_filtered_table_sql = f"""
        CREATE TABLE {FILTERED_DATA_TABLE_NAME} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stock_symbol TEXT NOT NULL,
            percent_change REAL,
            record_timestamp TEXT NOT NULL
        );
        """
        cursor.execute(create_filtered_table_sql)
        logging.info(f"Table {FILTERED_DATA_TABLE_NAME} created (or recreated).")

        filtered_rows_inserted_count = 0
        for symbol, p_change in filtered_stocks_data:
            current_ts = datetime.now(timezone.utc).isoformat()
            insert_sql = f"""
            INSERT INTO {FILTERED_DATA_TABLE_NAME} (
                stock_symbol, percent_change, record_timestamp
            ) VALUES (?, ?, ?);
            """
            cursor.execute(insert_sql, (symbol, p_change, current_ts))
            filtered_rows_inserted_count += 1
        conn.commit()
        summary["filtered_rows_inserted"] = filtered_rows_inserted_count
        logging.info(f"Successfully inserted {filtered_rows_inserted_count} rows into {FILTERED_DATA_TABLE_NAME}.")

        summary["status"] = "success"

    except sqlite3.Error as e:
        error_msg = f"SQLite error: {e}"
        logging.error(error_msg)
        summary["status"] = "error"
        summary["errors"].append(error_msg)
    except Exception as e:
        error_msg = f"An unexpected error occurred: {e}"
        logging.error(error_msg)
        summary["status"] = "error"
        summary["errors"].append(error_msg)
    finally:
        if conn:
            logging.info("Closing database connection.")
            conn.close()
    
    return summary

if __name__ == "__main__":
    logging.info("Starting database storage process with simulated data.")
    # Use the simulated data defined at the top of the script
    result_summary = store_processed_data(
        SIMULATED_ALL_STOCKS_RAW_DATA,
        SIMULATED_FILTERED_STOCKS_PERCENT_CHANGE
    )
    
    print("\n--- Database Storage Summary ---")
    print(json.dumps(result_summary, indent=4))

    # Optional: Verification by reading from DB (if needed for confirmation here)
    if result_summary["status"] == "success":
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        print(f"\n--- Verifying data in {RAW_DATA_TABLE_NAME} (first 5 rows) ---")
        cursor.execute(f"SELECT * FROM {RAW_DATA_TABLE_NAME} LIMIT 5")
        for row in cursor.fetchall():
            print(row)
        
        print(f"\n--- Verifying data in {FILTERED_DATA_TABLE_NAME} (first 5 rows) ---")
        cursor.execute(f"SELECT * FROM {FILTERED_DATA_TABLE_NAME} LIMIT 5")
        for row in cursor.fetchall():
            print(row)
        conn.close()
