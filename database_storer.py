import sqlite3
import json
from datetime import datetime, timezone

# --- Configuration ---
DB_PATH = "./upstox_data_v2.db"
ASSUMED_CURRENT_DATE = "2025-05-17" # For table naming and context
TABLE_NAME = f"data_{ASSUMED_CURRENT_DATE.replace('-', '_')}" # e.g., data_2025_05_17

# Data to be inserted (as per prompt for this subtask)
STOCK_SYMBOL = "RELIANCE"
# From Subtask 14 (equity data for 2025-05-16):
PREV_DAY_EQUITY_CLOSE = 1456.4 
PREV_DAY_EQUITY_OI = 0
# From Subtask 15 (equity intraday for 2025-05-17):
EQUITY_920_PRICE = None # As no data was available for 2025-05-17 in tests
EQUITY_920_OI = None    # As no data was available for 2025-05-17 in tests

def initialize_database_and_insert_equity_data():
    """
    Connects to SQLite, creates table (new schema), inserts equity data,
    queries for verification, and closes connection.
    """
    conn = None 
    try:
        print(f"Connecting to database at: {DB_PATH}")
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        print(f"Successfully connected. Using table name: {TABLE_NAME}")

        # 4. Create the table if it doesn't already exist with the new schema
        # Note: If the table exists from a previous subtask with a *different* schema, 
        # this CREATE TABLE IF NOT EXISTS will not modify it. 
        # For a clean test, the old table might need to be dropped manually if schema changes are radical.
        # However, this subtask implies adding to or using this specific schema.
        # Let's assume this is the first time this specific schema is defined for the table or it's compatible.
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stock_symbol TEXT NOT NULL,
            prev_day_equity_close REAL,
            prev_day_equity_oi INTEGER,
            equity_920_price REAL,
            equity_920_oi INTEGER,
            record_timestamp TEXT NOT NULL
        );
        """
        # Drop table if it exists, to ensure new schema is applied if it changed.
        # This is for robust re-runnability if schema was different in subtask 12.
        # In a real scenario, migrations would be handled more carefully.
        # For this task, if table data_2025_05_17 exists, we're essentially replacing its schema
        # if it's different from what subtask 12 created.
        # Subtask 12 schema: id, stock_symbol, prev_day_equity_close, futures_920_price, futures_920_oi, record_timestamp
        # Current schema:   id, stock_symbol, prev_day_equity_close, prev_day_equity_oi, equity_920_price, equity_920_oi, record_timestamp
        # The schemas are different. So, to be safe, I will drop the table first if it exists.
        # This ensures the new schema is applied correctly.
        
        print(f"Dropping table {TABLE_NAME} if it exists, to apply potentially new schema...")
        cursor.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
        conn.commit()
        print(f"Table {TABLE_NAME} dropped (if it existed).")
        
        print(f"Executing CREATE TABLE statement for table {TABLE_NAME} with new schema...")
        cursor.execute(create_table_sql)
        conn.commit()
        print(f"Table {TABLE_NAME} created with the specified schema.")

        # 5. Insert the defined data into the table
        current_utc_timestamp_iso = datetime.now(timezone.utc).isoformat()
        
        insert_sql = f"""
        INSERT INTO {TABLE_NAME} (
            stock_symbol, 
            prev_day_equity_close, 
            prev_day_equity_oi,
            equity_920_price, 
            equity_920_oi, 
            record_timestamp
        ) VALUES (?, ?, ?, ?, ?, ?);
        """
        data_to_insert = (
            STOCK_SYMBOL,
            PREV_DAY_EQUITY_CLOSE,
            PREV_DAY_EQUITY_OI,
            EQUITY_920_PRICE, # Will be stored as NULL if None
            EQUITY_920_OI,    # Will be stored as NULL if None
            current_utc_timestamp_iso
        )
        
        print(f"Inserting data into {TABLE_NAME}: {data_to_insert}")
        cursor.execute(insert_sql, data_to_insert)
        inserted_row_id = cursor.lastrowid 
        conn.commit()
        print(f"Data inserted successfully. Row ID: {inserted_row_id}")

        # 7. Verify by querying the inserted row
        query_sql = f"SELECT id, stock_symbol, prev_day_equity_close, prev_day_equity_oi, equity_920_price, equity_920_oi, record_timestamp FROM {TABLE_NAME} WHERE id = ?;"
        print(f"Verifying insertion by querying row ID: {inserted_row_id}")
        cursor.execute(query_sql, (inserted_row_id,))
        fetched_row = cursor.fetchone()
        
        if fetched_row:
            print(f"Verification successful. Fetched row: {fetched_row}")
            column_names = [description[0] for description in cursor.description]
            fetched_row_dict = dict(zip(column_names, fetched_row))
            return {"status": "success", "table_name": TABLE_NAME, "inserted_row_id": inserted_row_id, "verified_data": fetched_row_dict}
        else:
            print("Verification failed: Could not fetch the inserted row.")
            return {"status": "error", "message": "Verification failed, could not fetch inserted row.", "table_name": TABLE_NAME, "inserted_row_id": inserted_row_id}

    except sqlite3.Error as e:
        error_message = f"SQLite error: {e}"
        print(error_message)
        return {"status": "error", "message": error_message, "db_path": DB_PATH, "table_name": TABLE_NAME if 'TABLE_NAME' in locals() else "Unknown"}
    except Exception as e:
        error_message = f"An unexpected error occurred: {e}"
        print(error_message)
        return {"status": "error", "message": error_message}
    finally:
        if conn:
            print("Closing database connection.")
            conn.close()

if __name__ == "__main__":
    # Renamed main function to avoid conflict if imported elsewhere.
    result = initialize_database_and_insert_equity_data() 
    print("--- Script Result JSON ---")
    print(json.dumps(result, indent=4))
