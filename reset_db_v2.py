import mysql.connector
import os
from dotenv import load_dotenv

load_dotenv()

def reset_db():
    config = {
        'host': os.getenv("SQL_HOST"),
        'port': int(os.getenv("SQL_PORT", 3306)),
        'user': os.getenv("SQL_USER"),
        'password': os.getenv("SQL_PASSWORD"),
        'database': os.getenv("SQL_DB_NAME")
    }

    try:
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        
        table_name = "structured_data"
        
        # Drop the table to start perfectly fresh
        print(f"Dropping table {table_name}...")
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        print("Database reset complete.")
        conn.commit()
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

if __name__ == "__main__":
    reset_db()
