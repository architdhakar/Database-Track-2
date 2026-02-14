import mysql.connector
import os
from dotenv import load_dotenv

load_dotenv()

def fix_schema():
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
        
        # 1. Identify Unique Constraints (Indices)
        # We look for indices that are UNIQUE but not the PRIMARY KEY (id)
        cursor.execute(f"SHOW INDEX FROM {table_name}")
        indices = cursor.fetchall()
        
        unique_indices = []
        for index in indices:
            index_name = index[2]
            non_unique = index[1]
            column_name = index[4]
            
            # non_unique = 0 means it IS unique. skip PRIMARY.
            if non_unique == 0 and index_name != 'PRIMARY':
                unique_indices.append((index_name, column_name))
        
        if not unique_indices:
            print("No incorrect UNIQUE constraints found.")
            return

        print(f"Found {len(unique_indices)} unique constraints to drop:")
        for idx_name, col_name in unique_indices:
            print(f" - Column: {col_name} (Index: {idx_name})")
            
            # 2. Drop the Index
            try:
                # In MySQL, UNIQUE constraints are usually dropped via DROP INDEX
                # If they were added via ALTER TABLE ADD UNIQUE (col), the index matches the col name.
                cursor.execute(f"ALTER TABLE {table_name} DROP INDEX {idx_name}")
                print(f"   Successfully dropped UNIQUE constraint for {col_name}")
            except mysql.connector.Error as e:
                print(f"   Error dropping index {idx_name}: {e}")

        conn.commit()
        print("\nDatabase remediation complete.")
        
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

if __name__ == "__main__":
    fix_schema()
