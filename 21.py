import sqlite3
import psycopg2

# Define connections
sqlite_path = "mydata.db"
pg_config = {
    "dbname": "mydb",
    "user": "myuser",
    "password": "377843",
    "host": "localhost",
    "port": "5432"
}

# Step 1: Read data from SQLite
sqlite_conn = sqlite3.connect(r'C:\Users\al.omidi\Desktop\gold_price_data.db')
sqlite_cursor = sqlite_conn.cursor()
sqlite_cursor.execute("SELECT * FROM gold_data")
def sanitize(row):
    return [None if val == '' else val for val in row]

rows = [sanitize(row) for row in sqlite_cursor.fetchall()]

# Get column names
column_names = [desc[0] for desc in sqlite_cursor.description]

sqlite_cursor.close()
sqlite_conn.close()

# Step 2: Connect to PostgreSQL
pg_conn = psycopg2.connect(**pg_config)
pg_cursor = pg_conn.cursor()

# Step 3: Create table in PostgreSQL if it doesn't exist
create_table_sql = '''
CREATE TABLE IF NOT EXISTS gold_data (
                        id TEXT,
                          hour text,
                          Time Text,
                          talasea INTEGER,
                          miligold INTEGER,
                          tlyn INTEGER,
                          daric INTEGER,
                          talapp INTEGER,
                          estjt INTEGER,
                          ap INTEGER,
                          TGJU INTEGER,
                          melli INTEGER,
                          wallgold INTEGER,
                          technogold INTEGER,
                          digikala INTEGER,
                          zarpad INTEGER,
                          goldis INTEGER,
                          goldika INTEGER,                          
                          bazaretala INTEGER,
                          ounce INTEGER,
                          dollar INTEGER,
                          dollar_based INTEGER,
                          coin INTEGER,
                          bubble_coin INTEGER)
'''
pg_cursor.execute(create_table_sql)

# Step 4: Insert rows
placeholders = ','.join(['%s'] * len(column_names))
insert_sql = f"INSERT INTO gold_data VALUES ({placeholders})"

pg_cursor.executemany(insert_sql, rows)

# Commit and close
pg_conn.commit()
pg_cursor.close()
pg_conn.close()

print("✅ Data migration from SQLite to PostgreSQL completed.")
