import pandas as pd
import sqlite3
from sqlalchemy import create_engine
from urllib.parse import quote_plus

# MySQL connection
mysql_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'sql@123',
    'database': 'phonepe_pulse'
}

mysql_engine = create_engine(
    f"mysql+pymysql://{mysql_config['user']}:{quote_plus(mysql_config['password'])}@"
    f"{mysql_config['host']}/{mysql_config['database']}"
)

# SQLite connection
sqlite_conn = sqlite3.connect('phonepe_data.db')

# Tables to migrate
tables = [
    'aggregated_transaction',
    'aggregated_user',
    'map_transaction',
    'map_user',
    'top_transaction',
    'top_user'
]

print("Starting data migration from MySQL to SQLite...")

for table in tables:
    try:
        print(f"\nMigrating {table}...")
        # Read from MySQL
        df = pd.read_sql(f"SELECT * FROM {table}", mysql_engine)
        print(f"  Found {len(df)} records")
        
        # Write to SQLite
        df.to_sql(table, sqlite_conn, if_exists='replace', index=False)
        print(f"  ✅ Successfully migrated {table}")
    except Exception as e:
        print(f"  ❌ Error migrating {table}: {e}")

sqlite_conn.close()
print("\n✅ Migration complete! Database saved as 'phonepe_data.db'")