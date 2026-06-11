#!/usr/bin/env python3
import os
import sys
import sqlite3
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

USE_SQLITE = os.getenv('USE_SQLITE', 'true').lower() == 'true'
SQLITE_DB_PATH = os.getenv('SQLITE_DB_PATH', 'pm_monitoring.db')
DATABASE_URL = os.getenv('DATABASE_URL')

print("=" * 60)
print("PM MONITORING DATABASE DIAGNOSTICS")
print("=" * 60)
print(f"USE_SQLITE: {USE_SQLITE}")
if USE_SQLITE:
    print(f"SQLITE_DB_PATH: {os.path.abspath(SQLITE_DB_PATH)}")
    if os.path.exists(SQLITE_DB_PATH):
        print(f"  - File exists: Yes (Size: {os.path.getsize(SQLITE_DB_PATH)} bytes)")
    else:
        print(f"  - File exists: NO! (An empty or default database will be created on app run if writable)")
else:
    print(f"DATABASE_URL: {DATABASE_URL[:30]}..." if DATABASE_URL else "DATABASE_URL: NOT SET")

def get_connection():
    if USE_SQLITE:
        conn = sqlite3.connect(SQLITE_DB_PATH)
        conn.row_factory = sqlite3.Row
        return conn
    else:
        import psycopg2
        import urllib.parse
        parsed = urllib.parse.urlparse(DATABASE_URL)
        return psycopg2.connect(
            host=parsed.hostname,
            database=parsed.path.lstrip('/'),
            user=parsed.username,
            password=parsed.password,
            port=parsed.port or 5432
        )

try:
    conn = get_connection()
    cur = conn.cursor()
    print("Database Connection: SUCCESSFUL\n")
    
    # 1. Check users
    print("1. Dust Users Table:")
    try:
        cur.execute("SELECT id, username, email, is_admin FROM dust_users")
        users = cur.fetchall()
        for u in users:
            row = dict(u) if USE_SQLITE else u
            print(f"  - ID: {row[0] if not USE_SQLITE else row['id']}, Username: {row[1] if not USE_SQLITE else row['username']}, Email: {row[2] if not USE_SQLITE else row['email']}, Admin: {row[3] if not USE_SQLITE else row['is_admin']}")
        if not users:
            print("  - No users found.")
    except Exception as e:
        print(f"  - Error querying dust_users: {e}")
        
    # 2. Check data sources
    print("\n2. Dust Data Sources Table:")
    try:
        cur.execute("SELECT id, source_type, broker_url, description FROM dust_data_sources")
        sources = cur.fetchall()
        for s in sources:
            row = dict(s) if USE_SQLITE else s
            print(f"  - ID: {row[0] if not USE_SQLITE else row['id']}, Type: {row[1] if not USE_SQLITE else row['source_type']}, Broker: {row[2] if not USE_SQLITE else row['broker_url']}, Desc: {row[3] if not USE_SQLITE else row['description']}")
        if not sources:
            print("  - No data sources found.")
    except Exception as e:
        print(f"  - Error querying dust_data_sources: {e}")

    # 3. Check devices
    print("\n3. Dust Devices Table:")
    try:
        cur.execute("SELECT id, deviceid, name, user_id, data_source_id, has_relay FROM dust_devices")
        devices = cur.fetchall()
        for d in devices:
            row = dict(d) if USE_SQLITE else d
            print(f"  - ID: {row[0] if not USE_SQLITE else row['id']}, DeviceID: {row[1] if not USE_SQLITE else row['deviceid']}, Name: {row[2] if not USE_SQLITE else row['name']}, User_ID: {row[3] if not USE_SQLITE else row['user_id']}, DataSource_ID: {row[4] if not USE_SQLITE else row['data_source_id']}, HasRelay: {row[5] if not USE_SQLITE else row['has_relay']}")
        if not devices:
            print("  - No devices found.")
    except Exception as e:
        print(f"  - Error querying dust_devices: {e}")

    # 4. Check device + datasource JOIN (What the API runs)
    print("\n4. Devices Endpoint Join Query:")
    try:
        cur.execute("""
            SELECT d.id, d.deviceid, d.name, d.has_relay, ds.source_type
            FROM dust_devices d
            JOIN dust_data_sources ds ON d.data_source_id = ds.id
        """)
        joined = cur.fetchall()
        for j in joined:
            row = dict(j) if USE_SQLITE else j
            print(f"  - ID: {row[0] if not USE_SQLITE else row['id']}, DeviceID: {row[1] if not USE_SQLITE else row['deviceid']}, Name: {row[2] if not USE_SQLITE else row['name']}, SourceType: {row[4] if not USE_SQLITE else row['source_type']}")
        if not joined:
            print("  - JOIN QUERY RETURNED 0 ROWS! This is why no devices show up in the UI.")
            
            # Sub-diagnostics: check for mismatched IDs
            cur.execute("SELECT DISTINCT data_source_id FROM dust_devices")
            mismatched_ids = [r[0] for r in cur.fetchall()]
            print(f"    - Hint: The devices refer to data_source_ids: {mismatched_ids}")
            cur.execute("SELECT id FROM dust_data_sources")
            available_ids = [r[0] for r in cur.fetchall()]
            print(f"    - Hint: The available data_source IDs are: {available_ids}")
    except Exception as e:
        print(f"  - Error querying join: {e}")

    # 5. Check database size and write capability
    print("\n5. Database Write Capability Check:")
    if USE_SQLITE:
        try:
            cur.execute("CREATE TABLE IF NOT EXISTS _write_test (val TEXT)")
            cur.execute("INSERT INTO _write_test VALUES ('ok')")
            conn.commit()
            cur.execute("SELECT * FROM _write_test")
            val = cur.fetchone()[0]
            cur.execute("DROP TABLE _write_test")
            conn.commit()
            print(f"  - Write permission to DB file: SUCCESS ({val})")
        except Exception as e:
            print(f"  - Write permission check FAILED: {e}")
            
    conn.close()

except Exception as e:
    print(f"Error establishing connection: {e}")
    sys.exit(1)

print("\n" + "=" * 60)
print("Diagnostics complete.")
print("=" * 60)
