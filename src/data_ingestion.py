import sqlite3
import os
import polars as pl
from sqlite3.dbapi2 import Connection, Cursor

filePath = os.path.abspath(__file__)
db_loc = filePath.split("project_loop")[0] + "project_loop/ingestor.db"

def defineTables(cursor: Cursor):
    cursor.execute('''CREATE TABLE IF NOT EXISTS store_timezones (
        store_id TEXT PRIMARY KEY NOT NULL,
        timezone TEXT NOT NULL
    )''')
    print("created_timezones")
    cursor.execute('''CREATE TABLE IF NOT EXISTS store_pings (
        id  INT PRIMARY KEY,
        store_id TEXT NOT NULL,
        is_active INT NOT NULL,
        recorded_at TEXT NOT NULL,
        FOREIGN KEY (store_id) REFERENCES store_timezones(store_id)
    )''')
    print("created_pings")
    cursor.execute('''CREATE TABLE IF NOT EXISTS store_hours (
        id INT PRIMARY KEY,
        store_id TEXT NOT NULL,
        week_day INTEGER NOT NULL,
        start_time TEXT NOT NULL,
        end_time TEXT NOT NULL,
        FOREIGN KEY (store_id) REFERENCES store_timezones(store_id)
    )''')
    print("created_hours")
    return

def ingest_data(cursor: Cursor, conn: Connection):
    raw_path = os.path.abspath(__file__)
    timezones_path = raw_path.split("project_loop")[0] + "project_loop/data/timezones.csv"
    ping_path = raw_path.split("project_loop")[0] + "project_loop/data/store_status.csv"
    hours_path = raw_path.split("project_loop")[0] + "project_loop/data/menu_hours.csv"
    df_timezones = pl.scan_csv(timezones_path).collect()
    df_ping = pl.scan_csv(ping_path).collect()
    df_hours = pl.scan_csv(hours_path).collect()
    df_timezone = df_timezones.
    cursor.executemany("INSERT INTO store_timezones VALUES(?, ?, ?)", df)
    conn.commit()
    return

try:
    conn = sqlite3.connect(db_loc)
    cursor = conn.cursor()

    # define and create Tables
    # defineTables(cursor)

    # Ingest data from csv files
    ingest_data(cursor, conn)

    cursor.close()
    conn.close()
except sqlite3.OperationalError as e :
    print("Failed to open database: ", e)
