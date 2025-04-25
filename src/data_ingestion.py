import sqlite3
import os
import polars as pl
from sqlite3.dbapi2 import Cursor

filePath = os.path.abspath(__file__)
db_loc = filePath.split("project_loop")[0] + "project_loop/ingestor.db"

def defineTables(cursor: Cursor):
    cursor.execute('''CREATE TABLE IF NOT EXISTS store_timezones (
        store_id TEXT PRIMARY KEY NOT NULL,
        timezone TEXT NOT NULL
    )''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS store_pings (
        id  INTEGER PRIMARY KEY AUTOINCREMENT,
        store_id TEXT NOT NULL,
        is_active INT NOT NULL,
        recorded_at TEXT NOT NULL,
        FOREIGN KEY (store_id) REFERENCES store_timezones(store_id)
    )''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS store_hours (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        store_id TEXT NOT NULL,
        week_day INTEGER NOT NULL,
        start_time TEXT NOT NULL,
        end_time TEXT NOT NULL,
        FOREIGN KEY (store_id) REFERENCES store_timezones(store_id)
    )''')
    return

def ingest_data():
    print("starting data ingestion")
    raw_path = os.path.abspath(__file__)
    timezones_path = raw_path.split("project_loop")[0] + "project_loop/data/timezones.parquet"
    ping_path = raw_path.split("project_loop")[0] + "project_loop/data/store_status.parquet"
    hours_path = raw_path.split("project_loop")[0] + "project_loop/data/menu_hours.parquet"
    df_timezones = pl.scan_parquet(timezones_path).collect()
    df_ping = pl.scan_parquet(ping_path).collect()
    df_hours = pl.scan_parquet(hours_path).collect()
    df_p = df_ping.select(
        pl.col('store_id').unique(maintain_order=True).alias("UstoreId"),
        pl.col('store_id').unique_counts().alias("count"),
    )
    df_h = df_hours.select(
        pl.col('store_id').unique(maintain_order=True).alias('UstoreId'),
        pl.col('store_id').unique_counts().alias("count"),
    )
    df_ph = df_p.vstack(df_h)
    df_ph_uniq = df_ph.select(
        pl.col('UstoreId').unique(maintain_order=True).alias('UstoreId'),
        pl.col('UstoreId').unique_counts().alias("count"),
    )
    df_extra_timezones = df_ph_uniq.filter(
        ~pl.col('UstoreId').is_in(df_timezones['store_id']),
    )
    df_proc = df_extra_timezones.select(
        pl.col('UstoreId').alias('store_id'),
        pl.lit('America/Chicago').alias('timezone_str'),
    )
    df_timezones.vstack(df_proc, in_place = True)
    df_timezones = df_timezones.rename({"timezone_str": "timezone"})
    df_ping = df_ping.select(
        pl.col('store_id'),
        pl.when(pl.col('status') == 'active').then(pl.lit(1)).otherwise(pl.lit(0)).alias('is_active'),
        pl.col('timestamp_utc').alias('recorded_at'),
    )
    df_hours = df_hours.rename({"dayOfWeek": "week_day", "start_time_local": "start_time", "end_time_local": "end_time"})

    # Latest ping
    # latest_ping = df_ping.select(
    #     pl.col('recorded_at').max()
    # ).item()
    # print(latest_ping)

    df_timezones.write_database(
        table_name="store_timezones",
        connection=f'sqlite:///{db_loc}',
        if_table_exists="append"
    )
    df_ping.write_database(
        table_name="store_pings",
        connection=f'sqlite:///{db_loc}',
        if_table_exists="append",
    )
    df_hours.write_database(
        table_name="store_hours",
        connection=f'sqlite:///{db_loc}',
        if_table_exists="append",
    )
    print("data ingestion complete")
    return


def data_ingestor():
    try:
        conn = sqlite3.connect(db_loc)
        cursor = conn.cursor()

        # define and create Tables
        defineTables(cursor)

        # Ingest data from csv files
        ingest_data()

        cursor.close()
        conn.close()

    except sqlite3.OperationalError as e :
        print("Failed to open database: ", e)
