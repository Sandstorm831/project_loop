# import sqlite3
import os
import polars as pl
from test.test_typing import Other
# from sqlite3.dbapi2 import Connection, Cursor

def tester():
    raw_path = os.path.abspath(__file__)
    timezones_path = raw_path.split("project_loop")[0] + "project_loop/data/timezones.csv"
    ping_path = raw_path.split("project_loop")[0] + "project_loop/data/store_status.csv"
    hours_path = raw_path.split("project_loop")[0] + "project_loop/data/menu_hours.csv"
    df_timezones = pl.scan_csv(timezones_path).collect()
    df_ping = pl.scan_csv(ping_path).collect()
    df_hours = pl.scan_csv(hours_path).collect()

    # df_p = df_ping.select(
    #     pl.col('store_id').unique(maintain_order=True).alias("UstoreId"),
    #     pl.col('store_id').unique_counts().alias("count"),
    # )
    # df_h = df_hours.select(
    #     pl.col('store_id').unique(maintain_order=True).alias('UstoreId'),
    #     pl.col('store_id').unique_counts().alias("count"),
    # )
    # df_temp = df_p.vstack(df_h)
    # df_uniq = df_temp.select(
    #     pl.col('UstoreId').unique(maintain_order=True).alias('UstoreId'),
    #     pl.col('UstoreId').unique_counts().alias("count"),
    # )
    # df_temp = df_uniq.filter(
    #     ~pl.col('UstoreId').is_in(df_timezones['store_id']),
    # )
    # df_proc = df_temp.select(
    #     pl.col('UstoreId').alias('store_id'),
    #     pl.lit('America/Chicago').alias('timezone_str'),
    # )
    # df_timezones.vstack(df_proc, in_place = True)
    df_ping = df_ping.select(
        pl.col('store_id'),
        pl.when(pl.col('status') == 'active').then(pl.lit(1)).otherwise(pl.lit(0)).alias('is_active'),
        pl.col('timestamp_utc').alias('recorded_at'),
    )
    print(df_ping)
    return

tester()

# def ingest_data(cursor: Cursor, conn: Connection):
#     raw_path = os.path.abspath(__file__)
#     # timezones_path = raw_path.split("project_loop")[0] + "project_loop/data/timezones.csv"
#     # ping_path = raw_path.split("project_loop")[0] + "project_loop/data/store_status.csv"
#     hours_path = raw_path.split("project_loop")[0] + "project_loop/data/menu_hours.csv"
#     # df_timezones = pl.scan_csv(timezones_path).collect()
#     # df_ping = pl.scan_csv(ping_path).collect()
#     df_hours = pl.scan_csv(hours_path).collect()
    # df_tp = df_hours.with_columns(
    #     pl.col('store_id'),
    #     # timezone = "America/Chicago"
    # )
#     print(df_tp)
#     # cursor.executemany("INSERT INTO store_timezones VALUES(?, ?, ?)", df)
#     # conn.commit()
#     return

# try:
    # conn = sqlite3.connect(db_loc)
    # cursor = conn.cursor()

    # define and create Tables
    # defineTables(cursor)

    # Ingest data from csv files
    # ingest_data(cursor, conn)

    # cursor.close()
    # conn.close()
# except sqlite3.OperationalError as e :
    # print("Failed to open database: ", e)
