# import sqlite3
import os
import polars as pl
# from sqlite3.dbapi2 import Connection, Cursor

def tester():
    raw_path = os.path.abspath(__file__)
    timezones_path = raw_path.split("project_loop")[0] + "project_loop/data/timezones.csv"
    # ping_path = raw_path.split("project_loop")[0] + "project_loop/data/store_status.csv"
    hours_path = raw_path.split("project_loop")[0] + "project_loop/data/menu_hours.csv"
    df_timezones = pl.scan_csv(timezones_path).collect()
    # df_ping = pl.scan_csv(ping_path).collect()
    df_hours = pl.scan_csv(hours_path).collect()
    # df_tp = df_hours.with_columns(
    #     ~df_hours["store_id"].is_in(df_timezones['store_id']).alias("is_present"),
    #     pl.lit("America/Chicago").alias('timezone')
    # )
    df_tp = pl.DataFrame({
    "store_id": df_hours["store_id"],
    "is_present": df_hours["store_id"].is_in(df_timezones["store_id"]),
    })
    df_tp.write_csv("hello.csv")
    print(df_tp)j
    # cursor.executemany("INSERT INTO store_timezones VALUES(?, ?, ?)", df)
    # conn.commit()
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
