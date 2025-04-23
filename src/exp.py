import sqlite3
import os
from sqlite3.dbapi2 import Connection, Cursor
import math
filePath = os.path.abspath(__file__)
db_loc = filePath.split("project_loop")[0] + "project_loop/ingestor.db"

from datetime import datetime, timedelta
from dateutil import tz

internal_curr_datetime = "2024-10-14 23:55:18.727055 UTC"
internal_curr_datetime_obj_utc = datetime.strptime(internal_curr_datetime, "%Y-%m-%d %H:%M:%S.%f UTC").replace(tzinfo=tz.gettz('UTC'))

def store_hour_converter(temp_res):
    x = [() for i in range(7)]
    for obj in temp_res:
        x[obj[2]] = (obj[3], obj[4])
    return x


def datetimeToDay(datetimeStr):
    if datetimeStr[-1] != 'C':
        datetimeStr += " UTC"
    weekday = datetime.strptime(datetimeStr, "%Y-%m-%d %H:%M:%S.%f UTC").weekday()
    return weekday

def utc_to_localtimestamp(timezone, datetimestr):
    from_zone = tz.gettz('UTC')
    to_zone = tz.gettz(timezone)
    utc = datetime.strptime(datetimestr, "%Y-%m-%d %H:%M:%S.%f UTC").replace(tzinfo=from_zone)
    localtime_obj = utc.astimezone(to_zone)
    localtime_str = localtime_obj.strftime("%Y-%m-%d %H:%M:%S.%f")
    return localtime_str

def diffInTimes(local, last):
    format = "%Y-%m-%d %H:%M:%S.%f"
    local_obj = datetime.strptime(local, format)
    last_obj = datetime.strptime(last, format)
    time_diff = local_obj - last_obj
    hours = time_diff.total_seconds() // 3600
    minutes = (time_diff.total_seconds() % 3600) // 60
    return (hours, minutes)

def tester():
    try:
        conn = sqlite3.connect(db_loc)
        cursor = conn.cursor()

        total_store_id_query = 'SELECT COUNT(*) FROM store_timezones'
        store_ids_per_page = 200
        cursor.execute(total_store_id_query)
        total_store_ids = cursor.fetchone()[0]
        total_pages = math.ceil(total_store_ids / store_ids_per_page)
        for page in range(total_pages):
            store_id_time_zones_query = f'''SELECT * FROM store_timezones
            ORDER BY store_id
            LIMIT {store_ids_per_page} OFFSET {page * store_ids_per_page}'''
            cursor.execute(store_id_time_zones_query)
            store_id_time_zones = cursor.fetchall()
            for store_id, curr_zone in store_id_time_zones:
                working_hour_query = f'''SELECT * FROM store_hours
                WHERE store_id = ?'''
                cursor.execute(working_hour_query, (store_id,))
                temp_res = cursor.fetchall()
                working_hours = store_hour_converter(temp_res)

                # getting all active (1) observations since 1 week before
                a_week_ago_obj = internal_curr_datetime_obj_utc - timedelta(weeks=1)
                a_day_ago_obj = internal_curr_datetime_obj_utc - timedelta(days=1)
                an_hour_ago_obj = internal_curr_datetime_obj_utc - timedelta(hours=1)
                an_hour_and_half_ago_obj = internal_curr_datetime_obj_utc - timedelta(hours=1, minutes=30)
                a_week_str =  a_week_ago_obj.strftime("%Y-%m-%d %H:%M:%S.%f UTC")
                a_day_str = a_day_ago_obj.strftime("%Y-%m-%d %H:%M:%S.%f UTC")
                an_hour_str = an_hour_ago_obj.strftime("%Y-%m-%d %H:%M:%S.%f UTC")
                an_hour_and_half_str = an_hour_and_half_ago_obj.strftime("%Y-%m-%d %H:%M:%S.%f UTC")
                timestamp_since_week_ago_query = f'''SELECT * FROM store_pings
                WHERE store_id = ? AND recorded_at >= ? AND is_active = 1
                '''
                cursor.execute(timestamp_since_week_ago_query, (store_id, a_week_str))
                timestamps_and_days = cursor.fetchall()
                timestamps_and_days.sort(key = lambda x: (x[3], x[0]))
                last_timestamp = None
                newday = True
                a_week_upticks = 0
                a_day_upticks = 0
                a_hour_upticks = 0
                a_local_day_str = utc_to_localtimestamp(curr_zone, a_day_str)
                a_local_hour_str = utc_to_localtimestamp(curr_zone, an_hour_str)
                a_local_hour_and_half_str = utc_to_localtimestamp(curr_zone, an_hour_and_half_str)
                for index in range(len(timestamps_and_days)):
                    local_timestamp = utc_to_localtimestamp(curr_zone, timestamps_and_days[index][3])
                    if last_timestamp == None or last_timestamp.split(" ")[0] != local_timestamp.split(" ")[0]:
                        # as new day is there, adding the last timestamp forward time covered from the closing hour
                        if index > 0 and last_timestamp != None:
                            temp_stamp = utc_to_localtimestamp(curr_zone, timestamps_and_days[index-1][3])
                            temp_stamp = f'{temp_stamp.split(" ")[0]} {working_hours[datetimeToDay(temp_stamp)][1]}.000000'
                            (x,y) = diffInTimes(temp_stamp, last_timestamp)
                            if x>0 or y>=30:
                                a_week_upticks += 30*60
                            else:
                                a_week_upticks += y*60

                        last_timestamp = f'{local_timestamp.split(" ")[0]} {working_hours[datetimeToDay(local_timestamp)][0]}.000000'
                        newday = True
                    (h,m) = diffInTimes(local_timestamp, last_timestamp)
                    if local_timestamp >= a_local_hour_str:
                        a_hour_upticks = 60*60
                    elif local_timestamp >= a_local_hour_and_half_str:
                        a_hour_upticks += (30 - diffInTimes(local_timestamp, a_local_hour_and_half_str)[1])*60
                    if newday:
                        a_week_upticks += m*60
                        if local_timestamp > a_local_day_str:
                            a_day_upticks += m*60
                        newday = False
                        continue
                    elif h == 0 and m <= 30:
                        continue
                    elif h >= 1:
                        a_week_upticks += 3600
                        if local_timestamp > a_local_day_str:
                            a_day_upticks += 3600
                        continue
                    elif h==0 and m > 30:
                        a_week_upticks += m*60
                        if local_timestamp > a_local_day_str:
                            a_day_upticks += m*60
                        continue


                print(a_week_upticks)
                print(a_day_upticks)
                print(a_hour_upticks)
                break
            print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")


        cursor.close()
        conn.close()

    except sqlite3.OperationalError as e :
        print("Failed to open database: ", e)
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
