from dateutil import tz
from datetime import datetime, timezone, timedelta
import sqlite3
import os
from sqlite3.dbapi2 import Connection, Cursor
import math
import time
import polars as pl

filePath = os.path.abspath(__file__)
db_loc = filePath.split("project_loop")[0] + "project_loop/ingestor.db"
result_loc = filePath.split("project_loop")[0] + "project_loop/report.csv"

internal_curr_datetime = "2024-10-14 23:55:18.727055 UTC"
internal_curr_datetime_obj_utc = datetime.strptime(
    internal_curr_datetime, "%Y-%m-%d %H:%M:%S.%f UTC").replace(tzinfo=timezone.utc)


def store_hour_converter(temp_res):
    x = [["00:01:00", "23:59:59"] for i in range(7)]
    if len(temp_res) == 0:
        return x
    for obj in temp_res:

        x[obj[2]] = [obj[3], obj[4]]
    return x


def datetimeToDay(datetimeStr):
    if datetimeStr[-1] != 'C':
        datetimeStr += " UTC"
    weekday = datetime.strptime(
        datetimeStr, "%Y-%m-%d %H:%M:%S.%f UTC").weekday()
    return weekday


def utc_to_localtimestamp(timezone, datetimestr):
    from_zone = tz.gettz('UTC')
    to_zone = tz.gettz(timezone)
    utc = datetime.strptime(
        datetimestr, "%Y-%m-%d %H:%M:%S.%f UTC").replace(tzinfo=from_zone)
    localtime_obj = utc.astimezone(to_zone)
    localtime_str = localtime_obj.strftime("%Y-%m-%d %H:%M:%S.%f")
    return localtime_str


def datetimeDiff(local, last):
    format = "%Y-%m-%d %H:%M:%S.%f"
    local_obj = datetime.strptime(local, format)
    last_obj = datetime.strptime(last, format)
    time_diff = local_obj - last_obj
    hours = time_diff.total_seconds() // 3600
    minutes = (time_diff.total_seconds() % 3600) // 60
    return (hours, minutes)


def timeDiff(open, close):
    format = "%H:%M:%S"
    close_obj = datetime.strptime(close, format)
    open_obj = datetime.strptime(open, format)
    time_diff = close_obj - open_obj
    hours = time_diff.total_seconds() // 3600
    minutes = (time_diff.total_seconds() % 3600) // 60
    return (hours, minutes)


def calc_uptime_downtime(week_u, day_u, hour_u, working_hours, store_id, timezone, weekday_day_ago):
    week_work = [0.0, 0.0]
    day_work = [0.0, 0.0]
    hour_work = 0.0
    for i in range(len(working_hours)):
        temp = timeDiff(working_hours[i][0], working_hours[i][1])
        week_work[0] += temp[0]
        week_work[1] += temp[1]

    a_day_ago_obj = internal_curr_datetime_obj_utc - timedelta(days=1)
    a_day_ago_str = a_day_ago_obj.strftime("%Y-%m-%d %H:%M:%S.%f UTC")
    a_day_ago_local_str = utc_to_localtimestamp(timezone, a_day_ago_str)
    today_local_str = utc_to_localtimestamp(timezone, internal_curr_datetime)
    weekday_today = (weekday_day_ago + 1) % 7
    day_ago_open = f'{a_day_ago_local_str.split(" ")[0]} {working_hours[weekday_day_ago][0]}.000000'
    day_ago_close = f'{a_day_ago_local_str.split(" ")[0]} {working_hours[weekday_day_ago][1]}.000000'
    today_open = f'{today_local_str.split(" ")[0]} {working_hours[weekday_today][0]}.000000'
    today_close = f'{today_local_str.split(" ")[0]} {working_hours[weekday_today][1]}.000000'
    if a_day_ago_local_str <= day_ago_open:
        temp = datetimeDiff(day_ago_close, day_ago_open)
        day_work[0] += temp[0]
        day_work[1] += temp[1]
    elif a_day_ago_local_str > day_ago_open and a_day_ago_local_str < day_ago_close:
        temp = datetimeDiff(day_ago_close, a_day_ago_local_str)
        day_work[0] += temp[0]
        day_work[1] += temp[1]
    if today_local_str >= today_close:
        temp = datetimeDiff(today_close, today_open)
        day_work[0] += temp[0]
        day_work[1] += temp[1]
    elif today_local_str > today_open and today_local_str < today_close:
        temp = datetimeDiff(today_local_str, today_open)
        day_work[0] += temp[0]
        day_work[1] += temp[1]

    a_hour_ago_obj = internal_curr_datetime_obj_utc - timedelta(hours=1)
    a_hour_ago_str = a_hour_ago_obj.strftime("%Y-%m-%d %H:%M:%S.%f UTC")
    a_hour_ago_local_str = utc_to_localtimestamp(timezone, a_hour_ago_str)
    if a_hour_ago_local_str >= today_open:
        if today_local_str <= today_close:
            hour_work = 60
        elif today_local_str > today_close and today_close >= a_hour_ago_local_str:
            temp = datetimeDiff(today_close, a_hour_ago_local_str)
            hour_work += temp[1]
    elif a_hour_ago_local_str < today_open and today_local_str >= today_open:
        if today_local_str <= today_close:
            temp = datetimeDiff(a_hour_ago_local_str, today_open)
            hour_work += temp[1]
        elif today_local_str > today_close:
            temp = datetimeDiff(today_close, today_open)
            hour_work += temp[1]

    week_seconds = week_work[0]*3600 + week_work[1]*60
    day_seconds = day_work[0]*3600 + day_work[1]*60
    hour_seconds = hour_work*60
    week_uptime_hours = week_u // 3600
    day_uptime_hours = day_u // 3600
    hour_uptime_minutes = hour_u // 60
    week_downtime_hours = (week_seconds - week_u) // 3600
    day_downtime_hours = (day_seconds - day_u) // 3600
    hour_downtime_minutes = (hour_seconds - hour_u) // 60
    return [store_id, hour_uptime_minutes, day_uptime_hours, week_uptime_hours, hour_downtime_minutes, day_downtime_hours, week_downtime_hours]


def start_queries(conn: Connection, cursor: Cursor):
    schema = {
        "store_id": pl.String,
        "uptime_last_hour": pl.Float32,
        "uptime_last_day": pl.Float32,
        "update_last_week": pl.Float32,
        "downtime_last_hour": pl.Float32,
        "downtime_last_day": pl.Float32,
        "downtime_last_week": pl.Float32,
    }
    result_df = pl.DataFrame(schema=schema)
    total_store_id_query = 'SELECT COUNT(*) FROM store_timezones'
    store_ids_per_page = 200
    cursor.execute(total_store_id_query)
    total_store_ids = cursor.fetchone()[0]
    total_pages = math.ceil(total_store_ids / store_ids_per_page)
    poor_time_start = time.time()
    for page in range(total_pages):
        store_id_time_zones_query = f'''SELECT * FROM store_timezones
        ORDER BY store_id
        LIMIT {store_ids_per_page} OFFSET {page * store_ids_per_page}'''
        cursor.execute(store_id_time_zones_query)
        store_id_time_zones = cursor.fetchall()
        index_iterator = 0
        start_time_ = time.time()
        page_result_store = []
        for store_id, curr_zone in store_id_time_zones:
            index_iterator += 1
            working_hour_query = f'''SELECT * FROM store_hours
            WHERE store_id = ?'''
            cursor.execute(working_hour_query, (store_id,))
            temp_res = cursor.fetchall()
            working_hours = store_hour_converter(temp_res)

            # getting all active (1) observations since 1 week before
            a_week_ago_obj = internal_curr_datetime_obj_utc - \
                timedelta(weeks=1)
            a_day_ago_obj = internal_curr_datetime_obj_utc - timedelta(days=1)
            an_hour_ago_obj = internal_curr_datetime_obj_utc - \
                timedelta(hours=1)
            an_hour_and_half_ago_obj = internal_curr_datetime_obj_utc - \
                timedelta(hours=1, minutes=30)
            a_week_str = a_week_ago_obj.strftime("%Y-%m-%d %H:%M:%S.%f UTC")
            a_day_str = a_day_ago_obj.strftime("%Y-%m-%d %H:%M:%S.%f UTC")
            an_hour_str = an_hour_ago_obj.strftime("%Y-%m-%d %H:%M:%S.%f UTC")
            an_hour_and_half_str = an_hour_and_half_ago_obj.strftime(
                "%Y-%m-%d %H:%M:%S.%f UTC")
            timestamp_since_week_ago_query = f'''SELECT * FROM store_pings
            WHERE store_id = ? AND recorded_at >= ? AND is_active = 1
            '''
            cursor.execute(timestamp_since_week_ago_query,
                           (store_id, a_week_str))
            timestamps_and_days = cursor.fetchall()
            timestamps_and_days.sort(key=lambda x: (x[3], x[0]))
            last_timestamp = None
            newday = True
            a_week_upticks = 0
            a_day_upticks = 0
            a_hour_upticks = 0
            a_local_day_str = utc_to_localtimestamp(curr_zone, a_day_str)
            a_local_hour_str = utc_to_localtimestamp(curr_zone, an_hour_str)
            a_local_hour_and_half_str = utc_to_localtimestamp(
                curr_zone, an_hour_and_half_str)
            for index in range(len(timestamps_and_days)):
                local_timestamp = utc_to_localtimestamp(
                    curr_zone, timestamps_and_days[index][3])

                # check if timestamp lies in working hours, if not, continue
                local_opening_time = f'{local_timestamp.split(" ")[0]} {working_hours[datetimeToDay(local_timestamp)][0]}.000000'
                local_closing_time = f'{local_timestamp.split(" ")[0]} {working_hours[datetimeToDay(local_timestamp)][1]}.000000'
                if local_timestamp < local_opening_time or local_timestamp > local_closing_time:
                    continue

                if last_timestamp == None or last_timestamp.split(" ")[0] != local_timestamp.split(" ")[0]:
                    # as new day is there, adding the last timestamp forward time covered from the closing hour
                    if index > 0 and last_timestamp != None:
                        temp_stamp = utc_to_localtimestamp(
                            curr_zone, timestamps_and_days[index-1][3])
                        temp_stamp = f'{temp_stamp.split(" ")[0]} {working_hours[datetimeToDay(temp_stamp)][1]}.000000'
                        (x, y) = datetimeDiff(temp_stamp, last_timestamp)
                        if x > 0 or y >= 30:
                            a_week_upticks += 30*60
                        else:
                            a_week_upticks += y*60

                    last_timestamp = f'{local_timestamp.split(" ")[0]} {working_hours[datetimeToDay(local_timestamp)][0]}.000000'
                    newday = True
                (h, m) = datetimeDiff(local_timestamp, last_timestamp)
                if local_timestamp >= a_local_hour_str:
                    a_hour_upticks = 60*60
                elif local_timestamp >= a_local_hour_and_half_str:
                    a_hour_upticks += (30 - datetimeDiff(local_timestamp,
                                       a_local_hour_and_half_str)[1])*60
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
                elif h == 0 and m > 30:
                    a_week_upticks += m*60
                    if local_timestamp > a_local_day_str:
                        a_day_upticks += m*60
                    continue

            final_output = calc_uptime_downtime(
                a_week_upticks, a_day_upticks, a_hour_upticks, working_hours, store_id, curr_zone, datetimeToDay(a_local_day_str))
            page_result_store.append(final_output)
            if index_iterator % 40 == 0:
                print(f'sample {index_iterator} of page {page+1} processed')
        end_time_ = time.time()
        print(f'{page+1} / {total_pages} completed, time taken for processing {page+1} page : {end_time_ - start_time_}')
        temp_df = pl.DataFrame(page_result_store, schema=schema, orient='row')
        result_df = result_df.vstack(temp_df)
    poor_time_end = time.time()
    print(f'Total time taken : {poor_time_end - poor_time_start}')
    result_df.write_csv(result_loc)


def report_processor():
    try:
        conn = sqlite3.connect(db_loc)
        cursor = conn.cursor()

        start_queries(conn, cursor)

        cursor.close()
        conn.close()

    except sqlite3.OperationalError as e:
        print("Failed to open database: ", e)


report_processor()
