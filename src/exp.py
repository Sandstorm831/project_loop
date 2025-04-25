import sqlite3
import os
from sqlite3.dbapi2 import Connection, Cursor
import math
import polars as pl
import time
filePath = os.path.abspath(__file__)
db_loc = filePath.split("project_loop")[0] + "project_loop/ingestor.db"
result_loc = filePath.split("project_loop")[0] + "project_loop/report_iter2.csv"
from datetime import datetime, timedelta
from dateutil import tz

internal_curr_datetime = "2024-10-14 23:55:18.727055 UTC"
internal_curr_datetime_obj_utc = datetime.strptime(internal_curr_datetime, "%Y-%m-%d %H:%M:%S.%f UTC").replace(tzinfo=tz.gettz('UTC'))

def store_hour_converter(temp_res):
    x = [["00:01:00", "23:59:59"] for i in range(7)]
    if len(temp_res) == 0:
        return x
    for obj in temp_res:
        x[obj[2]] = [obj[3], obj[4]]
    return x

def polars_store_hour_converter(temp_res: pl.DataFrame):
    x = [["00:01:00", "23:59:59"] for i in range(7)]
    if temp_res.height == 0:
        return x
    for obj in temp_res.rows():
        x[obj[2]] = [obj[3], obj[4]]
    return x

TheBigSchema = {
    "store_id": pl.String,
    "utc_pings": pl.String, 
    "timezone": pl.String,
    "local_pings": pl.String,
    "local_weekday": pl.Int8,
    "opening_hour": pl.String,
    "closing_hour": pl.String,
}


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
        # print(temp)
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

def tester():
    try:
        conn = sqlite3.connect(db_loc)
        cursor = conn.cursor()
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
        store_ids_per_page = 2000
        cursor.execute(total_store_id_query)
        total_store_ids = cursor.fetchone()[0]
        total_pages = math.ceil(total_store_ids / store_ids_per_page)
        
        # getting all active (1) observations since 1 week before
        a_week_ago_obj = internal_curr_datetime_obj_utc - timedelta(weeks=1)
        a_day_ago_obj = internal_curr_datetime_obj_utc - timedelta(days=1)
        an_hour_ago_obj = internal_curr_datetime_obj_utc - timedelta(hours=1)
        an_hour_and_half_ago_obj = internal_curr_datetime_obj_utc - timedelta(hours=1, minutes=30)
        a_week_str =  a_week_ago_obj.strftime("%Y-%m-%d %H:%M:%S.%f UTC")
        a_day_str = a_day_ago_obj.strftime("%Y-%m-%d %H:%M:%S.%f UTC")
        an_hour_str = an_hour_ago_obj.strftime("%Y-%m-%d %H:%M:%S.%f UTC")
        an_hour_and_half_str = an_hour_and_half_ago_obj.strftime("%Y-%m-%d %H:%M:%S.%f UTC")
        poor_time_start = time.time()
        for page in range(total_pages):
            # store_id_time_zones_query = f'''SELECT * FROM store_timezones
            # ORDER BY store_id
            # LIMIT {store_ids_per_page} OFFSET {page * store_ids_per_page}'''
            # cursor.execute(store_id_time_zones_query)
            # store_id_time_zones = cursor.fetchall()
            # page_store_ids = [tup[0] for tup in store_id_time_zones]
            # a_week_ago_obj = internal_curr_datetime_obj_utc - timedelta(weeks=1)
            # a_week_str =  a_week_ago_obj.strftime("%Y-%m-%d %H:%M:%S.%f UTC")
            ################
            store_id_time_zones_query = '''SELECT * FROM store_timezones
            ORDER BY store_id
            LIMIT ? OFFSET ?'''
            timezone__df = pl.read_database(store_id_time_zones_query, connection=conn, execute_options={"parameters": [store_ids_per_page, page * store_ids_per_page]})
            page_store_ids = timezone__df["store_id"].to_list()
            a_week_ago_obj = internal_curr_datetime_obj_utc - timedelta(weeks=1)
            a_week_str =  a_week_ago_obj.strftime("%Y-%m-%d %H:%M:%S.%f UTC")
            
            store_id_placeholder = ",".join("?" for _ in range(timezone__df.height))
            working_hour_query = f'''SELECT * FROM store_hours
            WHERE store_id in ({store_id_placeholder})
            ORDER BY store_id ASC'''
            hours_df = pl.read_database(query=working_hour_query, connection=conn, execute_options={"parameters": page_store_ids})
            timestamp_since_week_ago_query = f'''SELECT store_id, recorded_at FROM store_pings
            WHERE store_id in ({store_id_placeholder}) AND recorded_at >= ? AND is_active = 1
            ORDER BY store_id ASC'''
            page_store_ids.append(a_week_str)
            pings_df = pl.read_database(query=timestamp_since_week_ago_query, connection=conn, execute_options={"parameters": page_store_ids})
            page_result_store = []
            
            for row in timezone__df.iter_rows():
                store_id = row[0]
                curr_zone = row[1]
                working_hours = hours_df.filter(
                    pl.col('store_id') ==  store_id
                ).sort("week_day")
                working_hours = polars_store_hour_converter(working_hours)
                timestamps_and_days = pings_df.filter(
                    pl.col('store_id') ==  store_id
                ).sort('recorded_at')
                timestamps_and_days = [list(rows) for rows in timestamps_and_days.rows()]
                # print(timestamps_and_days)
                # break
            # print(pings_df.rows)
            # TheBigDF = pings_df.select(
            #     pl.col('store_id'),
            #     pl.col('recorded_at').alias('utc_pings'),
            # )
            # TheBigDF = TheBigDF.join(timezone__df, on="store_id", how="left")
            # TheBigDF = TheBigDF.with_columns(
            #     (pl.col('utc_pings') + "/" + pl.col('timezone')).alias('utc/timezone')
            # )
            # # print(TheBigDF)
            # TheBigDF = TheBigDF.with_columns(
            #     # (datetime.strptime(pl.col('utc_pings'),"%Y-%m-%d %H:%M:%S.%f UTC").replace(tzinfo=tz.gettz('UTC')).astimezone(tz.gettz(pl.col('timezone'))).strftime("%Y-%m-%d %H:%M:%S.%f")).alias('local_pings')
            #     # pl.col('utc_pings').str.to_datetime("%Y-%m-%d %H:%M:%S.%f UTC").dt.convert_time_zone(time_zone=pl.col('timezone').str.split(by="UTC")).alias("local_pings")
            #     # pl.col('utc/timezone').pipe(polars_Timezone_to_localtimestamp).alias("local_pings")
            # )
            # print(TheBigDF)
            
            
            
            ################
            # break
            
            # for store_id, curr_zone in store_id_time_zones:
                # working_hour_query = f'''SELECT * FROM store_hours
                # WHERE store_id = ?'''
                # cursor.execute(working_hour_query, (store_id,))
                # temp_res = cursor.fetchall()
                # working_hours = store_hour_converter(temp_res)

                # timestamp_since_week_ago_query = f'''SELECT * FROM store_pings
                # WHERE store_id = ? AND recorded_at >= ? AND is_active = 1
                # '''
                # cursor.execute(timestamp_since_week_ago_query, (store_id, a_week_str))
                # timestamps_and_days = cursor.fetchall()
                # timestamps_and_days.sort(key = lambda x: (x[3], x[0]))
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
                        curr_zone, timestamps_and_days[index][1])

                    # check if timestamp lies in working hours, if not, continue
                    local_opening_time = f'{local_timestamp.split(" ")[0]} {working_hours[datetimeToDay(local_timestamp)][0]}.000000'
                    local_closing_time = f'{local_timestamp.split(" ")[0]} {working_hours[datetimeToDay(local_timestamp)][1]}.000000'
                    if local_timestamp < local_opening_time or local_timestamp > local_closing_time:
                        continue

                    if last_timestamp == None or last_timestamp.split(" ")[0] != local_timestamp.split(" ")[0]:
                        # as new day is there, adding the last timestamp forward time covered from the closing hour
                        if index > 0 and last_timestamp != None:
                            temp_stamp = utc_to_localtimestamp(
                                curr_zone, timestamps_and_days[index-1][1])
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
                
                # break
            print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
            temp_df = pl.DataFrame(page_result_store, schema=schema, orient='row')
            result_df = result_df.vstack(temp_df)
        poor_time_end = time.time()
        result_df.write_csv(result_loc)
        print(f'Processing completed, Total time taken : {poor_time_end - poor_time_start}')

        cursor.close()
        conn.close()

    except sqlite3.OperationalError as e :
        print("Failed to open database: ", e)
    return

def convertint_csv_to_parquet():
    raw_path = os.path.abspath(__file__)
    timezones_path = raw_path.split("project_loop")[0] + "project_loop/data/timezones.csv"
    timezones_path_p = raw_path.split("project_loop")[0] + "project_loop/data/timezones.parquet"
    ping_path = raw_path.split("project_loop")[0] + "project_loop/data/store_status.csv"
    ping_path_p = raw_path.split("project_loop")[0] + "project_loop/data/store_status.parquet"
    hours_path = raw_path.split("project_loop")[0] + "project_loop/data/menu_hours.csv"
    hours_path_p = raw_path.split("project_loop")[0] + "project_loop/data/menu_hours.parquet"
    pl.scan_csv(timezones_path).sink_parquet(timezones_path_p)
    pl.scan_csv(ping_path).sink_parquet(ping_path_p)
    pl.scan_csv(hours_path).sink_parquet(hours_path_p)


convertint_csv_to_parquet()
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
