import logging
import psycopg2
import pytz
from datetime import datetime, timezone ,timedelta ,time
from dotenv import load_dotenv
import os
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)


def get_utc_range_for_yesterday_local():
    try:
        tz_bkk = pytz.timezone('Asia/Bangkok')
        local_now = datetime.now(tz_bkk)
        local_yesterday = local_now.date() - timedelta(days=1)

        local_start = tz_bkk.localize(datetime.combine(local_yesterday, time(0, 0, 0)))
        local_end = tz_bkk.localize(datetime.combine(local_yesterday, time(23, 59, 59)))

        start_time = local_start.astimezone(timezone.utc)
        end_time = local_end.astimezone(timezone.utc)
        return local_yesterday, start_time, end_time
    except Exception as e:
        logging.error(e)

# Connect to DB
load_dotenv()
DB_CONFIG = {
    'host' : os.getenv("DB_HOST"),
    'dbname' : os.getenv("DB_NAME"),
    'user' : os.getenv("DB_USER"),
    'password' : os.getenv("DB_PASS"),
    'port' : os.getenv("DB_PORT")
}

def get_db_connection(DB_CONFIG):
    conn=None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logging.info("Connect to PostgreSQL database successfully")
        return conn
    except psycopg2.Error as e:
        logging.error(f"[ERROR] Failed to connect to Database : {e}")
        exit(1)

# Get connect 
conn = get_db_connection(DB_CONFIG)

def summarize_daily_weather():
    cur = conn.cursor()
    local_date, start_time, end_time = get_utc_range_for_yesterday_local()
    logging.info(f"Summarizing weather for {local_date} (UTC Range: {start_time} → {end_time})")
    sql_query = """
        SELECT 
            weather_station_id, 
            %s::timestamp WITH TIME ZONE AS obs_datetime,
            AVG(temp)        AS temp,
            MAX(temp_max)    AS temp_max,
            MIN(temp_min)    AS temp_min,
            AVG(feels_like)  AS feels_like,
            AVG(humidity)    AS humidity,
            AVG(pressure)    AS pressure,
            SUM(COALESCE(rain_1h, 0)) AS rainfall,
            AVG(wind_speed)  AS wind_speed,
            MAX(wind_gust)   AS wind_gust,
            AVG(cloud_all)   AS cloud,
            (ARRAY_AGG(weather_description ORDER BY obs_datetime DESC))[1] AS weather_description
        FROM weather."tblWeather_hourly_ow"
        WHERE obs_datetime >= %s AND obs_datetime <= %s
        GROUP BY weather_station_id;
    """
    try:
        cur.execute(sql_query,(local_date, start_time, end_time))
        rows = cur.fetchall()
        cur.close()
        logging.info(f"Fetched {len(rows)} station summaries.")
        return rows
    except Exception as e:
        logging.error(f"Error during summary query: {e}")
        return []

# Insert data 

def process_weather_data():
    cur = conn.cursor()
    weather_daily = summarize_daily_weather()
    create_by = 'Narawut.T'
    update_by = 'Narawut.T'
    create_date = datetime.now(timezone.utc)
    update_date = datetime.now(timezone.utc)

    count_stations = 0
    for row in weather_daily:
        try:
            weather_station_id = int(row[0])
            obs_datetime = str(row[1])

            try: temp = float(row[2])
            except: temp = None

            try: temp_max = float(row[3])
            except: temp_max  = None

            try: temp_min = float(row[4])
            except: temp_min = None

            try: feels_like = float(row[5])
            except: feels_like = None

            try: humidity = float(row[6])
            except: humidity = None

            try: pressure = float(row[7])
            except: pressure = None

            try: rainfall = float(row[8])
            except: rainfall = None

            try: wind_speed = float(row[9])
            except: wind_speed = None

            try: wind_gust_max = float(row[10])
            except: wind_gust_max = None

            try: cloud = float(row[11])
            except: cloud = None

            try: weather_description = str(row[12])
            except: weather_description = None

            # Insert SQL
            sql_query = '''
                INSERT INTO weather."tblWeather_daily_ow"(
                    weather_station_id, obs_datetime, temp, temp_max, temp_min, 
                    feels_like, humidity, pressure, rainfall, wind_speed, 
                    wind_gust_max, cloud, weather_description, create_by, create_date, 
                    update_by, update_date)
                VALUES (
                %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, 
                %s, %s);
            '''
            cur.execute(sql_query, (
                weather_station_id, obs_datetime, temp, temp_max, temp_min,
                feels_like, humidity, pressure, rainfall,
                wind_speed, wind_gust_max, cloud,
                weather_description,
                 create_by, create_date, update_by, update_date
            ))
            count_stations +=1
            logging.info(f"Complete to Inserted summarized weather data. (WeatherStationID: {weather_station_id}, DateTime = {obs_datetime})")

        except Exception as e:
            conn.rollback()
            logging.error(f" Insert error: StationID {weather_station_id} {e}")
    
    conn.commit()
    cur.close()


    logging.info("ETL Completed")
    logging.info(f"Total successful inserts: {count_stations}")

process_weather_data()