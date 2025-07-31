import logging
import psycopg2
import pytz
from datetime import datetime, timezone ,timedelta ,time
from dotenv import load_dotenv
import os

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
        return None,  None, None

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

def summarize_daily_weather(conn):
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
        with conn.cursor() as cur:
            cur.execute(sql_query,(local_date, start_time, end_time))
            rows = cur.fetchall()
        logging.info(f"Fetched {len(rows)} station summaries.")
        return rows
    except Exception as e:
        logging.error(f"Error during summary query: {e}")
        return []

# Insert data 
def process_weather_data(conn):
    def safe_cast(val, cast_type=float, default=None):
        try:
            return cast_type(val)
        except (ValueError, TypeError):
            return default

    weather_daily = summarize_daily_weather(conn)
    create_by = 'Narawut.T'
    update_by = 'Narawut.T'
    create_date = datetime.now(timezone.utc)
    update_date = datetime.now(timezone.utc)

    count_stations = 0
    with conn.cursor() as cur:
        for row in weather_daily:
            try:
                weather_station_id = safe_cast(row[0], int)
                obs_datetime = row[1] 

                temp = safe_cast(row[2])
                temp_max = safe_cast(row[3])
                temp_min = safe_cast(row[4])
                feels_like = safe_cast(row[5])
                humidity = safe_cast(row[6])
                pressure = safe_cast(row[7])
                rainfall = safe_cast(row[8])
                wind_speed = safe_cast(row[9])
                wind_gust_max = safe_cast(row[10])
                cloud = safe_cast(row[11])
                weather_description = safe_cast(row[12], str)

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
                count_stations += 1
                logging.info(f"Complete to Inserted summarized weather data. (WeatherStationID: {weather_station_id}, DateTime = {obs_datetime})")

            except Exception as e:
                conn.rollback()
                logging.error(f" Insert error: StationID {weather_station_id} {e}")
    
        conn.commit()
    logging.info("ETL Completed")
    logging.info(f"Total successful inserts: {count_stations}")