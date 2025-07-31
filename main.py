import logging
from datetime import datetime
from process_weather import process_weather_data, get_db_connection, DB_CONFIG

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

def main():
    logging.info("=== Start Weather ETL ===")
    conn = get_db_connection(DB_CONFIG)

    try:
        process_weather_data(conn)
    except Exception as e:
        logging.error(f"[MAIN ERROR] {e}")
    finally:
        conn.close()
        logging.info("Closed database connection.")

    logging.info("Process Completed")

if __name__ == "__main__":
    main()