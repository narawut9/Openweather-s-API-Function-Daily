# Weather Daily Summarize Pipeline  
Extarct hourly weather data from OpenWeather API, transfrom to daily summary,  and insert into PostgreSQL database.

---

## Project Structure

```bash
weather_etl_project/
├── main.py             # Main script 
├── process_weather.py  # ETL logic: DB connect, transform, insert
├── .env                # Environment variables (DB credentials)
├── requirements.txt    # Python dependencies
└── README.md           # Project overview and documentation
```
## Feature 
- **Extract**: Fetch hourly weather data in PostgreSQL
- **Transforms**: hourly → daily summary (AVG, MAX, MIN, SUM)
- **Local timezone**: (Asia/Bangkok) → UTC Range
- **Function safe_cast**: Safe cast & error handling for clean data
- **Loads**: Loads into `tblWeather_daily_ow` (PostgreSQL)
- **Logging**: Structured logging with `logging` module

---
### 1. Clone the repo

```yaml
git clone https://github.com/yourusername/weather_etl_project.git
```

## Create and activate virtual environment

```bash
python -m venv venv 
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

## Install dependencies
```bash
pip install -r requirements.txt
```

## Run the Pipeline

```bash
python main.py
```

## Example Insert Output

```yaml
2025-07-31 11:40:08,235 | INFO | === Start Weather ETL ===
2025-07-31 11:40:08,591 | INFO | Connect to PostgreSQL database successfully
2025-07-31 11:40:08,625 | INFO | Summarizing weather for 2025-07-30 (UTC Range: 2025-07-29 17:00:00+00:00 → 2025-07-30 16:59:59+00:00)
2025-07-31 11:40:08,722 | INFO | Fetched 124 station summaries.
2025-07-31 11:40:08,761 | INFO | Complete to Inserted summarized weather data. (WeatherStationID: 2561, DateTime = 2025-07-30 00:00:00+00:00)
...
2025-07-31 11:40:13,041 | INFO | ETL Completed
2025-07-31 11:40:13,041 | INFO | Total successful inserts: 124
2025-07-31 11:40:13,042 | INFO | Closed database connection.
2025-07-31 11:40:13,042 | INFO | Process Completed
```

## Flowchart (Daily Summary)

```mermaid
flowchart TD
    A[Start] --> B[Connect to PostgreSQL]
    B --> C[Get UTC Range for Yesterday - from local to UTC]
    C --> D[Query hourly data from tblWeather_hourly_ow]
    D --> E[Transform to daily summary: AVG temp, humidity, MAX temp_max/min, SUM rainfall, Latest description]
    E --> F[Loop through stations]
    F --> G[Safe cast values: float, str, etc.]
    G --> H[Insert into tblWeather_daily_ow]
    H --> I{More stations?}
    I -- Yes --> F
    I -- No --> J[Commit transaction]
    J --> K[Close DB connection]
    K --> L[Done]