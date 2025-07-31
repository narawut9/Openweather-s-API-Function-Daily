# Weather Daily Summarize Pipeline  
Extarct hourly weather data from OpenWeather APYI, transfrom to daily summary,  and insert into PostgreSQL database.

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

```bash
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

---

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
