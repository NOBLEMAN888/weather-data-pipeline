from __future__ import annotations

from datetime import datetime, timedelta
import os
import json
from pathlib import Path

import numpy as np

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


# --------- TRANSFORM ---------
def transform_data(ds: str, **_):
    """
    Читает сырой JSON за дату (../data/{ds}.json), извлекает нужные поля
    и возвращает нормализованную запись (dict) через XCom.
    """
    # Путь к файлу относительно текущего файла DAG
    data_dir = Path(os.path.dirname(__file__)).parent / "data"
    raw_path = data_dir / f"{ds}.json"

    if not raw_path.exists():
        raise FileNotFoundError(f"Raw JSON not found: {raw_path}")

    with open(raw_path, "r", encoding="utf-8") as f:
        doc = json.load(f)

    city        = str(doc["name"])
    country     = str(doc["sys"]["country"])
    lat         = float(doc["coord"]["lat"])
    lon         = float(doc["coord"]["lon"])
    humid       = float(doc["main"]["humidity"])
    press       = float(doc["main"]["pressure"])
    min_temp    = float(doc["main"]["temp_min"]) - 273.15
    max_temp    = float(doc["main"]["temp_max"]) - 273.15
    temp        = float(doc["main"]["temp"]) - 273.15
    weather     = str(doc["weather"][0]["description"])
    todays_date = ds  # дата запуска DAG (строка YYYY-MM-DD)

    numeric_values = [lat, lon, humid, press, min_temp, max_temp, temp]
    if any(np.isnan(numeric_values)):
        raise ValueError("Found NaN in numeric fields after parsing")

    return {
        "city": city,
        "country": country,
        "latitude": lat,
        "longitude": lon,
        "todays_date": todays_date,
        "humidity": humid,
        "pressure": press,
        "min_temp": min_temp,
        "max_temp": max_temp,
        "temp": temp,
        "weather": weather,
    }


# --------- LOAD ---------
def load_to_postgres(ti, **_):
    """
    Достаёт нормализованную запись из XCom предыдущего таска и вставляет в Postgres.
    Connection id: weather_id (настрой в Airflow Connections).
    """
    row = ti.xcom_pull(task_ids="transform_data")
    if not row:
        raise ValueError("No data received from transform_data via XCom")

    pg = PostgresHook(postgres_conn_id="weather_id")

    insert_cmd = """
        INSERT INTO weather_table
            (city, country, latitude, longitude,
             todays_date, humidity, pressure,
             min_temp, max_temp, temp, weather)
        VALUES
            (%(city)s, %(country)s, %(latitude)s, %(longitude)s,
             %(todays_date)s, %(humidity)s, %(pressure)s,
             %(min_temp)s, %(max_temp)s, %(temp)s, %(weather)s);
    """

    pg.run(insert_cmd, parameters=row)


# --------- DAG ---------
default_args = {
    "owner": "Matthew",
    "depends_on_past": False,
    "email": ["matveymonakhov@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 5,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="weatherDag",
    default_args=default_args,
    start_date=datetime(2025, 9, 14),
    schedule_interval=timedelta(days=1), # ежедневный запуск
    catchup=False,
    tags=["weather", "etl"],
) as dag:

    get_weather = BashOperator(
        task_id="get_weather",
        bash_command="python ./helpers/get_weather.py",
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    load = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
    )

    get_weather >> transform >> load
