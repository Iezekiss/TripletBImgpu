"""
DAG для анализа задержек авиарейсов
Вариант 11
"""

from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import os
import shutil
import io


# ------------------------------------------------------
# DEFAULT ARGS
# ------------------------------------------------------

default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


# ------------------------------------------------------
# DAG
# ------------------------------------------------------

dag = DAG(
    dag_id="variant_11_flight_delays",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="ETL по авиарейсам: задержки, Kaggle → Postgres → витрина",
    tags=['etl', 'flights', 'variant_11']
)


# ------------------------------------------------------
# TASK 1 — EXTRACT
# ------------------------------------------------------

def extract_from_kaggle(**context):
    """
    Extract: скачивание датасета с Kaggle через kagglehub.
    """
    import kagglehub

    DATA_DIR = "/opt/airflow/dags/data"
    os.makedirs(DATA_DIR, exist_ok=True)

    kaggle_json = "/home/airflow/.kaggle/kaggle.json"
    if not os.path.exists(kaggle_json):
        raise FileNotFoundError(f"kaggle.json не найден в {kaggle_json}")

    print("Kaggle авторизация найдена.")

    dataset = "usdot/flight-delays"
    print(f"Скачиваем датасет: {dataset}")

    path = kagglehub.dataset_download(dataset)
    print("Скачано в:", path)

    source = os.path.join(path, "flights.csv")
    dest = os.path.join(DATA_DIR, "flights.csv")

    shutil.copy2(source, dest)
    print(f"Файл скопирован в: {dest}")

    context["task_instance"].xcom_push(key="csv_path", value=dest)
    return dest


extract_task = PythonOperator(
    task_id="extract_from_kaggle",
    python_callable=extract_from_kaggle,
    dag=dag
)


# ------------------------------------------------------
# TASK 2 — LOAD RAW
# ------------------------------------------------------

def load_raw_to_postgres(**context):
    """
    Load Raw: загрузка в Postgres через COPY, ограничение 200k строк.
    """
    print("Загрузка сырых данных (ограничено по строкам)...")

    csv_path = context["task_instance"].xcom_pull(
        key="csv_path",
        task_ids="extract_from_kaggle"
    )

    LIMIT = 200_000
    print(f"Лимит загрузки: {LIMIT} строк")

    hook = PostgresHook(postgres_conn_id='analytics_postgres')
    conn = hook.get_conn()
    cursor = conn.cursor()

    print("Создаём raw_flights_11...")

    cursor.execute("DROP TABLE IF EXISTS raw_flights_11 CASCADE;")
    cursor.execute("""
        CREATE TABLE raw_flights_11 (
            YEAR INT,
            MONTH INT,
            DAY INT,
            DAY_OF_WEEK INT,
            AIRLINE TEXT,
            FLIGHT_NUMBER TEXT,
            TAIL_NUMBER TEXT,
            ORIGIN TEXT,
            DESTINATION TEXT,
            SCHEDULED_DEPARTURE INT,
            DEPARTURE_TIME INT,
            DEPARTURE_DELAY INT,
            TAXI_OUT INT,
            WHEELS_OFF INT,
            SCHEDULED_TIME INT,
            ELAPSED_TIME INT,
            AIR_TIME INT,
            DISTANCE INT,
            WHEELS_ON INT,
            TAXI_IN INT,
            SCHEDULED_ARRIVAL INT,
            ARRIVAL_TIME INT,
            ARRIVAL_DELAY INT,
            DIVERTED INT,
            CANCELLED INT,
            CANCELLATION_REASON TEXT,
            AIR_SYSTEM_DELAY INT,
            SECURITY_DELAY INT,
            AIRLINE_DELAY INT,
            LATE_AIRCRAFT_DELAY INT,
            WEATHER_DELAY INT
        );
    """)

    conn.commit()

    print("COPY FROM — начинаем чтение построчно...")

    inserted = 0
    with open(csv_path, "r") as f:
        next(f)  # пропускаем заголовок
        for line in f:
            if inserted >= LIMIT:
                break

            # COPY ... FROM STDIN WITH CSV NULL ''
            copy_sql = """
                COPY raw_flights_11
                FROM STDIN
                WITH (
                    FORMAT CSV,
                    NULL ''
                );
            """

            cursor.copy_expert(copy_sql, io.StringIO(line))
            inserted += 1

            if inserted % 50_000 == 0:
                print(f"Загружено {inserted} строк...")


    conn.commit()
    cursor.close()
    conn.close()

    print(f"Готово: загружено {inserted} строк")


load_task = PythonOperator(
    task_id="load_raw_to_postgres",
    python_callable=load_raw_to_postgres,
    dag=dag
)


# ------------------------------------------------------
# TASK 3 — TRANSFORM (витрина)
# ------------------------------------------------------

transform_task = PostgresOperator(
    task_id="create_datamart",
    postgres_conn_id="analytics_postgres",
    sql="sql/datamart_variant_11.sql",
    dag=dag
)


# ------------------------------------------------------
# TASK DEPENDENCIES
# ------------------------------------------------------

extract_task >> load_task >> transform_task
