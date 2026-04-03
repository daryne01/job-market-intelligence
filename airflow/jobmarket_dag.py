from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

PROJECT_DIR = r"C:\Users\bouad\job-market-intelligence"
PYTHON = r"C:\Users\bouad\venv\Scripts\python.exe"

default_args = {
    'owner': 'daryne',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='job_market_daily_pipeline',
    default_args=default_args,
    description='Daily UK job market intelligence pipeline',
    schedule_interval='0 13 * * *',
    start_date=datetime(2026, 4, 3),
    catchup=False,
    tags=['job-market', 'data-engineering'],
) as dag:

    scrape = BashOperator(
        task_id='scrape_reed_jobs',
        bash_command=f'cd {PROJECT_DIR} && {PYTHON} scraper/reed_scraper.py',
    )

    extract = BashOperator(
        task_id='extract_with_openai',
        bash_command=f'cd {PROJECT_DIR} && {PYTHON} extractor.py',
    )

    sponsors = BashOperator(
        task_id='refresh_sponsor_register',
        bash_command=f'cd {PROJECT_DIR} && {PYTHON} sponsor_register.py',
    )

    models = BashOperator(
        task_id='run_dbt_models',
        bash_command=f'cd {PROJECT_DIR} && {PYTHON} run_models.py',
    )

    scrape >> extract >> sponsors >> models
