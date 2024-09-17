from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


def run_script():
    import subprocess
    subprocess.run(["python", "task2.py"], check=True)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'daily_task',
    default_args=default_args,
    description='ETL DAG',
    schedule_interval=timedelta(days=1),  
    start_date=datetime(2024, 9, 17),     
    catchup=False,                       
)

run_task = PythonOperator(
    task_id='daily_opensky_update',
    python_callable=run_script,
    dag=dag,
)
