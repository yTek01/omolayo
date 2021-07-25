from datatime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "admin", 
    "depends_on_past" : False, 
    "start_date" : datetime(2021, 7, 20),
    "email": ['olasehindeomolayo9@gmail.com'], 
    "email_on_failure":False, 
    "email_on_retry": False, 
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}
dag = DAG(
    "spotify_dag", 
    default_args= default_args, 
    description="Our first DAG With ETL process", 
    schedule_interval=timedelta(days=1),  
)
def just_a_function():
    print("I am going to show you something, just calm down")
    
run_etl = PythonOperator(
    task_id='whole_spotify_etl', 
    python_callable=just_a_function
    dag=dag
)

run_etl