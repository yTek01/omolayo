try: 
    from datetime import timedelta
    from airflow import DAG 
    from airflow.operators.python_operator import PythonOperator
    from datetime import datetime
    print("All DAG Modules are ok. . .")
    
except Exception as e: 
    print("Error {}".format(e))
    
    
def first_function_execute():
    print("Hello world ")
    
    return "Hello World"

with DAG(
    dag_id="initial_dag", 
    schedule_interval="@daily",
    default_args={
        "owner":"admin", 
        "retries":1,
        "retry_delay": timedelta(minutes=5),
        "start_date": datetime(2021, 1, 1), 
    }, 
    catchup=False) as f:
    
    first_function_execute = PythonOperator(
        task_id="first_function_execute", python_callable=first_function_execute)
