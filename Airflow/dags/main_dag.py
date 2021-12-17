from airflow import DAG
from datetime import datetime
from random import randint
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.bash import BashOperator
from airflow.contrib.operators import spark_submit_operator

from Scripts_airforlow.main import main



with DAG("main_dag",start_date=datetime(2021,1,1),schedule_interval='@daily',catchup=False) as dag:

    main_dag = PythonOperator(
        task_id = "main_dag",
        python_callable=main
    )
