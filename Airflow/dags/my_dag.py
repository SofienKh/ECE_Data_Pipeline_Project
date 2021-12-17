from airflow import DAG
from datetime import datetime
from random import randint
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.bash import BashOperator
from airflow.contrib.operators import spark_submit_operator

from Scripts_airforlow.Le_pen import main_lepen
from Scripts_airforlow.Yannick_Jadot import main_jadot
from Scripts_airforlow.Valerie_Pecresse import main_pecresse
from Scripts_airforlow.Jeanluc_melenchon import main_melenchon
from Scripts_airforlow.Jean_Lasalle import main_lasalle
from Scripts_airforlow.François_Asselineau import main_francois
from Scripts_airforlow.Florian_Philippot import main_florian
from Scripts_airforlow.Fabien_Roussel import main_fabien
from Scripts_airforlow.Eric_Zemmour import main_eric
from Scripts_airforlow.Emmanuel_Macron import main_macron
from Scripts_airforlow.Arthaud_Nathalie import main_arthaud
from Scripts_airforlow.Arnaud_Montebourg import main_montebourg
from Scripts_airforlow.Anne_Hidalgo import main_hidalgo


with DAG("dag_of_spark_files",start_date=datetime(2021,1,1),schedule_interval='*/30 * * * *',catchup=False) as dag:

    Le_pen = PythonOperator(
        task_id = "Mariepen",
        python_callable=main_lepen
    )
    Yannick_Jadot = PythonOperator(
        task_id = "Yannick_Jadot",
        python_callable=main_jadot
    )
    Valerie_Pecresse = PythonOperator(
        task_id = "Valerie_Pecresse",
        python_callable=main_pecresse
    )
    Jeanluc_melenchon = PythonOperator(
        task_id = "Jeanluc_melenchon",
        python_callable=main_melenchon)

    Jean_Lasalle = PythonOperator(
        task_id = "Jean_Lasalle",
        python_callable=main_lasalle
    )
    François_Asselineau = PythonOperator(
        task_id = "Francois_Asselineau",
        python_callable=main_francois
    )
    Florian_Philippot = PythonOperator(
        task_id = "Florian_Philippot",
        python_callable=main_florian
    )
    Fabien_Roussel = PythonOperator(
        task_id = "Fabien_Roussel",
        python_callable=main_fabien
    )
    Eric_Zemmour = PythonOperator(
        task_id = "Eric_Zemmour",
        python_callable=main_eric
    )    
    Emmanuel_Macron = PythonOperator(
        task_id = "Emmanuel_Macron",
        python_callable=main_macron
    )    
    Arthaud_Nathalie = PythonOperator(
        task_id = "Arthaud_Nathalie",
        python_callable=main_arthaud
    )    
    Arnaud_Montebourg = PythonOperator(
        task_id = "Arnaud_Montebourg",
        python_callable=main_montebourg
    )    
    Anne_Hidalgo = PythonOperator(
        task_id = "Anne_Hidalgo",
        python_callable=main_hidalgo
    )    

Le_pen  >> Yannick_Jadot >> Valerie_Pecresse >> Jeanluc_melenchon >> Jean_Lasalle >> François_Asselineau >> Florian_Philippot >> Fabien_Roussel >> Eric_Zemmour >> Emmanuel_Macron >> Arthaud_Nathalie >> Arnaud_Montebourg >> Anne_Hidalgo