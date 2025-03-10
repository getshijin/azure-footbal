import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.wikipedia_pipeline import extract_wikipedia_data, transform_wikipedia_data, write_wikipedia_data

dag =DAG(
    dag_id='wikipedia_flow',
    default_args={
        "owner": "Shijin",
        "start_date": datetime(2023,3,8)
    },
    schedule_interval= None,
    catchup=False
)

#Extraction from wikipedia
extract_data_task = PythonOperator(
    task_id = "extract_data_task_id",
    provide_context = True,
    python_callable = extract_wikipedia_data,
    op_kwargs = {"url": "https://en.wikipedia.org/wiki/List_of_association_football_stadiums_by_capacity"},
    dag= dag
)
# 
#preprocessing
transform_task = PythonOperator(
    task_id = 'transform_task_id',
    provide_context = True,
    python_callable = transform_wikipedia_data,
    dag = dag
)

#write

write_task =PythonOperator(
    task_id = "write_task_id",
    provide_context =True,
    python_callable= write_wikipedia_data,
    dag = dag
)

extract_data_task >> transform_task >> write_task
 