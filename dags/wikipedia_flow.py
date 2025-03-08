from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from pipeline.wikipedia_pipeline import get_wikipedia_page

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
extract_data_from_wikipedia = PythonOperator(
    task_id = "extract_data_from_wikipedia",
    python_callable = get_wikipedia_page,
    provide_context = True,
    op_kwargs = {"url": "https://en.wikipedia.org/wiki/List_of_association_football_stadiums_by_capacity"},
    dag= dag
)
# 
#preprocessing
#write