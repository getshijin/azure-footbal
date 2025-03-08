from airflow import DAG

dag =DAG(
    dag_id='wikipedia_flow',
    default_args={
        "owner": "Shijin",
        "start_date": datetime(2023,3,8)
    },
    schedule_interval= None,
    catchup=False
)

#Extraction
#preprocessing
#write