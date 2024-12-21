
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
# from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

# Створення DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    'project_solution',
    default_args=default_args,
    description='A simple data pipeline',
    schedule_interval='@daily',
    tags=['MZh']
)

# Завдання для виконання скриптів
landing_to_bronze = SparkSubmitOperator(
    application='dags/MZh/landing_to_bronze.py',
    task_id='landing_to_bronze',
    conn_id='spark_default', 
    verbose=1, 
    conf={'spark.master': 'local'},
    dag=dag,
)

bronze_to_silver = SparkSubmitOperator(
    application='dags/MZh/bronze_to_silver.py',
    task_id='bronze_to_silver',
    conn_id='spark_default', 
    verbose=1, 
    conf={'spark.master': 'local'},
    dag=dag,
)

silver_to_gold = SparkSubmitOperator(
    application='dags/MZh/silver_to_gold.py',
    task_id='silver_to_gold',
    conn_id='spark_default', 
    verbose=1, 
    conf={'spark.master': 'local'},
    dag=dag,
)

# Послідовність виконання завдань
landing_to_bronze >> bronze_to_silver >> silver_to_gold
