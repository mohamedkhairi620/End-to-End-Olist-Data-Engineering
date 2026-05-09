from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'engin',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 8),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='olist_data_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['olist', 'final_project'],
) as dag:

    t_bronze = BashOperator(
        task_id='bronze_ingestion',
        bash_command='docker exec spark-jupyter spark-submit /home/jovyan/work/ingestion/bronze_ingestion.py',
    )

    t_feature = BashOperator(
        task_id='feature_engineering',
        bash_command='docker exec spark-jupyter spark-submit /home/jovyan/work/transformation/feature_engineering.py',
    )

    t_gold = BashOperator(
        task_id='silver_to_gold',
        bash_command='docker exec spark-jupyter spark-submit /home/jovyan/work/transformation/silver_to_gold.py',
    )

    t_snowflake = BashOperator(
        task_id='load_to_snowflake',
        bash_command='docker exec spark-jupyter spark-submit --jars /home/jovyan/work/jars/spark-snowflake_2.12-3.1.1.jar,/home/jovyan/work/jars/snowflake-jdbc-3.14.4.jar /home/jovyan/work/loading/load_to_snowflake.py',
    )

    t_bronze >> t_feature >> t_gold >> t_snowflake