from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
}

with DAG(
    dag_id='orchestrate_pipeline',
    default_args=default_args,
    description='Start producer -> consumer -> silver -> gold',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    run_producer = BashOperator(
        task_id='run_producer',
        bash_command='python /opt/airflow/workspace/producer.py',
    )

    run_consumer = BashOperator(
        task_id='run_consumer',
        bash_command='python /opt/airflow/workspace/consumer.py',
    )

    test_after_consumer = BashOperator(
        task_id='test_consumer_to_silver',
        bash_command='python /opt/airflow/workspace/scripts/data_tests.py --boundary consumer_silver',
    )

    run_silver = BashOperator(
        task_id='run_silver',
        bash_command='python /opt/airflow/workspace/medallion/silver/silver.py',
    )

    test_after_silver = BashOperator(
        task_id='test_silver_to_gold',
        bash_command='python /opt/airflow/workspace/scripts/data_tests.py --boundary silver_gold',
    )

    run_gold = BashOperator(
        task_id='run_gold',
        bash_command='python /opt/airflow/workspace/medallion/gold/gold.py',
    )

    test_after_gold = BashOperator(
        task_id='test_gold_to_dim',
        bash_command='python /opt/airflow/workspace/scripts/data_tests.py --boundary gold_dim',
    )

    run_producer >> run_consumer >> test_after_consumer >> run_silver >> test_after_silver >> run_gold >> test_after_gold
