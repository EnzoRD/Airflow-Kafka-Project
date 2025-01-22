from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'kafka_validate_ready',
    default_args=default_args,
    description='Valida que Kafka esté activo',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    validate_kafka = BashOperator(
        task_id='validate_kafka',
        bash_command="docker exec kafka-broker-1 /bin/bash -c 'kafka-topics --list --bootstrap-server kafka-broker-1:9092'"
    )
