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
    'create_kafka_topic',
    default_args=default_args,
    description='Crea un topic llamado crypto en Kafka',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    create_topic = BashOperator(
        task_id='create_kafka_topic',
        bash_command=(
            "docker exec kafka-broker-1 /bin/bash -c "
            "'kafka-topics --create --topic crypto --bootstrap-server localhost:9092 "
            "--partitions 1 --replication-factor 1'"
        )
    )

    create_topic
