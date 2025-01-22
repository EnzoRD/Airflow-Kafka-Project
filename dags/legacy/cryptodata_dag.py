from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

# Argumentos por defecto del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Crear el DAG
with DAG(
    'cryptodata_pipeline',
    default_args=default_args,
    description='Pipeline para procesar datos de criptomonedas con Kafka',
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Funciones para ejecutar scripts
    def run_producer():
        subprocess.run(["python3", "dags\proyecto_kafka_cryptodata\proyecto_kafka_cryptodata\producer.py"], check=True)

    def run_consumer():
        subprocess.run(["python3", "dags\proyecto_kafka_cryptodata\proyecto_kafka_cryptodata\consumer.py"], check=True)

    # Tarea 1: Ejecutar producer.py
    producer_task = PythonOperator(
        task_id='run_producer',
        python_callable=run_producer,
    )

    # Tarea 2: Ejecutar consumer.py
    consumer_task = PythonOperator(
        task_id='run_consumer',
        python_callable=run_consumer,
    )

    # Definir dependencias
    producer_task >> consumer_task
