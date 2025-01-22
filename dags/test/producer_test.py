from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from confluent_kafka import Producer
import requests
import json
import time

# Configuración común
KAFKA_BROKER = 'kafka-broker-1:9092'
TOPIC_NAME = 'debugeando'
cryptos_to_track = ['bitcoin', 'ethereum', 'dogecoin']
api_url = 'https://api.coincap.io/v2/assets'

def produce_data():
    producer_conf = {'bootstrap.servers': 'kafka-broker-1:9092'}

    producer = Producer(producer_conf)

    try:
        response = requests.get(api_url)
        print(f"Respuesta de la API: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            if 'data' in data and isinstance(data['data'], list):
                rows = [
                    {'timestamp': int(time.time()), 'name': crypto['name'], 'symbol': crypto['symbol'], 'price': crypto['priceUsd']}
                    for crypto in data['data'] if crypto['id'] in cryptos_to_track
                ]
                for row in rows:
                    producer.produce(TOPIC_NAME, json.dumps(row))
                    print(f"Mensaje enviado: {row}")
                producer.flush()
            else:
                print("La respuesta de la API no tiene la estructura esperada.")
        else:
            print(f"Error en la API: Código de estado {response.status_code}")
    except Exception as e:
        print(f"Error en produce_data: {e}")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'kafka_producer_dag',
    default_args=default_args,
    description='DAG que produce mensajes para Kafka con datos de criptomonedas',
    schedule_interval='*/30 * * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    produce_task = PythonOperator(
        task_id='produce_data',
        python_callable=produce_data,
    )
