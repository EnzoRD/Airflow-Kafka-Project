from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from confluent_kafka import Producer, Consumer, KafkaException
import requests
import json
import time
import psycopg2

# Configuración común
KAFKA_BROKER = 'kafka:9092'
TOPIC_NAME = 'cryptodata'
POSTGRES_CONN = {
    'dbname': 'airflow',
    'user': 'airflow',
    'password': 'airflow',
    'host': 'postgres',
    'port': 5432
}

# Productor
cryptos_to_track = ['bitcoin', 'ethereum', 'dogecoin']
api_url = 'https://api.coincap.io/v2/assets'

def produce_data():
    producer_conf = {'bootstrap.servers': KAFKA_BROKER}
    producer = Producer(producer_conf)

    response = requests.get(api_url)
    if response.status_code == 200:
        data = response.json()
        if 'data' in data and isinstance(data['data'], list):
            rows = [
                {'timestamp': int(time.time()), 'name': crypto['name'], 'symbol': crypto['symbol'], 'price': crypto['priceUsd']}
                for crypto in data['data'] if crypto['id'] in cryptos_to_track
            ]
            if rows:
                for row in rows:
                    print(f"Mensajes enviados al topic {TOPIC_NAME}: {len(row)}")
                    producer.produce(TOPIC_NAME, json.dumps(row))
                producer.flush()
            else:
                print("No se encontraron criptomonedas válidas para enviar.")
        else:
            print("Estructura de respuesta de la API no válida.")
    else:
        print(f"La solicitud a la API falló con el código de estado {response.status_code}")

# Consumidor
def consume_and_save():
    consumer_conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'airflow-consumer-group',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([TOPIC_NAME])

    try:
        print("Esperando mensajes del topic...")
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                print("No se encontraron mensajes en este momento.")
                break
            if msg.error():
                print(f"Error del consumidor: {msg.error()}")
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    continue
                else:
                    break
            data = json.loads(msg.value().decode('utf-8'))
            print(f"Mensaje recibido: {data}")
            
            # Intenta guardar en PostgreSQL
            try:
                conn = psycopg2.connect(
                    host="airflowkafkasetup-postgres-1",
                    database="airflow",
                    user="airflow",
                    password="airflow",
                    port=5432
                )
                cursor = conn.cursor()
                query = """
                    INSERT INTO cryptodata (timestamp, name, symbol, price)
                    VALUES (%s, %s, %s, %s);
                """
                cursor.execute(query, (data['timestamp'], data['name'], data['symbol'], data['price']))
                conn.commit()
                cursor.close()
                conn.close()
                print(f"Mensaje guardado en PostgreSQL: {data}")
            except Exception as db_error:
                print(f"Error al guardar en PostgreSQL: {db_error}")
    except Exception as e:
        print(f"Error durante el consumo: {e}")
    finally:
        consumer.close()


# DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'kafka_producer_consumer_dag',
    default_args=default_args,
    description='DAG que produce y consume mensajes Kafka con datos de criptomonedas',
    schedule_interval='*/30 * * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    produce_task = PythonOperator(
        task_id='produce_data',
        python_callable=produce_data,
    )

    consume_task = PythonOperator(
        task_id='consume_data',
        python_callable=consume_and_save,
    )

    produce_task >> consume_task
