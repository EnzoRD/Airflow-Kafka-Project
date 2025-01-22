from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from confluent_kafka import Producer, Consumer, KafkaException
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import json
import time
import psycopg2




# Configuración común
KAFKA_BROKER = 'kafka-broker-1:9092'
TOPIC_NAME = 'cryptodata'
POSTGRES_CONN = {
    'dbname': 'airflow',
    'user': 'airflow',
    'password': 'airflow',
    'host': 'postgres',
    'port': 5432
}
#ACTUALIZADO

# Productor
cryptos_to_track = ['bitcoin', 'ethereum', 'dogecoin']
api_url = 'https://api.coincap.io/v2/assets'

def produce_data():
    producer_conf = {'bootstrap.servers': KAFKA_BROKER}
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
                if rows:
                    for row in rows:
                        producer.produce(TOPIC_NAME, json.dumps(row))
                        print(f"Mensaje enviado: {row}")
                    producer.flush()
                else:
                    print("No hay datos válidos para enviar al topic.")
            else:
                print("La respuesta de la API no tiene la estructura esperada.")
        else:
            print(f"Error en la API: Código de estado {response.status_code}")
    except Exception as e:
        print(f"Error en produce_data: {e}")


def debug_consumer_and_save_to_postgres():
    print("Iniciando consumidor y conexión a PostgreSQL...")
    
    # Configuración del consumidor
    consumer_conf = {
        'bootstrap.servers': 'kafka-broker-1:9092',
        'group.id': 'airflow-consumer-group',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe(['cryptodata'])

    # Configuración de PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')

    try:
        print("Esperando mensajes del topic 'cryptodata'...")
        while True:
            msg = consumer.poll(10.0)
            if msg is None:
                print("No se encontraron mensajes en este momento.")
                break
            if msg.error():
                print(f"Error del consumidor: {msg.error()}")
                continue
            
            # Decodificar mensaje
            try:
                data = json.loads(msg.value().decode('utf-8'))
                print(f"Mensaje recibido: {data}")
            except json.JSONDecodeError as decode_error:
                print(f"Error al decodificar el mensaje: {decode_error}")
                continue

            # Guardar en PostgreSQL con deduplicación
            try:
                # Verificar si el registro ya existe
                select_query = """
                    SELECT COUNT(*) FROM cryptodata
                    WHERE timestamp = %s AND name = %s AND symbol = %s;
                """
                existing_records = pg_hook.get_first(select_query, parameters=(data['timestamp'], data['name'], data['symbol']))

                if existing_records and existing_records[0] > 0:
                    print(f"Registro duplicado encontrado, ignorado: {data}")
                else:
                    # Insertar el registro si no existe
                    insert_query = """
                        INSERT INTO cryptodata (timestamp, name, symbol, price)
                        VALUES (%s, %s, %s, %s);
                    """
                    pg_hook.run(insert_query, parameters=(data['timestamp'], data['name'], data['symbol'], float(data['price'])))
                    print(f"Mensaje guardado en PostgreSQL: {data}")
            except Exception as db_error:
                print(f"Error al guardar en PostgreSQL: {db_error}")
    except Exception as e:
        print(f"Error durante el consumo: {e}")
    finally:
        consumer.close()
        print("Consumo finalizado y conexión cerrada.")

#Con deduplicación

# DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'kafka_producer_consumer_dag_final',
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
        python_callable=debug_consumer_and_save_to_postgres,
    )

    produce_task >> consume_task
