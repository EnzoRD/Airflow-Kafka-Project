from airflow import DAG
from airflow.operators.python import PythonOperator
from confluent_kafka import Consumer, KafkaException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import json
#actualizado
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

            # Guardar en PostgreSQL
            try:
                insert_query = """
                    INSERT INTO cryptodata (timestamp, name, symbol, price)
                    VALUES (%s, %s, %s, %s)
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

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'kafka_consumer_postgres_debug_dag',
    default_args=default_args,
    description='DAG para depurar la conexión entre Kafka y PostgreSQL',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    debug_consume_task = PythonOperator(
        task_id='debug_consume_and_save_to_postgres',
        python_callable=debug_consumer_and_save_to_postgres,
    )
