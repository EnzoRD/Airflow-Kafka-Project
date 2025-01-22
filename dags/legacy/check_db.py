from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from confluent_kafka import Producer, Consumer, KafkaException
import requests
import json
import time
import psycopg2

def test_postgres_connection():
    try:
        conn = psycopg2.connect(
            host="airflowkafkasetup-postgres-1",
            database="airflow",
            user="airflow",
            password="airflow",
            port=5432
        )
        cursor = conn.cursor()
        cursor.execute("INSERT INTO cryptodata (timestamp, name, symbol, price) VALUES (1234567890, 'TestCoin', 'TEST', 1.23);")
        conn.commit()
        cursor.close()
        conn.close()
        print("Conexión a PostgreSQL exitosa y datos insertados.")
    except Exception as e:
        print(f"Error al conectar a PostgreSQL: {e}")
