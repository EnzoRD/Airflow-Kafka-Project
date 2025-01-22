from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def insert_and_fetch_data():
    # Conectar a PostgreSQL usando PostgresHook
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    # Insertar un registro de prueba
    insert_query = """
    INSERT INTO cryptodata (timestamp, name, symbol, price)
    VALUES (%s, %s, %s, %s)
    """
    pg_hook.run(insert_query, parameters=(int(datetime.now().timestamp()), 'TestCoin', 'TEST', 123.45))

    # Recuperar registros de la tabla
    select_query = "SELECT * FROM cryptodata"
    records = pg_hook.get_records(select_query)
    for record in records:
        print(record)

with DAG(
    'postgres_test_dag',
    default_args=default_args,
    description='DAG para probar la conexión con PostgreSQL',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    test_postgres_task = PythonOperator(
        task_id='test_postgres',
        python_callable=insert_and_fetch_data,
    )




