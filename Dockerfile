# Usar la imagen base de Airflow
FROM apache/airflow:2.9.0

# Cambiar a usuario airflow antes de instalar paquetes
USER airflow

# Instalar confluent_kafka
RUN pip install confluent_kafka psycopg2-binary
