# Airflow-Kafka-Project


Este repositorio documenta el desarrollo de un proyecto que integra Apache Airflow, Apache Kafka y PostgreSQL para procesar y almacenar datos de criptomonedas en tiempo real. El proyecto refleja un flujo de trabajo completo, desde la obtención de datos desde una API hasta su almacenamiento seguro en una base de datos, todo orquestado y monitorizado mediante Airflow.

---

## Introducción

El proyecto tiene como objetivo demostrar habilidades avanzadas en:
- **Orquestación de flujos de datos:** Usando Apache Airflow.
- **Procesamiento en tiempo real:** Usando Apache Kafka como broker de mensajes.
- **Almacenamiento estructurado:** Usando PostgreSQL.

La configuración fue realizada en un entorno Docker para garantizar portabilidad y replicabilidad.

---

## Estructura del Proyecto

```plaintext
AirflowKafkaSetup/
├── dags/
│   ├── legacy/                   # Pruebas descartadas
│   ├── test/                     # Pruebas exitosas
│   ├── proyecto_kafka_cryptodata/ # Proyecto Kafka original
│   └── kafka_producer_consumer.py # DAG final funcional
├── config/                       # Archivos de configuración
├── logs/                         # Logs generados por Airflow
├── plugins/                      # Plugins adicionales para Airflow
├── docker-compose.yml            # Configuración de Docker Compose
├── Dockerfile                    # Dockerfile personalizado
└── README.md                     # Documentación principal
```

---

## Configuración Inicial

### Requisitos Previos

1. **Docker y Docker Compose:** Para la gestión de contenedores.
2. **Python 3.8+ (opcional):** Si deseas probar scripts localmente.
3. **Acceso a internet:** Para descargar dependencias y acceder a la API de CoinCap.

### Instalación

1. Clona este repositorio:
   ```bash
   git clone https://github.com/tu-usuario/AirflowKafkaSetup.git
   cd AirflowKafkaSetup
   ```

2. Construye e inicia los servicios:
   ```bash
   docker-compose up --build
   ```

3. Accede a la interfaz de Airflow:
   - URL: [http://localhost:8080](http://localhost:8080)
   - Usuario y contraseña predeterminados: `airflow`

---

## Descripción del Flujo

### Productor (Producer)
- Obtiene datos de criptomonedas (Bitcoin, Ethereum, Dogecoin) desde la API de CoinCap.
- Envía los datos al topic `cryptodata` en Apache Kafka.
- Estructura del mensaje enviado:
  ```json
  {
      "timestamp": 1674415013,
      "name": "Bitcoin",
      "symbol": "BTC",
      "price": "25000.00"
  }
  ```

### Consumidor (Consumer)
- Lee los mensajes del topic `cryptodata` en Kafka.
- Almacena los datos en la tabla `cryptodata` en PostgreSQL, evitando duplicados gracias a un índice primario.
- **Esquema de la tabla:**
  ```sql
  CREATE TABLE cryptodata (
      timestamp BIGINT PRIMARY KEY,
      name TEXT,
      symbol TEXT,
      price DECIMAL
  );
  ```

### PostgreSQL
- Base de datos utilizada para almacenar los datos procesados.
- Configurada en Docker y accesible desde el DAG de Airflow.

---

## Proceso de Desarrollo

### 1. Configuración de Kafka y PostgreSQL
- Se utilizó `docker-compose.yml` para configurar los servicios.
- Verificación de conectividad entre los contenedores.
- Creación de topics en Kafka y tablas en PostgreSQL para pruebas.

### 2. Desarrollo del Productor y Consumidor
- Se construyeron funciones independientes para producir y consumir mensajes.
- Se probaron localmente utilizando comandos de Kafka CLI.

### 3. Integración con Airflow
- Creación de DAGs para orquestar las tareas del productor y consumidor.
- Ajustes en los tiempos de polling y manejo de errores para garantizar la estabilidad.

### 4. Debugging y Validación
- Uso de logs detallados en cada etapa del proceso.
- Verificación de mensajes en Kafka y datos en PostgreSQL tras cada ejecución.

---

## Pruebas y Validaciones

1. **Verificación de mensajes en Kafka:**
   ```bash
   docker exec -it kafka-broker-1 kafka-console-consumer --bootstrap-server localhost:9092 --topic cryptodata --from-beginning
   ```

2. **Consulta de datos en PostgreSQL:**
   ```bash
   docker exec -it airflowkafkasetup-postgres-1 psql -U airflow -d airflow -c "SELECT * FROM cryptodata;"
   ```

3. **Ejecución manual de DAGs:** Desde la interfaz de Airflow.

---

## Futuras Mejoras

- Implementar autenticación en Kafka y PostgreSQL.
- Migrar a un clúster de Kafka con múltiples brokers para mayor tolerancia a fallos.
- Agregar métricas y monitorización mediante Prometheus y Grafana.

---

## Contacto
- [LinkedIn](https://www.linkedin.com/in/enzo-ruiz-diaz/)

## Autor

Creado por: **Enzo Ruiz Diaz**
- [LinkedIn](https://www.linkedin.com/in/enzo-ruiz-diaz/)
- [GitHub](https://github.com/tu-usuario/)

