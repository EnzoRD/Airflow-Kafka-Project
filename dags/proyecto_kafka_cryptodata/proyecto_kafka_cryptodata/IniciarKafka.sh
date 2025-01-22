#Crear nuevo Cluster Kafka con un ID random
kafka-storage.sh random-uuid

#Configurar directorio Logs
kafka-storage.sh format -t ID -c ~/kafka_2.13-3.9.0/config/kraft/server.properties

#Inicializar Kafka en daemon mode
kafka-server-start.sh ~/kafka_2.13-3.9.0/config/kraft/server.properties


#je9bzBCXRuGDI2DmOedN7w