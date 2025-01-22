from confluent_kafka import Consumer, KafkaError
import csv
import json
import time

#Configurar consumer kafka
topic_name = 'cryptodata'
gourp_id = 'cryptodata_group'
consumer_conf = {'bootstrap.servers': "kafka:9092", 'group.id': gourp_id, 'auto.offset.reset': 'earliest'}
consumer = Consumer(consumer_conf)
consumer.subscribe([topic_name])


csv_file_path = 'cryptodata.csv'

with open(csv_file_path, mode='a', newline='') as file:
    #crear un objeto Dictwriter
    writer = csv.DictWriter(file, fieldnames=['timestamp', 'name', 'symbol', 'price'])
    
    #Escribir el encabezado solo si el archivo no existe
    if not file.tell():
        writer.writeheader()
    
    #Leer mensajes de kafka y escribir en el csv, el consumidor va a estar leyendo mensajes de kafka sin  parar
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                continue
            row = json.loads(msg.value())
            writer.writerow(row)
            print(f"Data written to csv file {csv_file_path}")
            
            
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        print("Consumer closed, final data written to csv file")
        time.sleep(5) # le doy 5 segundo después de cerrar para que envie la info al csv
