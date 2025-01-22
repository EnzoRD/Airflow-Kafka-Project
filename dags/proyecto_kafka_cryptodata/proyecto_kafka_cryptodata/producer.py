#Importar librerias
from confluent_kafka import Producer
import requests
import json
import time


#Criptos a seguir
cryptos_to_track = ['bitcoin', 'ethereum', 'dogecoin']

#URL de la API de coincap
api_url = 'https://api.coincap.io/v2/assets'

#Configuración de kafka
producer_conf = {'bootstrap.servers': "kafka:9092"}
producer = Producer(producer_conf)
topic_name = 'cryptodata'

while True:
    #Obtener datos de la API
    response = requests.get(api_url)
    data = json.loads(response.text)
    if response.status_code == 200:
        data = response.json()
        #Verificar la estrucutra de la respuesta
        if 'data' in data and isinstance(data['data'], list):
            #Crear una lista de diccionarios con los datos de las criptomonedas
            rows = [{'timestamp': int(time.time()), 'name': crypto['name'], 'symbol': crypto['symbol'],'price': crypto['priceUsd']} for crypto in data['data'] if crypto['id'] in cryptos_to_track]
            
            # Enviar los datos a Kafka
            for row in rows:
                producer.produce(topic_name, json.dumps(row))
            
            producer.flush()
            
            print('Data sent successfully')
    else:
        print("la estructura de la respuesta no es correcta")
        
   
    time.sleep(30)#Puede hacer con consultas por 2 segundos pero depende de origen de la API