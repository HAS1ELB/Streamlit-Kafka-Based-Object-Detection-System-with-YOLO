import gzip
from confluent_kafka import Producer
import yaml
import os

# Charger la variable depuis le fichier YAML
with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)

# Accéder à la variable
HOSTNAME = config['HOSTNAME']

def delivery_report(err, msg):
    """Callback appelé une fois le message envoyé."""
    if err is not None:
        print('Échec de la livraison : {}'.format(err))
    else:
        print('Message livré à {} [{}]'.format(msg.topic(), msg.partition()))


def start_producer(file_key, file_path):
    # Configuration du producteur Confluent Kafka
    producer_config = {
        'bootstrap.servers': HOSTNAME + ':9092',  # Adresse du cluster Kafka
        'client.id': 'streamlit_producer',
        'compression.type': 'gzip',  # Ajout de la compression côté Kafka
        'message.max.bytes': 104857600  # Taille maximale du message (100 Mo)
    }
    
    producer = Producer(producer_config)
    print(f"Fichier clé : {file_key}")
    
    try:
        # Compression du fichier avec gzip
        compressed_file_path = f"{file_path}.gz"
        with open(file_path, 'rb') as f_in:
            with gzip.open(compressed_file_path, 'wb') as f_out:
                f_out.writelines(f_in)              
        
        # Lecture du fichier compressé et envoi à Kafka
        with open(compressed_file_path, 'rb') as f:
            data = f.read()
            # Envoyer le fichier compressé avec la clé (nom et extension)
            producer.produce('streamlit_topic', key=file_key, value=data, callback=delivery_report)
            producer.flush()  # S'assurer que les messages sont envoyés
            
        print(f"Fichier compressé envoyé avec succès : {compressed_file_path}")
    
    except Exception as e:
        print(f"Erreur lors de l'envoi : {e}")
    
    finally:
        producer.flush()
        os.remove(compressed_file_path)
        os.remove(file_path)
