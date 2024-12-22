import gzip
import os
import cv2
import time
import math
import streamlit as st
from ultralytics import YOLO
import cvzone
from confluent_kafka import Consumer, KafkaException
import yaml

# Charger la variable depuis le fichier YAML
with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)

# Accéder à la variable
HOSTNAME = config['HOSTNAME']


# Charger le modèle YOLO
model = YOLO('../yolov8n.pt')
video_extensions =[".mp4", ".mkv", ".avi", ".mov", ".wmv", ".flv", ".webm",".mpg", ".mpeg", ".3gp", ".ts", ".ogg", ".rm"]
image_extensions = [".jpg", ".jpeg", ".png", ".gif", ".bmp", ".tiff", ".tif",".webp", ".heif", ".heic", ".raw", ".svg", ".ico", ".pdf", ".eps"]

def run_yolo_detection(output_path):
    """Effectuer la détection d'objets avec YOLO et enregistrer l'image/vidéo traitée."""
    output_file = f"recu_traite/" + os.path.basename(output_path)

    
    if output_path.lower().endswith(tuple(video_extensions)):
        cap = cv2.VideoCapture(output_path)
        fourcc = cv2.VideoWriter_fourcc(*'avc1')
        fps = cap.get(cv2.CAP_PROP_FPS)
        width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))

        out = cv2.VideoWriter(output_file, fourcc, fps, (width, height))
        prev_frame_time = 0

        while True:
            success, img = cap.read()
            if not success:
                break

            new_frame_time = time.time()
            results = model(img, stream=True)
            for r in results:
                for box in r.boxes:
                    x1, y1, x2, y2 = map(int, box.xyxy[0])
                    w, h = x2 - x1, y2 - y1
                    cvzone.cornerRect(img, (x1, y1, w, h))
                    conf = math.ceil((box.conf[0] * 100)) / 100
                    cls = int(box.cls[0])
                    cvzone.putTextRect(img, f'{model.names[cls]} {conf}', (x1, y1 - 5), scale=1)

            out.write(img)
            prev_frame_time = new_frame_time

        cap.release()
        out.release()
        cv2.destroyAllWindows()
        
        if not os.path.exists(output_file):
            raise FileNotFoundError(f"❌ Fichier vidéo traité introuvable : {output_file}")
        
        print(f"✅ Fichier vidéo traité avec succès : {output_file}")
        return output_file

    elif output_path.lower().endswith(tuple(image_extensions)):
        img = cv2.imread(output_path)
        if img is None:
            raise FileNotFoundError(f"❌ Impossible de lire le fichier image : {output_path}")

        results = model(img, stream=True)

        for r in results:
            for box in r.boxes:
                x1, y1, x2, y2 = map(int, box.xyxy[0])
                w, h = x2 - x1, y2 - y1
                cvzone.cornerRect(img, (x1, y1, w, h))
                conf = math.ceil((box.conf[0] * 100)) / 100
                cls = int(box.cls[0])
                cvzone.putTextRect(img, f'{model.names[cls]} {conf}', (x1, y1 - 5), scale=1)

        cv2.imwrite(output_file, img)
        
        if not os.path.exists(output_file):
            raise FileNotFoundError(f"❌ Fichier image traité introuvable : {output_file}")
        
        print(f"✅ Fichier image traité avec succès : {output_file}")
        return output_file


def start_consumer():
    """Démarrer le consommateur Kafka pour traiter les fichiers entrants."""
    consumer_config = {
        'bootstrap.servers': HOSTNAME +':9092',
        'group.id': 'streamlit_consumer',
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(consumer_config)
    consumer.subscribe(['streamlit_topic'])

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            else:
                file_key = msg.key().decode('utf-8') if msg.key() else "unknown_file"
                compressed_data = msg.value()
                decompressed_data = gzip.decompress(compressed_data)

                output_path = f"recu/{file_key}"
                with open(output_path, 'wb') as f:
                    f.write(decompressed_data)

                if os.path.exists(output_path):
                    if 'refresh_key' not in st.session_state:
                        st.session_state['refresh_key'] = 0
                    st.session_state['refresh_key'] += 1

                    if output_path.endswith((".jpg", ".png")):
                        st.image(output_path, caption="🖼️ Image originale")
                        st.header("🛠️ Traitement de l'image en cours ...")
                    elif output_path.endswith((".mp4", ".avi")):
                        st.header("🎥 Vidéo originale")
                        with open(output_path, 'rb') as video_file:
                            st.video(video_file.read())
                        st.header("🛠️ Traitement de la vidéo en cours ...")

                # Traitement avec YOLO
                processed_file = run_yolo_detection(output_path)

                if os.path.exists(processed_file):
                    if 'processed_files' not in st.session_state:
                        st.session_state['processed_files'] = []
                    st.session_state['processed_files'].append(processed_file)

                    if processed_file.lower().endswith(tuple(image_extensions)):
                        st.image(processed_file, caption="✅ Image traitée avec succès")
                    elif processed_file.lower().endswith(tuple(video_extensions)):
                        with open(processed_file, 'rb') as video_file:
                            st.video(video_file.read())
                else:
                    st.error(f"❌ Le fichier traité est manquant : {processed_file}")

    except KeyboardInterrupt:
        print("🛑 Arrêt du consommateur Kafka.")
    finally:
        consumer.close()
        st.cache_data.clear()
        st.cache_resource.clear()
        st.rerun()


if __name__ == '__main__':
    st.title("🛠️ Kafka Consumer avec YOLO Object Detection")
    start_consumer()