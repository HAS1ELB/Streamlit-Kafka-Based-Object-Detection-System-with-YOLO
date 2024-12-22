import gzip
import os
import cv2
import time
import math
import streamlit as st
from ultralytics import YOLO
import cvzone
from confluent_kafka import Consumer, KafkaException, KafkaError

# Charger le modèle YOLO
model = YOLO('../yolov8n.pt')

def run_yolo_detection(output_path):
    """Effectuer la détection d'objets avec YOLO et enregistrer l'image/vidéo traitée."""
    output_file = f"recu/output_{int(time.time())}_" + os.path.basename(output_path)

    if output_path.endswith((".mp4", ".avi")):
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
        return output_file

    elif output_path.endswith((".jpg", ".png")):
        img = cv2.imread(output_path)
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
        return output_file


def start_consumer():
    """Démarrer le consommateur Kafka pour traiter les fichiers entrants."""
    consumer_config = {
        'bootstrap.servers': 'localhost:9092',
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
                        st.image(output_path, caption="Image originale")
                        print("image affichée avec succès")
                        st.header("Image en cours de traitement ...")
                    elif output_path.endswith((".mp4", ".avi")):
                        st.header("Video original")
                        with open(output_path, 'rb') as video_file:
                            st.video(video_file.read())
                        print("Vidéo affichée avec succès")
                        st.header("Video en cours de traitement ...")

                processed_file = run_yolo_detection(output_path)

                if os.path.exists(processed_file):
                    if 'refresh_key' not in st.session_state:
                        st.session_state['refresh_key'] = 0
                    st.session_state['refresh_key'] += 1

                    if processed_file.endswith((".jpg", ".png")):
                        st.image(processed_file, caption="Image traitée")
                        print("image affichée avec succès")
                    elif processed_file.endswith((".mp4", ".avi")):
                        with open(processed_file, 'rb') as video_file:
                            st.video(video_file.read())
                        print("Vidéo affichée avec succès")
                        
    
    except KeyboardInterrupt:
        print("Arrêt du consommateur Kafka.")
    finally:
        consumer.close()
        st.rerun()