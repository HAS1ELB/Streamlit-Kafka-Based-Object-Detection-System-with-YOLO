import streamlit as st
from producer import start_producer
from consumer import start_consumer
import os

os.makedirs("recu", exist_ok=True)

# Configuration de l'interface Streamlit
st.title("Kafka Streamlit App: Producer & Consumer")

# Menu de navigation
page = st.sidebar.radio("Choisissez une option", ["Producer", "Consumer"])

if page == "Producer":
    st.header("Producer: Envoyer une image ou une vidéo à Kafka")
    uploaded_file = st.file_uploader("Importez une image ou une vidéo", type=["jpg", "png", "mp4", "avi"])
    if uploaded_file is not None:
        # Extraire le nom et l'extension du fichier
        file_name, file_extension = uploaded_file.name.split(".")
        file_key = f"{file_name}.{file_extension}"  # Combine le nom et l'extension comme clé
        print(file_key)
        
        with open("temp_file", "wb") as f:
            f.write(uploaded_file.read())
        st.success(f"Fichier '{file_key}' chargé avec succès !")
        
        if st.button("Envoyer à Kafka"):
            # Passer la clé et le chemin du fichier au producteur
            start_producer(file_key, "temp_file")
            st.success("Données envoyées à Kafka.")

elif page == "Consumer":
    st.header("Consumer: Détection d'objets YOLO depuis Kafka")
    if st.button("Démarrer le consommateur"):
        start_consumer()
