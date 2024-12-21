# Kafka Streamlit App: Producer & Consumer

## Description

Ce projet met en œuvre un système de détection d'objets basé sur Kafka et YOLO, intégré dans une interface Streamlit. Il permet de :

- Envoyer des images et vidéos à Kafka via un producteur.
- Consommer les messages Kafka pour effectuer une détection d'objets à l'aide du modèle YOLO.

## Structure du projet

```
├── .gitignore
├── app
│   ├── consumer.py
│   ├── main.py
│   └── producer.py
├── envoi
│   ├── 1.png
│   ├── 2.png
│   ├── 3.png
│   ├── bikes.mp4
│   ├── mask.png
│   └── motorbikes.mp4
└── yolov8n.pt
```

- **`app/producer.py`** : Code pour envoyer des fichiers (images/vidéos) compressés à Kafka.
- **`app/consumer.py`** : Code pour consommer les messages Kafka, effectuer la détection d'objets et afficher les résultats.
- **`app/main.py`** : Interface Streamlit pour interagir avec le producteur et le consommateur.
- **`envoi/`** : Contient des exemples de fichiers multimédias.
- **`yolov8n.pt`** : Modèle YOLO utilisé pour la détection d'objets.

## Installation

1. Clonez ce dépôt :

   ```bash
   git clone <url-du-repo>
   cd <nom-du-repo>
   ```
2. Installez les dépendances Python :

   ```bash
   pip install -r requirements.txt
   ```
3. Assurez-vous que Kafka est configuré et en cours d'exécution localement :

   ```bash
   kafka-server-start.sh config/server.properties
   ```

## Utilisation

### 1. Lancement de l'application

Exécutez Streamlit :

```bash
streamlit run app/main.py
```

### 2. Fonctionnalités

- **Producteur** :

  - Chargez une image ou une vidéo via l'interface.
  - Cliquez sur "Envoyer à Kafka" pour transmettre le fichier au topic Kafka.
- **Consommateur** :

  - Cliquez sur "Démarrer le consommateur" pour consommer les messages Kafka et afficher les résultats (images/vidéos avec détection d'objets).

## Exemples

### Interface Streamlit - Producteur

![Producer Example](envoi/1.png)

### Interface Streamlit - Consommateur

![Consumer Example](envoi/2.png)

### Détection d'objets avec YOLO

![YOLO Detection Example](envoi/3.png)

## Dépendances

- Python 3.8+
- Streamlit
- Confluent Kafka Python
- OpenCV
- ultralytics YOLO

## Licence

Ce projet est sous licence MIT. Voir le fichier LICENSE pour plus de détails.
