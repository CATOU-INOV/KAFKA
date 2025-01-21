from confluent_kafka import Consumer
import json
import pandas as pd
import re
import mysql.connector

# Configuration de la connexion à MySQL
mysql_config = {
    'host': 'localhost',
    'user': 'root',  # Remplacer par ton utilisateur MySQL
    'password': 'admin',  # Remplacer par ton mot de passe MySQL
    'database': 'tweets_db'  # Nom de ta base de données
}

# Fonction de connexion à MySQL avec gestion des erreurs
def connect_to_mysql():
    try:
        connection = mysql.connector.connect(**mysql_config)
        print("Connexion réussie à MySQL")
        return connection
    except mysql.connector.Error as err:
        print(f"Erreur de connexion à MySQL : {err}")
        return None

# Fonction pour insérer un tweet dans la base de données avec gestion des erreurs
def insert_tweet(tweet_id, created_at, author, full_text, cleaned_tweet):
    connection = connect_to_mysql()
    if connection is None:
        return  # Ne pas continuer si la connexion échoue

    try:
        cursor = connection.cursor()

        # Requête SQL pour insérer un tweet
        query = """
        INSERT INTO tweets_propre (tweet_id, created_at, author, full_text, cleaned_tweet)
        VALUES (%s, %s, %s, %s, %s)
        """
        
        # Valeurs à insérer
        data = (tweet_id, created_at, author, full_text, cleaned_tweet)
        
        # Exécution de la requête
        cursor.execute(query, data)
        connection.commit()
        print(f"Insertion réussie pour le tweet avec ID: {tweet_id}")  # Affiche l'ID pour vérifier
    except mysql.connector.Error as err:
        print(f"Erreur lors de l'insertion du tweet : {err}")
    finally:
        cursor.close()
        connection.close()

# Configuration du consommateur Kafka
conf = {
    'bootstrap.servers': 'localhost:9092',  # Adresse de votre broker Kafka
    'group.id': 'my_consumer_group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe(['tweets_topic'])  # Remplacez par le nom de votre topic

# Fonction de nettoyage des tweets
def clean_tweet(tweet):
    if not tweet:
        return None
    tweet = re.sub(r"http\S+|www\S+|https\S+", "", tweet, flags=re.MULTILINE)  # Retirer les URLs
    tweet = re.sub(r"@\w+", "", tweet)  # Retirer les mentions
    tweet = re.sub(r"#\w+", "", tweet)  # Retirer les hashtags
    tweet = re.sub(r"\d+", "", tweet)  # Retirer les chiffres
    tweet = re.sub(r"[^\w\s]", "", tweet)  # Retirer les caractères spéciaux
    tweet = re.sub(r"\s+", " ", tweet).strip()  # Retirer les espaces supplémentaires
    return tweet

# Consommer les messages et les traiter
while True:
    msg = consumer.poll(timeout=1.0)

    if msg is None:
        continue
    elif msg.error():
        print(msg.error())
        continue

    # Traitement du message
    tweet_json = msg.value().decode('utf-8')
    tweet_data = json.loads(tweet_json)

    # Afficher la structure du tweet pour inspection
    print(json.dumps(tweet_data, indent=4))  # Cette ligne te permettra de voir la structure du tweet

    # Utilisation de 'tweet_id' comme clé pour l'ID du tweet
    tweet_id = tweet_data.get('tweet_id', 'ID non trouvé')  # Utilisation de get pour éviter KeyError
    created_at = tweet_data.get('created_at', None)  # Récupère la date de création du tweet
    author = tweet_data.get('author', None)  # Récupère l'auteur du tweet

    # Créer un DataFrame Pandas à partir des données du tweet
    tweet_df = pd.DataFrame([tweet_data])

    # Nettoyer le tweet
    tweet_df['cleaned_tweet'] = tweet_df['full_text'].apply(clean_tweet)

    # Insérer les données dans la base de données
    for _, row in tweet_df.iterrows():
        full_text = row['full_text']
        cleaned_tweet = row['cleaned_tweet']
        insert_tweet(tweet_id, created_at, author, full_text, cleaned_tweet)

consumer.close()

