import mysql.connector
from textblob import TextBlob
from textblob_fr import PatternAnalyzer
from langdetect import detect, LangDetectException
from datetime import datetime

# Configuration de la connexion à MySQL
mysql_config = {
    'host': 'localhost',
    'user': 'root',  # Remplacer par ton utilisateur MySQL
    'password': 'admin',  # Remplacer par ton mot de passe MySQL
    'database': 'tweets_db'  # Nom de ta base de données
}

# Fonction de connexion à MySQL
def connect_to_mysql():
    return mysql.connector.connect(**mysql_config)

# Fonction pour insérer les résultats dans la base de données
def insert_sentiment_data(tweet_text, author, sentiment_score, created_at):
    db_connection = connect_to_mysql()
    cursor = db_connection.cursor()
    
    # Requête SQL pour insérer les données
    insert_query = """
        INSERT INTO tweets_db.sentiments_tweets (tweet_text, author, sentiment_score, created_at)
        VALUES (%s, %s, %s, %s)
    """
    
    # Exécuter la requête avec les valeurs
    cursor.execute(insert_query, (tweet_text, author, sentiment_score, created_at))
    
    # Valider l'insertion
    db_connection.commit()
    
    # Fermer la connexion
    cursor.close()
    db_connection.close()

# Fonction pour récupérer les tweets et analyser leur sentiment
def get_tweets_and_analyze_sentiment():
    # Connexion à la base de données
    db_connection = connect_to_mysql()
    
    # Créer un curseur pour exécuter des requêtes
    cursor = db_connection.cursor()

    # Requête SQL pour récupérer les tweets, leurs auteurs, et la date/heure
    cursor.execute("SELECT cleaned_tweet, Author, created_at FROM tweets_propre")

    # Récupérer tous les résultats
    tweets = cursor.fetchall()

    # Analyser chaque tweet
    for tweet in tweets:
        tweet_text = tweet[0]  # Le texte du tweet
        author = tweet[1]  # L'auteur du tweet
        created_at = tweet[2]  # La date et l'heure du tweet

        # Vérifier si le tweet n'est pas vide
        if not tweet_text.strip():  # Si le tweet est vide ou contient uniquement des espaces
            print(f"Auteur: {author}, Tweet: [VIDE]")
            continue

        # Détecter la langue du tweet
        try:
            language = detect(tweet_text)
        except LangDetectException:
            print(f"Auteur: {author}, Tweet: [LANGUE INCONNUE]")
            continue

        # Analyser le sentiment en fonction de la langue
        blob = TextBlob(tweet_text)
        if language == 'fr':  # Si le tweet est en français
            sentiment_score = blob.sentiment.polarity  # Utilisation du score de TextBlob
            if sentiment_score == 0:
                # Si score égal à 0, utiliser un analyseur plus précis pour le français
                sentiment_score = PatternAnalyzer().analyze(tweet_text)[0]  # Utilisation de l'index 0 pour accéder à la polarité
        else:  # Si le tweet est en anglais ou autre
            sentiment_score = blob.sentiment.polarity

        # Afficher le résultat
        print(f"Auteur: {author}, Tweet: {tweet_text}")
        print(f"Sentiment: {sentiment_score} (Positivité), Date: {created_at}")

        # Insérer les résultats dans la base
        insert_sentiment_data(tweet_text, author, sentiment_score, created_at)

    # Fermer la connexion
    cursor.close()
    db_connection.close()

# Appel de la fonction pour récupérer les tweets et analyser leur sentiment
get_tweets_and_analyze_sentiment()

