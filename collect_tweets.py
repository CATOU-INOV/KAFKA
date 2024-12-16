import asyncio
from twikit import Client
import json
import pandas as pd
import mysql.connector
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

# Fonction pour insérer un tweet dans la base de données
def insert_tweet(tweet_data):
    connection = connect_to_mysql()
    cursor = connection.cursor()

    # Format de la date attendu par MySQL : 'YYYY-MM-DD HH:MM:SS'
    if isinstance(tweet_data['created_at'], datetime):
        created_at = tweet_data['created_at'].strftime('%Y-%m-%d %H:%M:%S')
    else:
        created_at = None  # Gérer le cas où la date est invalide

    query = "INSERT INTO tweets_db.tweets (id_str, created_at, favorite_count, full_text, author) VALUES (%s, %s, %s, %s, %s)"
    
    # Extraire les données du tweet
    data = (tweet_data['tweet_id'], created_at, tweet_data['favorite_count'], tweet_data['full_text'], tweet_data['author'])
    
    cursor.execute(query, data)
    connection.commit()

    cursor.close()
    connection.close()

# Fonction pour formater datetime en string pour JSON
def datetime_converter(o):
    if isinstance(o, datetime):
        return o.isoformat()  # Format ISO standard : 'YYYY-MM-DDTHH:MM:SS'

# Fonction principale asynchrone
async def main():
    client = Client('en-US')

    # Login à Twitter
    await client.login(
        auth_info_1='@vincecours',  # Remplacer par ton nom d'utilisateur
        password='Vincent0123.',  # Remplacer par ton mot de passe
    )

    # Sauvegarde et chargement des cookies
    client.save_cookies('cookies.json')
    client.load_cookies(path='cookies.json')

    # Définir la recherche
    theme = "bayrou"  # Remplacer par ton thème
    product = "latest"  # "top", "latest", ou "user"

    # Recherche des tweets
    tweets = await client.search_tweet(query=theme, product=product, count=200)

    # Préparer les données des tweets
    tweets_to_store = []
    for tweet in tweets:
        print(tweet.__dict__)  # Inspecter l'objet Tweet
        
        # Extraire les informations pertinentes
        try:
            # Convertir la date au format datetime
            created_at = datetime.strptime(tweet.created_at, "%a %b %d %H:%M:%S +0000 %Y")
        except Exception as e:
            print(f"Erreur de conversion de la date: {e}")
            created_at = None

        tweet_data = {
            'created_at': created_at,
            'favorite_count': tweet.favorite_count,
            'full_text': tweet.full_text,
            'author': tweet.user.screen_name if hasattr(tweet, 'user') else 'Unknown',
            'tweet_id': tweet.id,  # Utiliser l'attribut id au lieu de id_str
        }
        
        # Ajouter les données à la liste
        tweets_to_store.append(tweet_data)

        # Insérer le tweet dans la base de données
        insert_tweet(tweet_data)

    # Sauvegarder les tweets dans un fichier CSV
    df = pd.DataFrame(tweets_to_store)
    df.to_csv('tweets_theme.csv', index=False)

    # Afficher les données triées
    print(df.sort_values(by='favorite_count', ascending=False))

    # Afficher les données en format JSON (en convertissant les datetime)
    print(json.dumps(tweets_to_store, default=datetime_converter, indent=4))

# Exécuter la fonction principale
asyncio.run(main())
