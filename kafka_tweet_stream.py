from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, from_json
from pyspark.sql.types import StringType
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
def insert_tweet(full_text, cleaned_tweet):
    connection = connect_to_mysql()
    if connection is None:
        return  # Ne pas continuer si la connexion échoue

    try:
        cursor = connection.cursor()

        # Requête SQL pour insérer un tweet
        query = "INSERT INTO tweets_propre (full_text, cleaned_tweet) VALUES (%s, %s)"
        
        # Valeurs à insérer
        data = (full_text, cleaned_tweet)
        
        # Exécution de la requête
        cursor.execute(query, data)
        connection.commit()
        print(f"Insertion réussie pour le tweet avec ID: {full_text[:10]}...")  # Affiche les 10 premiers caractères pour vérifier
    except mysql.connector.Error as err:
        print(f"Erreur lors de l'insertion du tweet : {err}")
    finally:
        cursor.close()
        connection.close()

# Initialiser la session Spark
spark = SparkSession.builder \
    .appName("KafkaTweetStream") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .getOrCreate()

# Se connecter à Kafka
kafka_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "tweets_topic") \
    .option("startingOffsets", "earliest") \
    .load()

# Afficher le schéma pour vérifier les colonnes disponibles
kafka_stream_df.printSchema()

# Décoder les messages Kafka (ils sont en bytes, donc on les transforme en string)
tweets_df = kafka_stream_df.selectExpr("CAST(value AS STRING) AS tweet")

# Définir le schéma pour le JSON
json_schema = "struct<created_at:string, full_text:string, id_str:string>"  # Schéma du JSON

# Parser le JSON et extraire le champ 'full_text'
tweets_parsed_df = tweets_df.select(from_json(col("tweet"), json_schema).alias("parsed_tweet"))

# Extraire uniquement le champ full_text
tweets_text_df = tweets_parsed_df.select("parsed_tweet.full_text")

# Définir la fonction de nettoyage
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

# Enregistrer la fonction comme un UDF Spark
clean_tweet_udf = udf(clean_tweet, StringType())

# Appliquer la transformation pour nettoyer les tweets
cleaned_tweets_df = tweets_text_df.withColumn("cleaned_tweet", clean_tweet_udf(col("full_text")))

# Fonction pour insérer les données dans MySQL, avec gestion des erreurs
def process_batch(df, epoch_id):
    # Insérer chaque ligne du batch dans la base de données
    try:
        for row in df.collect():
            full_text = row['full_text']
            cleaned_tweet = row['cleaned_tweet']
            insert_tweet(full_text, cleaned_tweet)
    except Exception as e:
        print(f"Erreur lors du traitement du batch: {e}")

# Afficher le flux nettoyé en console pour déboguer
query = cleaned_tweets_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .format("console") \
    .start()

# Attendre la fin du traitement
query.awaitTermination()

