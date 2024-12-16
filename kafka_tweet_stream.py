from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.streaming import DataStreamReader
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import re

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


# Décoder les messages Kafka (ils sont en bytes, donc on les transforme en string)
tweets_df = kafka_stream_df.selectExpr("CAST(value AS STRING) AS tweet")

# Fonction pour nettoyer les tweets
def clean_tweet(tweet):
    # Supprimer les URLs, mentions, hashtags, emojis
    tweet = re.sub(r"http\S+", "", tweet)  # Supprimer les URLs
    tweet = re.sub(r"@\S+", "", tweet)  # Supprimer les mentions
    tweet = re.sub(r"#\S+", "", tweet)  # Supprimer les hashtags
    tweet = re.sub(r"[^\w\s]", "", tweet)  # Supprimer les emojis et autres caractères spéciaux
    return tweet

# Enregistrer la fonction comme un UDF Spark
clean_tweet_udf = udf(clean_tweet, StringType())

# Appliquer la transformation pour nettoyer les tweets
cleaned_tweets_df = tweets_df.withColumn("cleaned_tweet", clean_tweet_udf(col("tweet")))

# Écrire le flux nettoyé en sortie (par exemple, dans la console)
query = cleaned_tweets_df.select("cleaned_tweet") \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Attendre la fin du traitement
query.awaitTermination()
