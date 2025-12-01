from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum
from pyspark.sql.types import StructType, StringType, IntegerType

# Création d'une session Spark pour le traitement en streaming
spark = SparkSession.builder.appName("SpeedLayer").getOrCreate()

# Définition du schéma des messages JSON qui arrivent depuis Kafka
# Chaque message va contenir : {"customer": "Ali", "amount": 120}
schema = StructType() \
    .add("customer", StringType()) \
    .add("amount", IntegerType())

# Lecture d'un flux Kafka en temps réel
# - format("kafka") : source = Kafka
# - kafka.bootstrap.servers : adresse du broker Kafka (nom du service Docker)
# - subscribe : nom du topic Kafka à lire
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "real-time-orders") \
    .load()

# Les messages Kafka sont au format binaire (bytes) dans la colonne 'value'.
# On convertit la colonne "value" en STRING, puis on parse le JSON selon le schéma défini.
json_df = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# Agrégation en streaming :
# - groupBy("customer") : regrouper les événements par client
# - sum("amount") : accumulation des montants reçus en temps réel
agg = json_df.groupBy("customer").agg(sum("amount")).alias("total")
agg = agg.withColumnRenamed("sum(amount)", "total_amount_stream") # Ajout pour la clarté

# Définition de la requête de streaming:
# - outputMode("complete") : affiche la vue globale à chaque mise à jour
# - format("console") : affiche dans le terminal (pour le TP)
query = agg.writeStream.outputMode("complete") \
    .format("console") \
    .trigger(processingTime='5 seconds') \
    .start()

# La requête reste active tant que l'application tourne
query.awaitTermination()
