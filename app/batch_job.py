from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum

spark = SparkSession.builder.appName("BatchLayer").getOrCreate()

df = spark.read.json("/app/datasets/transactions.json")

result = df.groupBy("customer").agg(spark_sum("amount").alias("total_amount"))
result.show()

result.write.mode("overwrite").json("/app/batch_output")


spark.stop()
