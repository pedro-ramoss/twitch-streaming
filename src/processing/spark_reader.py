from pyspark.sql import SparkSession

spark = (SparkSession.builder
    .appName("twitch streaming")
    .getOrCreate()
)

df = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "twitch-streams")
    .option("startingOffsets", "earliest")
    .load()
)

df_texto = df.selectExpr(
    "CAST(value AS STRING) AS mensagem"
)

query = (df_texto.writeStream
    .format("console")
    .outputMode("append")
    .start()
)

query.awaitTermination()