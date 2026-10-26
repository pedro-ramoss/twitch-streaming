import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from pyspark.storagelevel import StorageLevel


RAIZ = Path(__file__).resolve().parents[2]
VALIDOS = RAIZ / "data" / "silver" / "streams"
REJEITADOS = RAIZ / "data" / "quarantine" / "streams"
CHECKPOINT = RAIZ / "data" / "checkpoints" / "twitch_spark_v1"
LOG_PROCESSAMENTO = RAIZ / "logs" / "spark_processing.jsonl"

TOPICO = "twitch-streams"
KAFKA = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


# Campos conhecidos de Get Streams. A mensagem Kafka original permanece
# salva em raw_message, inclusive com eventuais campos novos da API.
STREAM_SCHEMA = StructType([
    StructField("id", StringType()),
    StructField("user_id", StringType()),
    StructField("user_login", StringType()),
    StructField("user_name", StringType()),
    StructField("game_id", StringType()),
    StructField("game_name", StringType()),
    StructField("type", StringType()),
    StructField("title", StringType()),
    StructField("viewer_count", IntegerType()),
    StructField("started_at", StringType()),
    StructField("language", StringType()),
    StructField("thumbnail_url", StringType()),
    StructField("tag_ids", ArrayType(StringType())),
    StructField("tags", ArrayType(StringType())),
    StructField("content_classification_labels", ArrayType(StringType())),
    StructField("is_mature", BooleanType()),
    StructField("is_branded_content", BooleanType()),
    # Metadados que o nosso streaming_runner acrescenta a cada live.
    StructField("collection_id", StringType()),
    StructField("collected_at", StringType()),
])

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("collected_at", StringType()),
    StructField("collection_id", StringType()),
    StructField("source", StringType()),
    StructField("stream", STREAM_SCHEMA),
    StructField("data", ArrayType(STREAM_SCHEMA)),
    # Os campos acima tambem podem existir na raiz (formato legado).
    *[
        StructField(c.name, c.dataType)
        for c in STREAM_SCHEMA.fields
        if c.name not in ("collection_id", "collected_at")
    ],
    StructField("_corrupt_record", StringType()),
])


def configurar_logger() -> logging.Logger:
    LOG_PROCESSAMENTO.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("twitch_spark_processing")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if not logger.handlers:
        handler = logging.FileHandler(LOG_PROCESSAMENTO, encoding="utf-8")
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
    return logger


def transformar_lote(df_kafka: DataFrame) -> DataFrame:
    """Expande JSON, valida transmissao e identifica duplicatas NO MESMO lote.

    Nao descarta linhas ruins: preenche motivo_rejeicao para quarentena.
    Duplicatas entre lotes serao protegidas por chave unica no PostgreSQL.
    """
    base = (
        df_kafka.select(
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("raw_message"),
        )
        .withColumn(
            "payload",
            F.from_json(
                F.col("raw_message"),
                EVENT_SCHEMA,
                {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"},
            ),
        )
    )

    # Compatibilidade com os tres formatos que ja foram enviados ao topico.
    transmissao_direta = F.struct(*[
        F.col(f"payload.{campo.name}").alias(campo.name)
        for campo in STREAM_SCHEMA.fields
    ])
    lista = (
        F.when(F.col("payload.data").isNotNull(), F.col("payload.data"))
        .when(F.col("payload.stream").isNotNull(), F.array(F.col("payload.stream")))
        .otherwise(F.array(transmissao_direta))
    )

    expandido = (
        base.withColumn("lista_streams", lista)
        .select("*", F.posexplode_outer("lista_streams").alias("indice_stream", "stream"))
    )

    resultado = expandido.select(
        "kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp",
        "kafka_key", "raw_message", "indice_stream",
        F.col("payload._corrupt_record").alias("json_corrompido"),
        F.col("payload.event_id").alias("event_id_original"),
        F.coalesce(
            F.col("stream.collection_id"), F.col("payload.collection_id")
        ).alias("collection_id"),
        F.coalesce(
            F.col("stream.collected_at"), F.col("payload.collected_at")
        ).alias("collected_at_raw"),
        F.coalesce(F.col("payload.source"), F.lit("twitch")).alias("source"),
        F.col("stream.id").alias("stream_id"),
        F.col("stream.user_id").alias("user_id"),
        *[
            F.col(f"stream.{campo.name}").alias(campo.name)
            for campo in STREAM_SCHEMA.fields
            if campo.name not in ("id", "user_id", "collection_id", "collected_at")
        ],
    )

    evento_kafka = F.concat_ws(
        ":", F.col("kafka_topic"), F.col("kafka_partition"),
        F.col("kafka_offset"), F.coalesce(F.col("indice_stream"), F.lit(0)),
    )
    resultado = (
        resultado
        .withColumn("event_id", F.coalesce(F.col("event_id_original"), evento_kafka))
        .withColumn("collected_at", F.coalesce(
            F.try_to_timestamp("collected_at_raw"), F.col("kafka_timestamp")
        ))
        .withColumn("started_at", F.try_to_timestamp("started_at"))
        .withColumn("collection_date", F.to_date("collected_at"))
        .withColumn("dedup_key", F.when(
            F.col("collection_id").isNotNull()
            & F.col("stream_id").isNotNull(),
            F.concat_ws(":", F.lit("collection"), "collection_id", "stream_id"),
        ).otherwise(
            F.concat_ws(":", F.lit("event"), F.col("event_id"),
                        F.coalesce(F.col("indice_stream"), F.lit(0)))
        ))
    )

    # Validacao com causas explicitas, sem perder o JSON original.
    resultado = resultado.withColumn(
        "motivo_rejeicao",
        F.when(
            F.col("json_corrompido").isNotNull() | F.col("raw_message").isNull(),
            "INVALID_JSON",
        ).when(
            F.col("stream_id").isNull() | (F.trim("stream_id") == ""),
            "MISSING_STREAM_ID",
        ).when(
            F.col("user_id").isNull() | (F.trim("user_id") == ""),
            "MISSING_USER_ID",
        ).when(
            F.col("viewer_count").isNull() | (F.col("viewer_count") < 0),
            "INVALID_VIEWER_COUNT",
        ),
    )

    # Deduplicacao sem estado infinito: apenas dentro de cada micro-lote.
    # Rejeitamos copias para auditoria em vez de descarta-las silenciosamente.
    janela = Window.partitionBy("dedup_key").orderBy(
        "kafka_timestamp", "kafka_partition", "kafka_offset", "indice_stream"
    )
    resultado = resultado.withColumn("_numero_ocorrencia", F.row_number().over(janela))
    return (
        resultado.withColumn(
            "motivo_rejeicao",
            F.when(
                (F.col("_numero_ocorrencia") > 1)
                & F.col("motivo_rejeicao").isNull(),
                F.lit("DUPLICATE_IN_BATCH")
            ).otherwise(F.col("motivo_rejeicao")),
        )
        .drop("_numero_ocorrencia", "json_corrompido", "event_id_original", "collected_at_raw")
    )


def processar_lote(df_kafka: DataFrame, batch_id: int, logger: logging.Logger) -> None:
    inicio = time.perf_counter()
    df = transformar_lote(df_kafka).persist(StorageLevel.MEMORY_AND_DISK)
    try:
        validos = df.filter(F.col("motivo_rejeicao").isNull())
        rejeitados = df.filter(F.col("motivo_rejeicao").isNotNull())
        qt_validos = validos.count()
        qt_rejeitados = rejeitados.count()

        # Cada micro-lote tem um destino estavel. Em caso de replay de batch_id,
        # sobrescrevemos o mesmo caminho, sem acrescentar novas copias.
        caminho_validos = VALIDOS / f"batch_id={batch_id:08d}"
        caminho_rejeitados = REJEITADOS / f"batch_id={batch_id:08d}"
        validos.write.mode("overwrite").parquet(str(caminho_validos))
        rejeitados.write.mode("overwrite").parquet(str(caminho_rejeitados))

        registro = {
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "batch_id": batch_id,
            "status": "SUCCESS",
            "validos": qt_validos,
            "rejeitados": qt_rejeitados,
            "duracao_segundos": round(time.perf_counter() - inicio, 3),
        }
        logger.info(json.dumps(registro, ensure_ascii=False))
        print(f"Batch {batch_id}: {qt_validos} validos, {qt_rejeitados} rejeitados.")
    except Exception as erro:
        logger.info(json.dumps({
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "batch_id": batch_id,
            "status": "ERROR",
            "erro": f"{type(erro).__name__}: {erro}",
        }, ensure_ascii=False))
        raise  # Falha visivel: Spark nao deve confirmar o checkpoint.
    finally:
        df.unpersist()


def main() -> None:
    logger = configurar_logger()
    spark = (
        SparkSession.builder
        .appName("twitch-streaming-processing")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df_kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA)
        .option("subscribe", TOPICO)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .load()
    )

    gravador = (
        df_kafka.writeStream
        .foreachBatch(lambda lote, n: processar_lote(lote, n, logger))
        .option("checkpointLocation", str(CHECKPOINT))
        .outputMode("append")
    )
    if os.getenv("SPARK_RUN_ONCE") == "1":
        gravador = gravador.trigger(availableNow=True)
    else:
        gravador = gravador.trigger(processingTime="30 seconds")

    query = gravador.start()
    try:
        query.awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
