import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import psycopg
from psycopg.types.json import Jsonb
from dotenv import load_dotenv

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


def config_postgres() -> dict:
    """Usa variaveis do ambiente/.env, nunca uma senha no codigo."""
    senha = os.getenv("POSTGRES_PASSWORD")
    if not senha:
        raise RuntimeError("POSTGRES_PASSWORD nao definido. Configure no .env do projeto.")
    return {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
        "dbname": os.getenv("POSTGRES_DB", "twitch_data"),
        "user": os.getenv("POSTGRES_USER", "twitch"),
        "password": senha,
        "connect_timeout": 10,
    }


def recuperar_json_stream(raw_message: str, indice_stream: int | None) -> dict:
    """Isola a live correta, mesmo quando uma mensagem antiga contem varias."""
    payload = json.loads(raw_message)
    if not isinstance(payload, dict):
        raise ValueError("Mensagem Kafka valida precisa ser um objeto JSON")

    if isinstance(payload.get("stream"), dict):
        live = dict(payload["stream"])
    elif isinstance(payload.get("data"), list):
        indice = indice_stream if indice_stream is not None else 0
        live = dict(payload["data"][indice])
    else:
        live = dict(payload)

    # Registra origem da coleta tambem em raw_data, se existir no envelope.
    for campo in ("collection_id", "collected_at"):
        if campo not in live and payload.get(campo) is not None:
            live[campo] = payload[campo]
    return live


def valores_postgres(row) -> tuple:
    """Converte uma linha Spark validada no registro do PostgreSQL."""
    # Identificador natural do evento: collection_id + stream_id,
    # ou topico/particao/offset para mensagens antigas sem collection_id.
    identificador = uuid.uuid5(uuid.NAMESPACE_URL, "twitch-streaming:" + row.dedup_key)
    return (
        identificador,
        row.stream_id,
        row.user_id,
        row.user_name,
        row.game_name,
        row.viewer_count,
        row.started_at,
        row.collected_at,
        Jsonb(recuperar_json_stream(row.raw_message, row.indice_stream)),
    )

INSERT_SNAPSHOT = """
INSERT INTO public.stream_snapshots (
    event_id,
    stream_id,
    user_id,
    user_name,
    game_name,
    viewer_count,
    started_at,
    collected_at,
    raw_data
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (event_id) DO NOTHING
"""

def gravar_postgres(df_validos: DataFrame) -> tuple[int, int]:
    campos = [
        "dedup_key", "stream_id", "user_id", "user_name", "game_name",
        "viewer_count", "started_at", "collected_at", "raw_message", "indice_stream",
    ]
    candidatos = inseridos = 0
    pendentes = []
    with psycopg.connect(**config_postgres()) as conexao:
        with conexao.cursor() as cursor:
            # Iterator para nao juntar o DataFrame inteiro na memoria do driver.
            for row in df_validos.select(*campos).toLocalIterator():
                pendentes.append(valores_postgres(row))
                if len(pendentes) >= 200:
                    cursor.executemany(INSERT_SNAPSHOT, pendentes, returning=False)
                    candidatos += len(pendentes)
                    inseridos += cursor.rowcount
                    pendentes.clear()
            if pendentes:
                cursor.executemany(INSERT_SNAPSHOT, pendentes, returning=False)
                candidatos += len(pendentes)
                inseridos += cursor.rowcount
        # A conexao e commitada ao sair do contexto; erro causa rollback.
    return candidatos, inseridos


def backfill_parquet(spark: SparkSession, logger: logging.Logger) -> None:
    """Carga inicial opcional para lotes Silver ja consumidos pelo Spark."""
    if not VALIDOS.exists() or not next(VALIDOS.rglob("*.parquet"), None):
        print("Backfill: nenhum Parquet Silver encontrado. Novos eventos serao processados normalmente.")
        return
    inicio = time.perf_counter()
    historico = spark.read.parquet(str(VALIDOS))
    candidatos, inseridos = gravar_postgres(historico)
    duracao = round(time.perf_counter() - inicio, 3)
    logger.info(json.dumps({
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "etapa": "postgres_backfill",
        "status": "SUCCESS",
        "candidatos": candidatos,
        "inseridos": inseridos,
        "duracao_segundos": duracao,
    }, ensure_ascii=False))
    print(f"Backfill PostgreSQL: {inseridos} inseridos de {candidatos} candidatos em {duracao}s.")


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

        inicio_postgres = time.perf_counter()
        pg_candidatos, pg_inseridos = gravar_postgres(validos)
        duracao_postgres = round(time.perf_counter() - inicio_postgres, 3)

        registro = {
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "batch_id": batch_id,
            "status": "SUCCESS",
            "validos": qt_validos,
            "rejeitados": qt_rejeitados,
            "postgres_candidatos": pg_candidatos,
            "postgres_inseridos": pg_inseridos,
            "duracao_postgres_segundos": duracao_postgres,
            "duracao_segundos": round(time.perf_counter() - inicio, 3),
        }
        logger.info(json.dumps(registro, ensure_ascii=False))
        print(
            f"Batch {batch_id}: {qt_validos} validos, {qt_rejeitados} rejeitados, "
            f"{pg_inseridos} novos snapshots no PostgreSQL."
        )
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
    load_dotenv(RAIZ / ".env", override=False)
    # Falha rapidamente se o ambiente nao estiver configurado.
    config_postgres()
    logger = configurar_logger()
    spark = (
        SparkSession.builder
        .appName("twitch-streaming-processing")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    if os.getenv("SPARK_BACKFILL_POSTGRES") == "1":
        backfill_parquet(spark, logger)

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
