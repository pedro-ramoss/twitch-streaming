import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Permite executar o teste na raiz do repositorio.
from src.processing.spark_reader import transformar_lote


@pytest.fixture(scope="module")
def spark():
    sessao = (
        SparkSession.builder.master("local[2]")
        .appName("test-twitch-spark")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    sessao.sparkContext.setLogLevel("ERROR")
    yield sessao
    sessao.stop()


def criar_kafka_falso(spark, mensagens):
    agora = datetime(2026, 10, 26, 12, 0, 0, tzinfo=timezone.utc)
    linhas = [
        ("twitch-streams", 0, n, agora, b"chave", msg.encode("utf-8"))
        for n, msg in enumerate(mensagens)
    ]
    return spark.createDataFrame(
        linhas,
        "topic string, partition int, offset long, timestamp timestamp, key binary, value binary",
    )


def test_envelope_e_validacao(spark):
    evento = {
        "event_id": "evento-1",
        "collected_at": "2026-10-26T12:00:00+00:00",
        "source": "twitch",
        "stream": {
            "id": "live-1",
            "user_id": "user-1",
            "user_name": "Streamer",
            "game_name": "Counter-Strike",
            "viewer_count": 100,
            "collection_id": "col-1",
            "tags": ["portuguese", "fps"],
        },
    }
    ruim = {
        "stream": {"id": "live-2", "user_id": "user-2", "viewer_count": -10}
    }
    repetido = {**evento, "event_id": "evento-2"}
    kafka = criar_kafka_falso(spark, [
        json.dumps(evento), json.dumps(ruim), json.dumps(repetido), "{json-invalido"
    ])
    linhas = transformar_lote(kafka).select(
        "stream_id", "viewer_count", "collection_id", "motivo_rejeicao", "tags"
    ).collect()
    validas = [r for r in linhas if r.motivo_rejeicao is None]
    motivos = {r.motivo_rejeicao for r in linhas if r.motivo_rejeicao is not None}
    assert len(validas) == 1
    assert validas[0].stream_id == "live-1"
    assert validas[0].tags == ["portuguese", "fps"]
    assert "INVALID_VIEWER_COUNT" in motivos
    assert "DUPLICATE_IN_BATCH" in motivos
    assert "INVALID_JSON" in motivos


def test_legado_em_lote_e_direto(spark):
    uma = {"id": "a", "user_id": "ua", "viewer_count": 20, "game_name": "Just Chatting"}
    duas = {"id": "b", "user_id": "ub", "viewer_count": 15, "game_name": "Minecraft"}
    kafka = criar_kafka_falso(spark, [json.dumps({"data": [uma, duas]}), json.dumps(uma)])
    resultado = transformar_lote(kafka)
    assert resultado.filter(F.col("motivo_rejeicao").isNull()).count() == 3
    assert set(r.stream_id for r in resultado.select("stream_id").collect()) == {"a", "b"}
