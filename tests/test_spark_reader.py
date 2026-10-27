import json
from datetime import datetime, timezone

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.processing.spark_reader import transformar_lote


@pytest.fixture(scope="module")
def spark():
    sessao = (
        SparkSession.builder.master("local[2]")
        .appName("pytest-twitch-streaming")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    sessao.sparkContext.setLogLevel("ERROR")
    yield sessao
    sessao.stop()


def dataframe_kafka(spark, mensagens):
    """Simula APENAS o formato de registros lidos do Kafka, sem broker."""
    instante = datetime(2026, 10, 27, 12, 0, tzinfo=timezone.utc)
    linhas = [
        ("twitch-streams", 0, indice, instante, None, mensagem.encode("utf-8"))
        for indice, mensagem in enumerate(mensagens)
    ]
    schema = "topic string, partition int, offset long, timestamp timestamp, key binary, value binary"
    return spark.createDataFrame(linhas, schema)


def mensagem_live(id_live="live-1", user_id="user-1", viewer_count=100, collection_id="coleta-1"):
    return json.dumps({
        "event_id": f"evento-{id_live}-{collection_id}",
        "collected_at": "2026-10-27T12:00:00+00:00",
        "stream": {
            "id": id_live,
            "user_id": user_id,
            "user_name": "Streamer Teste",
            "game_name": "Counter-Strike",
            "viewer_count": viewer_count,
            "collection_id": collection_id,
            "tags": ["portuguese", "fps"],
        },
    })


@pytest.mark.parametrize(
    ("espectadores", "motivo_esperado"),
    [
        (0, None),
        (25, None),
        (50000, None),
        (-1, "INVALID_VIEWER_COUNT"),
        (-50, "INVALID_VIEWER_COUNT"),
    ],
)
def test_validacao_espectadores(spark, espectadores, motivo_esperado):
    entrada = dataframe_kafka(spark, [mensagem_live(viewer_count=espectadores)])
    saida = transformar_lote(entrada).select("motivo_rejeicao", "viewer_count").collect()
    assert len(saida) == 1
    assert saida[0].motivo_rejeicao == motivo_esperado
    assert saida[0].viewer_count == espectadores


@pytest.mark.parametrize(
    ("mensagem", "motivo"),
    [
        (mensagem_live(id_live="", collection_id="c-missing-id"), "MISSING_STREAM_ID"),
        (mensagem_live(user_id="", collection_id="c-missing-user"), "MISSING_USER_ID"),
        ("{json-malformado", "INVALID_JSON"),
    ],
)
def test_dados_invalidos_sao_rejeitados(spark, mensagem, motivo):
    saida = transformar_lote(dataframe_kafka(spark, [mensagem]))
    assert saida.select("motivo_rejeicao").first().motivo_rejeicao == motivo


def test_duplicatas_no_mesmo_lote_sao_quarentenadas(spark):
    evento = mensagem_live(id_live="mesma-live", collection_id="mesma-coleta")
    saida = transformar_lote(dataframe_kafka(spark, [evento, evento]))
    motivos = [linha.motivo_rejeicao for linha in saida.select("motivo_rejeicao").collect()]
    assert motivos.count(None) == 1
    assert motivos.count("DUPLICATE_IN_BATCH") == 1


def test_formato_legado_lista_preserva_todas_as_transmissoes(spark):
    lista = [
        {"id": "live-A", "user_id": "user-A", "viewer_count": 0},
        {"id": "live-B", "user_id": "user-B", "viewer_count": 42},
    ]
    df = dataframe_kafka(spark, [json.dumps({"data": lista})])
    saida = transformar_lote(df)
    assert saida.filter(F.col("motivo_rejeicao").isNull()).count() == 2
    assert {r.stream_id for r in saida.select("stream_id").collect()} == {"live-A", "live-B"}


def test_formato_legado_stream_direta(spark):
    raw = json.dumps({"id": "live-direta", "user_id": "u-1", "viewer_count": 10})
    registro = transformar_lote(dataframe_kafka(spark, [raw])).first()
    assert registro.stream_id == "live-direta"
    assert registro.motivo_rejeicao is None
    assert registro.raw_message == raw
