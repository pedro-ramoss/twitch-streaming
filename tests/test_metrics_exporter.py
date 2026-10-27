import json
from pathlib import Path

from prometheus_client import CollectorRegistry, generate_latest

from scripts.metrics_exporter import (
    TwitchCollector,
    instante_utc,
    ler_jsonl,
    resumir_ingestao,
    resumir_spark,
)


def _log(caminho: Path, entradas: list[dict]):
    caminho.parent.mkdir(parents=True, exist_ok=True)
    caminho.write_text("\n".join(json.dumps(entrada) for entrada in entradas) + "\n", encoding="utf-8")


def test_log_ausente_nao_quebra(tmp_path):
    assert ler_jsonl(tmp_path / "inexistente.jsonl") == []


def test_linha_incompleta_e_ignorada(tmp_path):
    destino = tmp_path / "ingestion.jsonl"
    destino.write_text('{"status":"SUCCESS"}\n{"status":\n', encoding="utf-8")
    assert ler_jsonl(destino) == [{"status": "SUCCESS"}]


def test_ingestao_soma_apenas_envios_confirmados():
    resumo = resumir_ingestao([
        {"status": "SUCCESS", "lives_coletadas": 25, "lives_enviadas": 25,
         "tentativas_api": 1, "timestamp": "2026-10-27T12:00:00+00:00", "duracao_segundos": 1.0},
        {"status": "ERROR", "lives_coletadas": 25, "lives_enviadas": None,
         "tentativas_api": 3, "timestamp": "2026-10-27T13:00:00+00:00"},
    ])
    assert resumo["contagens"]["SUCCESS"] == 1
    assert resumo["contagens"]["ERROR"] == 1
    assert resumo["enviados"] == 25
    assert resumo["coletados"] == 50
    assert resumo["tentativas"] == 4
    assert resumo["ultimo_sucesso"] == instante_utc("2026-10-27T12:00:00+00:00")


def test_spark_nao_conta_backfill_como_lote():
    resumo = resumir_spark([
        {"batch_id": 5, "status": "SUCCESS", "validos": 25, "rejeitados": 1,
         "postgres_inseridos": 25, "postgres_candidatos": 25,
         "timestamp": "2026-10-27T13:00:00+00:00"},
        {"etapa": "postgres_backfill", "status": "SUCCESS", "candidatos": 25,
         "inseridos": 0, "timestamp": "2026-10-27T13:10:00+00:00"},
        {"batch_id": 6, "status": "ERROR", "timestamp": "2026-10-27T13:20:00+00:00"},
    ])
    assert resumo["contagens"]["SUCCESS"] == 1
    assert resumo["contagens"]["ERROR"] == 1
    assert resumo["validos"] == 25
    assert resumo["rejeitados"] == 1
    assert resumo["pg_inseridos"] == 25
    assert resumo["backfill_candidatos"] == 25
    assert resumo["backfill_inseridos"] == 0


def test_metricas_prometheus_formatadas(tmp_path):
    _log(tmp_path / "ingestion.jsonl", [
        {"status": "SUCCESS", "lives_coletadas": 25, "lives_enviadas": 25,
         "timestamp": "2026-10-27T12:00:00+00:00"},
    ])
    _log(tmp_path / "spark_processing.jsonl", [
        {"batch_id": 0, "status": "SUCCESS", "validos": 25, "rejeitados": 0,
         "postgres_inseridos": 25, "timestamp": "2026-10-27T12:00:00+00:00"},
    ])
    registro = CollectorRegistry(auto_describe=False)
    registro.register(TwitchCollector(tmp_path, db_probe=lambda: (1, 240)))
    metricas = generate_latest(registro).decode()
    assert 'twitch_ingestion_runs_total{status="SUCCESS"} 1.0' in metricas
    assert 'twitch_spark_batches_total{status="SUCCESS"} 1.0' in metricas
    assert 'twitch_spark_valid_records_total 25.0' in metricas
    assert 'twitch_postgres_snapshots 240.0' in metricas
    assert 'twitch_postgres_up 1.0' in metricas


def test_postgres_offline_sinalizado(tmp_path):
    registro = CollectorRegistry(auto_describe=False)
    registro.register(TwitchCollector(tmp_path, db_probe=lambda: (0, None)))
    metricas = generate_latest(registro).decode()
    assert 'twitch_postgres_up 0.0' in metricas
    assert 'twitch_postgres_snapshots' not in metricas


def test_invalid_date_returns_zero():
    assert instante_utc("sem-data") == 0
