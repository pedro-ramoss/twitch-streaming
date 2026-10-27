from __future__ import annotations

import json
import logging
import os
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from prometheus_client import CollectorRegistry, start_http_server
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = Path(os.getenv("TWITCH_LOG_DIR", str(ROOT / "logs")))
LOGGER = logging.getLogger("twitch_metrics")


def ler_jsonl(caminho: Path) -> list[dict[str, Any]]:
    """Ignora linhas incompletas/inadequadas enquanto um processo escreve o log."""
    if not caminho.is_file():
        return []
    registros: list[dict[str, Any]] = []
    try:
        with caminho.open(encoding="utf-8") as arquivo:
            for linha in arquivo:
                try:
                    item = json.loads(linha)
                except (ValueError, TypeError):
                    continue
                if isinstance(item, dict):
                    registros.append(item)
    except OSError as erro:
        LOGGER.warning("Nao foi possivel ler log %s: %s", caminho.name, erro)
    return registros


def instante_utc(valor: Any) -> float:
    if not isinstance(valor, str):
        return 0.0
    try:
        instante = datetime.fromisoformat(valor.replace("Z", "+00:00"))
        if instante.tzinfo is None:
            instante = instante.replace(tzinfo=timezone.utc)
        return instante.timestamp()
    except ValueError:
        return 0.0


def numero_nao_negativo(valor: Any) -> float:
    if isinstance(valor, bool) or not isinstance(valor, (int, float)):
        return 0.0
    return max(0.0, float(valor))


def resumir_ingestao(registros: list[dict[str, Any]]) -> dict[str, Any]:
    validos = [item for item in registros if item.get("status") in {"SUCCESS", "ERROR"}]
    contagens = Counter(item["status"] for item in validos)
    sucessos = [instante_utc(item.get("timestamp")) for item in validos if item["status"] == "SUCCESS"]
    erros = [instante_utc(item.get("timestamp")) for item in validos if item["status"] == "ERROR"]
    return {
        "contagens": contagens,
        "coletados": sum(numero_nao_negativo(item.get("lives_coletadas")) for item in validos),
        # Quando o envio falhou e a quantidade e desconhecida (None), nao inventar resultado.
        "enviados": sum(numero_nao_negativo(item.get("lives_enviadas")) for item in validos),
        "tentativas": sum(numero_nao_negativo(item.get("tentativas_api")) for item in validos),
        "ultimo_sucesso": max(sucessos, default=0.0),
        "ultimo_erro": max(erros, default=0.0),
        "ultima_duracao": numero_nao_negativo(validos[-1].get("duracao_segundos")) if validos else 0.0,
    }


def resumir_spark(registros: list[dict[str, Any]]) -> dict[str, Any]:
    # Logs de backfill usam "etapa", sem batch_id; sao contabilizados separadamente.
    lotes = [item for item in registros if "batch_id" in item and item.get("status") in {"SUCCESS", "ERROR"}]
    backfills = [item for item in registros if item.get("etapa") == "postgres_backfill"]
    contagens = Counter(item["status"] for item in lotes)
    sucessos = [instante_utc(item.get("timestamp")) for item in lotes if item["status"] == "SUCCESS"]
    erros = [instante_utc(item.get("timestamp")) for item in lotes if item["status"] == "ERROR"]
    lotes_ok = [item for item in lotes if item["status"] == "SUCCESS"]
    ultimo_ok = lotes_ok[-1] if lotes_ok else {}
    return {
        "contagens": contagens,
        "validos": sum(numero_nao_negativo(item.get("validos")) for item in lotes_ok),
        "rejeitados": sum(numero_nao_negativo(item.get("rejeitados")) for item in lotes_ok),
        "pg_candidatos": sum(numero_nao_negativo(item.get("postgres_candidatos")) for item in lotes_ok),
        "pg_inseridos": sum(numero_nao_negativo(item.get("postgres_inseridos")) for item in lotes_ok),
        "backfill_candidatos": sum(numero_nao_negativo(item.get("candidatos")) for item in backfills if item.get("status") == "SUCCESS"),
        "backfill_inseridos": sum(numero_nao_negativo(item.get("inseridos")) for item in backfills if item.get("status") == "SUCCESS"),
        "ultimo_sucesso": max(sucessos, default=0.0),
        "ultimo_erro": max(erros, default=0.0),
        "ultima_duracao": numero_nao_negativo(ultimo_ok.get("duracao_segundos")),
        "ultima_duracao_pg": numero_nao_negativo(ultimo_ok.get("duracao_postgres_segundos")),
    }


def consultar_postgres() -> tuple[int, int | None]:
    """Somente leitura: verifica conectividade e conta snapshots; nunca escreve."""
    try:
        import psycopg
        with psycopg.connect(
            host=os.getenv("POSTGRES_HOST", "postgres"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            dbname=os.getenv("POSTGRES_DB", "twitch_data"),
            user=os.getenv("POSTGRES_USER", "twitch"),
            password=os.getenv("POSTGRES_PASSWORD", ""),
            connect_timeout=3,
        ) as conexao:
            with conexao.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM public.stream_snapshots")
                return 1, int(cursor.fetchone()[0])
    except Exception as erro:
        LOGGER.warning("PostgreSQL indisponivel para monitoramento: %s", type(erro).__name__)
        return 0, None


def contador(nome: str, descricao: str, valor: float) -> CounterMetricFamily:
    metric = CounterMetricFamily(nome, descricao)
    metric.add_metric([], valor)
    return metric


def medidor(nome: str, descricao: str, valor: float) -> GaugeMetricFamily:
    metric = GaugeMetricFamily(nome, descricao)
    metric.add_metric([], valor)
    return metric


class TwitchCollector:
    """Rele logs a cada scrape: nenhuma metrica se perde ao reiniciar o exporter."""

    def __init__(self, pasta_logs: Path = LOG_DIR, db_probe=consultar_postgres):
        self.pasta_logs = Path(pasta_logs)
        self.db_probe = db_probe

    def collect(self):
        inicio = time.perf_counter()
        ingestao = resumir_ingestao(ler_jsonl(self.pasta_logs / "ingestion.jsonl"))
        spark = resumir_spark(ler_jsonl(self.pasta_logs / "spark_processing.jsonl"))

        runs = CounterMetricFamily("twitch_ingestion_runs_total", "Coletas executadas por status.", labels=["status"])
        for status in ("SUCCESS", "ERROR"):
            runs.add_metric([status], ingestao["contagens"][status])
        yield runs

        yield contador("twitch_ingestion_streams_collected_total", "Streams retornadas pela Twitch.", ingestao["coletados"])
        yield contador("twitch_ingestion_streams_sent_total", "Streams com envio confirmado ao Kafka.", ingestao["enviados"])
        yield contador("twitch_ingestion_api_attempts_total", "Tentativas totais na API Twitch.", ingestao["tentativas"])
        yield medidor("twitch_ingestion_last_success_timestamp_seconds", "Ultima coleta bem-sucedida (epoch UTC).", ingestao["ultimo_sucesso"])
        yield medidor("twitch_ingestion_last_failure_timestamp_seconds", "Ultima falha de coleta (epoch UTC).", ingestao["ultimo_erro"])
        yield medidor("twitch_ingestion_last_duration_seconds", "Duracao da ultima execucao de coleta.", ingestao["ultima_duracao"])

        batches = CounterMetricFamily("twitch_spark_batches_total", "Micro-lotes Spark por status.", labels=["status"])
        for status in ("SUCCESS", "ERROR"):
            batches.add_metric([status], spark["contagens"][status])
        yield batches
        yield contador("twitch_spark_valid_records_total", "Registros validos em micro-lotes Spark.", spark["validos"])
        yield contador("twitch_spark_rejected_records_total", "Registros rejeitados pelo Spark.", spark["rejeitados"])
        yield contador("twitch_spark_postgres_candidates_total", "Candidatos gravados pelo Spark no Postgres.", spark["pg_candidatos"])
        yield contador("twitch_spark_postgres_inserted_total", "Snapshots novos inseridos em micro-lotes.", spark["pg_inseridos"])
        yield contador("twitch_postgres_backfill_candidates_total", "Candidatos avaliados em backfills.", spark["backfill_candidatos"])
        yield contador("twitch_postgres_backfill_inserted_total", "Snapshots inseridos via backfill.", spark["backfill_inseridos"])
        yield medidor("twitch_spark_last_success_timestamp_seconds", "Ultimo micro-lote bem-sucedido (epoch UTC).", spark["ultimo_sucesso"])
        yield medidor("twitch_spark_last_failure_timestamp_seconds", "Ultima falha do Spark (epoch UTC).", spark["ultimo_erro"])
        yield medidor("twitch_spark_last_duration_seconds", "Duracao do ultimo micro-lote bem-sucedido.", spark["ultima_duracao"])
        yield medidor("twitch_spark_last_postgres_duration_seconds", "Tempo PostgreSQL no ultimo lote bem-sucedido.", spark["ultima_duracao_pg"])

        disponivel, registros = self.db_probe()
        yield medidor("twitch_postgres_up", "1 se PostgreSQL respondeu a SELECT, senao 0.", disponivel)
        if registros is not None:
            yield medidor("twitch_postgres_snapshots", "Numero atual de snapshots no PostgreSQL.", registros)
        yield medidor("twitch_metrics_scrape_duration_seconds", "Tempo de coleta de metricas do exporter.", time.perf_counter() - inicio)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    porta = int(os.getenv("METRICS_PORT", "8000"))
    registro = CollectorRegistry(auto_describe=False)
    registro.register(TwitchCollector())
    start_http_server(port=porta, addr="0.0.0.0", registry=registro)
    LOGGER.info("Exporter disponivel em :%s/metrics", porta)
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
