from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import statistics
import sys
import time
from typing import Callable

RAIZ = Path(__file__).resolve().parents[1]
CAMINHO_INGESTAO = RAIZ / "logs" / "ingestion.jsonl"
CAMINHO_SPARK_LOG = RAIZ / "logs" / "spark_processing.jsonl"
CAMINHO_SILVER = RAIZ / "data" / "silver" / "streams"
CAMINHO_RELATORIOS = RAIZ / "reports" / "benchmarks"

# Somente comandos SELECT fixos, sem entrada SQL fornecida
CONSULTAS = {
    "total_snapshots": "SELECT count(*) FROM public.stream_snapshots",
    "ultimas_transmissoes": """
        SELECT stream_id, user_name, game_name, viewer_count, collected_at
        FROM public.stream_snapshots
        ORDER BY collected_at DESC LIMIT 25
    """,
    "top_jogos": """
        SELECT game_name, count(*) AS snapshots, max(viewer_count) AS pico_espectadores
        FROM public.stream_snapshots
        GROUP BY game_name ORDER BY snapshots DESC LIMIT 10
    """,
    "coletas_por_hora": """
        SELECT date_trunc('hour', collected_at) AS hora, count(*) AS snapshots
        FROM public.stream_snapshots
        GROUP BY 1 ORDER BY hora DESC LIMIT 24
    """,
}


def arredondar(valor: float | int | None, casas: int = 3):
    return round(float(valor), casas) if valor is not None else None


def resumo_tempos(valores: list[float]) -> dict:
    """Estatisticas de uma amostra real, sem extrapolar volume."""
    if not valores:
        return {"amostras": 0, "min_ms": None, "p50_ms": None, "p95_ms": None, "max_ms": None}
    ordenados = sorted(float(x) for x in valores)
    return {
        "amostras": len(ordenados),
        "min_ms": arredondar(ordenados[0]),
        "p50_ms": arredondar(statistics.median(ordenados)),
        "p95_ms": arredondar(ordenados[max(0, math.ceil(len(ordenados) * .95) - 1)]),
        "max_ms": arredondar(ordenados[-1]),
    }


def carregar_jsonl(caminho: Path) -> list[dict]:
    """Pula linhas ilegíveis; logs antigos continuam compatíveis."""
    if not caminho.is_file():
        return []
    entradas = []
    with caminho.open(encoding="utf-8") as arquivo:
        for linha in arquivo:
            if not linha.strip():
                continue
            try:
                dado = json.loads(linha)
            except json.JSONDecodeError:
                continue
            if isinstance(dado, dict):
                entradas.append(dado)
    return entradas


def tempos_log(entradas: list[dict], campo: str) -> dict:
    milissegundos = [
        float(item[campo]) * 1000
        for item in entradas
        if isinstance(item.get(campo), (int, float)) and item[campo] >= 0
    ]
    return resumo_tempos(milissegundos)


def analisar_logs() -> dict:
    """Dados HISTORICOS: nao e uma execucao nova de Kafka/Spark."""
    ingestao = carregar_jsonl(CAMINHO_INGESTAO)
    spark = carregar_jsonl(CAMINHO_SPARK_LOG)
    lotes = [x for x in spark if "batch_id" in x]
    ok_ingestao = [x for x in ingestao if x.get("status") == "SUCCESS"]
    ok_lotes = [x for x in lotes if x.get("status") == "SUCCESS"]
    taxas = [
        x["validos"] / x["duracao_segundos"]
        for x in ok_lotes
        if isinstance(x.get("validos"), int)
        and isinstance(x.get("duracao_segundos"), (int, float))
        and x["duracao_segundos"] > 0
    ]
    return {
        "ingestao": {
            "execucoes": len(ingestao),
            "sucesso": len(ok_ingestao),
            "falhas": len([x for x in ingestao if x.get("status") != "SUCCESS"]),
            "transmissoes_coletadas": sum(x.get("lives_coletadas", 0) for x in ok_ingestao),
            "duracao_total_ms": tempos_log(ok_ingestao, "duracao_segundos"),
            "api_ms": tempos_log(ok_ingestao, "duracao_api_segundos"),
            "kafka_ms": tempos_log(ok_ingestao, "duracao_kafka_segundos"),
        },
        "spark": {
            "lotes": len(lotes),
            "sucesso": len(ok_lotes),
            "falhas": len([x for x in lotes if x.get("status") != "SUCCESS"]),
            "validos_observados_nos_lotes": sum(x.get("validos", 0) for x in ok_lotes),
            "rejeitados_observados_nos_lotes": sum(x.get("rejeitados", 0) for x in ok_lotes),
            "duracao_lote_ms": tempos_log(ok_lotes, "duracao_segundos"),
            "duracao_postgres_ms": tempos_log(ok_lotes, "duracao_postgres_segundos"),
            "throughput_registros_por_segundo_por_lote": {
                "amostras": len(taxas),
                "mediana": arredondar(statistics.median(taxas)) if taxas else None,
            },
        },
    }


def medir(repeticoes: int, aquecimento: int, funcao: Callable[[], object]) -> dict:
    for _ in range(aquecimento):
        funcao()
    tempos = []
    for _ in range(repeticoes):
        inicio = time.perf_counter()
        funcao()
        tempos.append((time.perf_counter() - inicio) * 1000)
    return resumo_tempos(tempos)


def coletar_planos(plano: dict) -> dict:
    """Nomes dos nós de execucao (Seq Scan, Index Scan etc.)."""
    nos = []
    def visitar(no: dict):
        nos.append(no.get("Node Type", "?"))
        for filho in no.get("Plans", []):
            visitar(filho)
    visitar(plano.get("Plan", {}))
    return {
        "execucao_ms": arredondar(plano.get("Execution Time")),
        "planejamento_ms": arredondar(plano.get("Planning Time")),
        "nos": nos,
        "shared_hit_blocks": plano.get("Plan", {}).get("Shared Hit Blocks"),
        "shared_read_blocks": plano.get("Plan", {}).get("Shared Read Blocks"),
    }


def benchmark_postgres(repeticoes: int, aquecimento: int) -> dict:
    import psycopg
    from dotenv import load_dotenv

    load_dotenv(RAIZ / ".env", override=False)
    senha = os.getenv("POSTGRES_PASSWORD")
    if not senha:
        raise RuntimeError("POSTGRES_PASSWORD ausente. Configure o .env do projeto.")
    conexao = {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
        "dbname": os.getenv("POSTGRES_DB", "twitch_data"),
        "user": os.getenv("POSTGRES_USER", "twitch"),
        "password": senha,
        "connect_timeout": 10,
        "options": "-c default_transaction_read_only=on",  # Impede alteracoes acidentais.
    }
    saida = {}
    with psycopg.connect(**conexao) as conn:
        with conn.cursor() as cursor:
            for nome, sql in CONSULTAS.items():
                def executar():
                    cursor.execute(sql)
                    cursor.fetchall()  # Inclui custo de transferir o resultado.

                metricas = medir(repeticoes, aquecimento, executar)
                cursor.execute("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql)
                bruto = cursor.fetchone()[0]
                dados = json.loads(bruto) if isinstance(bruto, str) else bruto
                saida[nome] = {"latencia_cliente": metricas, "plano": coletar_planos(dados[0])}
            cursor.execute("SELECT count(*) FROM public.stream_snapshots")
            total = cursor.fetchone()[0]
    return {"total_snapshots": total, "consultas": saida}


def benchmark_spark(repeticoes: int, aquecimento: int) -> dict:
    """Mede operacoes reais no Parquet Silver. NAO mede Kafka em tempo real."""
    if not CAMINHO_SILVER.is_dir() or not next(CAMINHO_SILVER.rglob("*.parquet"), None):
        return {"status": "SEM_PARQUET", "mensagem": "Nenhum arquivo Parquet Silver encontrado."}

    from pyspark.sql import SparkSession, functions as F
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("twitch-de018-benchmark-readonly")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        df = spark.read.parquet(str(CAMINHO_SILVER))
        # Os registros sao contados a partir dos arquivos reais processados pelo Spark.
        quantidade = df.count()
        medicao_leitura = medir(repeticoes, aquecimento, lambda: df.count())
        comparacao = {}
        for particoes in (2, 4):
            spark.conf.set("spark.sql.shuffle.partitions", str(particoes))
            def agregar():
                return (df.groupBy("game_name")
                        .agg(F.count("*").alias("snapshots"), F.max("viewer_count").alias("pico"))
                        .orderBy(F.desc("snapshots"))
                        .limit(10).collect())
            comparacao[str(particoes)] = medir(repeticoes, aquecimento, agregar)
        p50 = medicao_leitura["p50_ms"]
        return {
            "status": "SUCCESS",
            "snapshots_silver": quantidade,
            "leitura_parquet": medicao_leitura,
            "leitura_registros_por_segundo_mediana": (
                arredondar(quantidade / (p50 / 1000)) if p50 and p50 > 0 else None
            ),
            "agregacao_jogos_por_particoes_shuffle": comparacao,
        }
    finally:
        spark.stop()


def _md_tempo(tempo: dict) -> str:
    if not tempo or tempo.get("p50_ms") is None:
        return "sem amostras"
    return f"mediana {tempo['p50_ms']} ms | p95 {tempo['p95_ms']} ms | n={tempo['amostras']}"


def gerar_markdown(relatorio: dict) -> str:
    linhas = [
        "# Benchmark DE-018 — Twitch Streaming",
        "",
        f"Data UTC: {relatorio['timestamp_utc']}",
        "",
        "**Escopo:** consultas PostgreSQL e leitura/agrupamento de Parquet reais; métricas históricas dos logs.",
        "Não mede ponta a ponta da Twitch em tempo real. Nenhum evento foi gravado por este benchmark.",
        "Com poucas centenas de linhas, latências podem ser dominadas pelo overhead; não extrapole para produção.",
        "",
        "## Logs históricos",
    ]
    log = relatorio["logs"]
    linhas.extend([
        f"- Ingestão: {log['ingestao']['sucesso']} execuções bem-sucedidas; {log['ingestao']['falhas']} falhas; duração {_md_tempo(log['ingestao']['duracao_total_ms'])}.",
        f"- Spark: {log['spark']['sucesso']} lotes bem-sucedidos; {log['spark']['falhas']} falhas; duração {_md_tempo(log['spark']['duracao_lote_ms'])}.",
        f"- Escrita PostgreSQL (logs Spark): {_md_tempo(log['spark']['duracao_postgres_ms'])}.",
        "",
        "## Consultas PostgreSQL",
    ])
    pg = relatorio.get("postgres")
    if pg and "consultas" in pg:
        linhas.append(f"- Snapshots existentes: {pg['total_snapshots']}.")
        linhas.append("| Consulta | Latência cliente p50 | p95 | Plano | Tempo no servidor (EXPLAIN) |")
        linhas.append("|---|---:|---:|---|---:|")
        for nome, m in pg["consultas"].items():
            q = m["latencia_cliente"]
            plano = m["plano"]
            linhas.append(f"| {nome} | {q['p50_ms']} ms | {q['p95_ms']} ms | {', '.join(plano['nos'])} | {plano['execucao_ms']} ms |")
        linhas.append("Nota: EXPLAIN ANALYZE executa a consulta e mede um evento adicional, não é a mesma amostra da latência do cliente.")
    else:
        linhas.append(f"- Indisponível: {relatorio.get('erros', {}).get('postgres', 'não solicitado')}.")
    linhas.extend(["", "## Spark (Parquet offline)"])
    sr = relatorio.get("spark_parquet", {})
    if sr.get("status") == "SUCCESS":
        linhas.extend([
            f"- Registros Parquet Silver: {sr['snapshots_silver']}",
            f"- Leitura: {_md_tempo(sr['leitura_parquet'])}.",
            f"- Leitura aproximada: {sr['leitura_registros_por_segundo_mediana']} registros/s (apenas Parquet).",
        ])
        for particoes, medicao in sr["agregacao_jogos_por_particoes_shuffle"].items():
            linhas.append(f"- Agrupamento por jogos com {particoes} partições shuffle: {_md_tempo(medicao)}.")
    else:
        linhas.append(f"- {sr.get('mensagem', relatorio.get('erros', {}).get('spark_parquet', 'não solicitado'))}")
    linhas.extend([
        "", "## Interpretação", "",
        "Compare este relatório com uma nova execução antes de concluir que uma otimização melhorou.",
        "Use a mesma máquina, volume de dados, número de repetições e carga concorrente.",
        "Amostras pequenas variam por cache, JVM e sistema operacional.",
        "Índices podem ajudar em tabelas grandes, mas não devem ser aplicados sem comparar EXPLAIN antes/depois.",
    ])
    return "\n".join(linhas) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Benchmark não destrutivo do Twitch Streaming")
    parser.add_argument("--repeat", type=int, default=5, help="Número de medições por cenário (padrão: 5)")
    parser.add_argument("--warmup", type=int, default=1, help="Execuções de aquecimento por cenário (padrão: 1)")
    parser.add_argument("--skip-spark", action="store_true", help="Não executar o Spark/Parquet")
    parser.add_argument("--skip-db", action="store_true", help="Não consultar o PostgreSQL")
    parser.add_argument("--output-dir", type=Path, default=CAMINHO_RELATORIOS)
    args = parser.parse_args(argv)
    if not 1 <= args.repeat <= 50 or not 0 <= args.warmup <= 10:
        parser.error("--repeat deve estar entre 1 e 50 e --warmup entre 0 e 10")

    timestamp = datetime.now(timezone.utc)
    relatorio = {
        "timestamp_utc": timestamp.isoformat(timespec="seconds"),
        "config": {"repeticoes": args.repeat, "aquecimento": args.warmup},
        "logs": analisar_logs(),
        "erros": {},
    }
    if not args.skip_db:
        print("[1/2] Medindo consultas PostgreSQL (somente leitura)...", flush=True)
        try:
            relatorio["postgres"] = benchmark_postgres(args.repeat, args.warmup)
        except Exception as exc:
            relatorio["erros"]["postgres"] = f"{type(exc).__name__}: {exc}"
            print(f"ERRO no PostgreSQL: {exc}", file=sys.stderr)
    if not args.skip_spark:
        print("[2/2] Medindo Parquet real com Spark (sem Kafka/checkpoint)...", flush=True)
        try:
            relatorio["spark_parquet"] = benchmark_spark(args.repeat, args.warmup)
        except Exception as exc:
            relatorio["erros"]["spark_parquet"] = f"{type(exc).__name__}: {exc}"
            print(f"ERRO no Spark: {exc}", file=sys.stderr)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    nome = timestamp.strftime("%Y%m%dT%H%M%S_%fZ")
    json_path = args.output_dir / f"benchmark_{nome}.json"
    md_path = args.output_dir / f"benchmark_{nome}.md"
    json_path.write_text(json.dumps(relatorio, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(gerar_markdown(relatorio), encoding="utf-8")
    print(f"Relatório JSON: {json_path}")
    print(f"Relatório Markdown: {md_path}")
    print(f"Status: {'FALHA PARCIAL' if relatorio['erros'] else 'CONCLUIDO'}")
    return 1 if relatorio["erros"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
