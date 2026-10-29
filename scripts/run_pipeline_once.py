from __future__ import annotations

import fcntl
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LOGS = ROOT / "logs"
AUDIT = LOGS / "scheduler_runs.jsonl"
LOCK = LOGS / ".pipeline_scheduler.lock"
INGESTION_LOG = LOGS / "ingestion.jsonl"
SPARK_LOG = LOGS / "spark_processing.jsonl"
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def log_size(path: Path) -> int:
    return path.stat().st_size if path.is_file() else 0


def new_entries(path: Path, offset: int) -> list[dict]:
    """Somente entradas realmente gravadas durante esta execucao."""
    if not path.is_file():
        return []
    if path.stat().st_size < offset:
        raise RuntimeError(f"Log foi truncado durante a execucao: {path}")
    with path.open("rb") as stream:
        stream.seek(offset)
        lines = stream.readlines()
    entries = []
    for line in lines:
        if not line.strip():
            continue
        try:
            entry = json.loads(line)
        except (ValueError, UnicodeDecodeError) as exc:
            raise RuntimeError(f"Linha invalida no log {path.name}") from exc
        if isinstance(entry, dict):
            entries.append(entry)
    return entries


def append_audit(record: dict) -> None:
    with AUDIT.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(record, ensure_ascii=False) + "\n")
        stream.flush()
        os.fsync(stream.fileno())


def wait_for_tcp(host: str, port: int, *, seconds: int = 40) -> None:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=2):
                return
        except OSError:
            time.sleep(2)
    raise RuntimeError(f"Servico indisponivel: {host}:{port}. Verifique o Docker Desktop.")


def command(args: list[str], *, env: dict[str, str], output, timeout_seconds: int) -> None:
    """Rastreia subprocessos no log; encerra grupo completo em caso de timeout."""
    proc = subprocess.Popen(args, cwd=ROOT, env=env, stdout=output,
                            stderr=subprocess.STDOUT, start_new_session=True)
    try:
        status = proc.wait(timeout=timeout_seconds)
    except subprocess.TimeoutExpired as exc:
        os.killpg(proc.pid, signal.SIGTERM)
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.wait()
        raise RuntimeError(f"Tempo esgotado para {args[0]} apos {timeout_seconds}s") from exc
    if status != 0:
        raise RuntimeError(f"Comando {args[0]} encerrou com codigo {status}")


def run_once() -> int:
    LOGS.mkdir(parents=True, exist_ok=True)
    run_id = uuid.uuid4().hex[:12]
    start = time.monotonic()
    record = {
        "run_id": run_id, "timestamp": now(), "status": "RUNNING",
        "coletados": 0, "enviados": 0, "spark_validos": 0,
        "postgres_inseridos": 0, "duracao_segundos": None, "erro": None,
    }

    with LOCK.open("a+") as lock_handle:
        try:
            fcntl.flock(lock_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            record.update(status="SKIPPED", erro="Outra execucao ainda esta em andamento",
                          duracao_segundos=0)
            append_audit(record)
            print("Execucao ignorada: ja existe outro ciclo em andamento.")
            return 0

        output_path = LOGS / "scheduler_runs" / f"{record['timestamp'].replace(':','').replace('+','_')}_{run_id}.log"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        print(f"Inicio: {record['timestamp']} | run_id={run_id}")
        print(f"Log completo: {output_path}")

        try:
            if not (ROOT / "venv" / "bin" / "python").exists():
                raise RuntimeError("venv/bin/python ausente no projeto")
            if not (ROOT / ".env").is_file():
                raise RuntimeError("Arquivo .env ausente na raiz do projeto")
            spark_submit = shutil.which("spark-submit")
            if not spark_submit:
                raise RuntimeError("spark-submit nao encontrado no PATH da venv")
            wait_for_tcp("127.0.0.1", 9092)
            wait_for_tcp("127.0.0.1", 5432)

            env = dict(os.environ)
            env["TWITCH_RUN_ONCE"] = "1"
            env["SPARK_RUN_ONCE"] = "1"
            env.pop("SPARK_BACKFILL_POSTGRES", None)  # nao recarrega todo o historico a cada 6h

            with output_path.open("w", encoding="utf-8") as output:
                # Coletor atual pode registrar ERROR e sair com codigo 0;
                # o JSONL e a fonte da verdade sobre essa execucao.
                ingestion_offset = log_size(INGESTION_LOG)
                print("[1/2] Twitch -> Kafka")
                command([sys.executable, "-m", "src.ingestion.streaming_runner"],
                        env=env, output=output, timeout_seconds=3600)
                records = new_entries(INGESTION_LOG, ingestion_offset)
                if len(records) != 1:
                    raise RuntimeError(f"Esperava 1 registro de ingestao, recebi {len(records)}")
                ingestion = records[0]
                if ingestion.get("status") != "SUCCESS":
                    raise RuntimeError(f"Falha na ingestao: {ingestion.get('erro', 'sem descricao')}")
                record["coletados"] = ingestion.get("lives_coletadas", 0)
                record["enviados"] = ingestion.get("lives_enviadas", 0)
                if not isinstance(record["enviados"], int) or record["enviados"] <= 0:
                    raise RuntimeError("Ingestao nao confirmou envio de eventos")

                print(f"[2/2] Kafka -> Spark -> Parquet/Postgres | eventos={record['enviados']}")
                spark_offset = log_size(SPARK_LOG)
                command([spark_submit, "--packages", KAFKA_PACKAGE,
                         "src/processing/spark_reader.py"],
                        env=env, output=output, timeout_seconds=7200)
                spark_entries = [r for r in new_entries(SPARK_LOG, spark_offset) if "batch_id" in r]
                if not spark_entries:
                    raise RuntimeError("Spark encerrou sem registrar lote novo; verificar log")
                if any(item.get("status") != "SUCCESS" for item in spark_entries):
                    raise RuntimeError("Spark registrou lote com erro")
                record["spark_validos"] = sum(int(r.get("validos") or 0) for r in spark_entries)
                record["postgres_inseridos"] = sum(int(r.get("postgres_inseridos") or 0) for r in spark_entries)

            record["status"] = "SUCCESS"
            print(f"OK: {record['coletados']} coletados; {record['spark_validos']} validos; "
                  f"{record['postgres_inseridos']} inseridos no PostgreSQL")
            return 0
        except Exception as exc:
            record.update(status="ERROR", erro=f"{type(exc).__name__}: {exc}")
            print(f"ERRO: {record['erro']} | detalhes em {output_path}", file=sys.stderr)
            return 1
        finally:
            record["duracao_segundos"] = round(time.monotonic() - start, 3)
            append_audit(record)
            print(f"Auditoria: {AUDIT}")


if __name__ == "__main__":
    sys.exit(run_once())
