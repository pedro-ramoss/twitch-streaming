import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import requests
from kafka.errors import KafkaError

from src.ingestion.kafka_producer import criar_produtor, enviar_kafka
from src.ingestion.twitch_client import get_access_token, get_streams, save_json


RAIZ_PROJETO = Path(__file__).resolve().parents[2]
ARQUIVO_LOG = RAIZ_PROJETO / "logs" / "ingestion.jsonl"
PASTA_RAW = RAIZ_PROJETO / "data" / "raw" / "history"
INTERVALO_SEGUNDOS = 6 * 60 * 60
MAX_TENTATIVAS_API = 3


def configurar_logger():
    ARQUIVO_LOG.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("twitch_ingestion")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    if not logger.handlers:
        handler = logging.FileHandler(ARQUIVO_LOG, encoding="utf-8")
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)

    return logger


def tempo_de_espera(erro, tentativa):
    """Respeita o reset da Twitch no 429; demais erros usam backoff."""
    resposta = erro.response
    if resposta is not None and resposta.status_code == 429:
        reset = resposta.headers.get("Ratelimit-Reset")
        if reset:
            try:
                return max(1.0, float(reset) - time.time() + 1.0)
            except ValueError:
                pass

        retry_after = resposta.headers.get("Retry-After")
        if retry_after:
            try:
                return max(1.0, float(retry_after))
            except ValueError:
                pass

    return min(2 ** tentativa, 30)  # 2s, 4s, 8s...


def coletar_com_retry(token, registro):
    """Só repete chamadas à API. Nunca repete o envio ao Kafka aqui."""
    for tentativa in range(1, MAX_TENTATIVAS_API + 1):
        registro["tentativas_api"] = tentativa
        try:
            if token is None:
                token = get_access_token()
            dados = get_streams(token)
            if not isinstance(dados, dict) or not isinstance(dados.get("data"), list):
                raise ValueError("Resposta inesperada da Twitch: campo 'data' inválido")
            return dados, token

        except requests.HTTPError as erro:
            resposta = erro.response
            status_http = resposta.status_code if resposta is not None else None
            if status_http == 401:
                token = None  # nova autenticação na próxima tentativa
            elif status_http not in (408, 429) and not (
                status_http is None or status_http >= 500
            ):
                raise  # 400/403 etc.: repetir não corrige a requisição

            if tentativa == MAX_TENTATIVAS_API:
                raise
            espera = tempo_de_espera(erro, tentativa)
            print(f"API retornou HTTP {status_http}. Tentativa {tentativa}/3; nova tentativa em {espera:.0f}s.")
            time.sleep(espera)

        except requests.RequestException as erro:
            if tentativa == MAX_TENTATIVAS_API:
                raise
            espera = min(2 ** tentativa, 30)
            print(f"Erro de rede: {erro}. Nova tentativa em {espera}s.")
            time.sleep(espera)

    raise RuntimeError("Tentativas da API esgotadas")


def salvar_historico(dados, inicio_coleta, identificador):
    """Snapshot imutável: não substitui registros de coletas anteriores."""
    pasta = PASTA_RAW / inicio_coleta.strftime("%Y-%m-%d")
    pasta.mkdir(parents=True, exist_ok=True)
    arquivo = pasta / f"{identificador}.json"
    temporario = pasta / f"{identificador}.tmp"
    try:
        with temporario.open("w", encoding="utf-8") as f:
            json.dump(dados, f, ensure_ascii=False, indent=2)
        temporario.replace(arquivo)
    finally:
        temporario.unlink(missing_ok=True)
    return arquivo


def executar():
    logger = configurar_logger()
    producer = None
    token = None
    executar_uma_vez = os.getenv("TWITCH_RUN_ONCE") == "1"
    print("Coleta Twitch iniciada. Ctrl+C para encerrar.")

    try:
        while True:
            inicio_ciclo = time.monotonic()
            inicio_coleta = datetime.now(timezone.utc)
            identificador = uuid.uuid4().hex
            registro = {
                "collection_id": identificador,
                "timestamp": inicio_coleta.isoformat(timespec="seconds"),
                "status": "ERROR",
                "lives_coletadas": 0,
                "lives_enviadas": None,  # desconhecido se ocorrer falha durante o envio
                "tentativas_api": 0,
                "duracao_api_segundos": 0.0,
                "duracao_arquivo_segundos": 0.0,
                "duracao_kafka_segundos": 0.0,
                "duracao_segundos": 0.0,
                "arquivo_raw": None,
                "erro": None,
            }

            try:
                inicio_etapa = time.perf_counter()
                try:
                    dados, token = coletar_com_retry(token, registro)
                finally:
                    registro["duracao_api_segundos"] = round(time.perf_counter() - inicio_etapa, 3)

                lives = dados["data"]
                registro["lives_coletadas"] = len(lives)
                if not lives:
                    raise ValueError("Nenhuma transmissão retornada pela API")

                inicio_etapa = time.perf_counter()
                try:
                    save_json(dados)  # mantém o snapshot atual usado pelo projeto
                    arquivo = salvar_historico(dados, inicio_coleta, identificador)
                    registro["arquivo_raw"] = str(arquivo.relative_to(RAIZ_PROJETO))
                finally:
                    registro["duracao_arquivo_segundos"] = round(time.perf_counter() - inicio_etapa, 3)

                # Preserva todos os campos originais e adiciona rastreabilidade.
                dados_kafka = {
                    **dados,
                    "data": [
                        {**live, "collection_id": identificador,
                         "collected_at": inicio_coleta.isoformat(timespec="seconds")}
                        for live in lives
                    ],
                }

                inicio_etapa = time.perf_counter()
                try:
                    if producer is None:
                        producer = criar_produtor()
                    enviadas = enviar_kafka(dados_kafka, producer)
                    registro["lives_enviadas"] = enviadas
                    if enviadas != len(lives):
                        raise RuntimeError(f"Envio incompleto: {enviadas}/{len(lives)}")
                finally:
                    registro["duracao_kafka_segundos"] = round(time.perf_counter() - inicio_etapa, 3)

                registro["status"] = "SUCCESS"
                print(f"Coleta OK: {enviadas} transmissões entregues ao Kafka.")

            except KeyboardInterrupt:
                registro["erro"] = "Interrompida manualmente"
                raise
            except Exception as erro:
                registro["erro"] = f"{type(erro).__name__}: {erro}"
                print(f"Coleta falhou: {registro['erro']}")
                # Um envio parcial não é repetido automaticamente: deduplicação
                # e reprocessamento controlado entrarão na etapa PostgreSQL.
                if producer is not None and isinstance(erro, KafkaError):
                    try:
                        producer.close(timeout=5)
                    except Exception:
                        pass
                    producer = None
            finally:
                registro["duracao_segundos"] = round(time.monotonic() - inicio_ciclo, 3)
                logger.info(json.dumps(registro, ensure_ascii=False))

            if executar_uma_vez:
                break
            time.sleep(max(0, INTERVALO_SEGUNDOS - (time.monotonic() - inicio_ciclo)))

    except KeyboardInterrupt:
        print("Coleta interrompida pelo usuário.")
    finally:
        if producer is not None:
            producer.close(timeout=10)


if __name__ == "__main__":
    executar()
