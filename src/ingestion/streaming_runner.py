import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
import requests
from kafka.errors import KafkaError
from src.ingestion.kafka_producer import criar_produtor, enviar_kafka
from src.ingestion.twitch_client import get_access_token, get_streams, save_json


INTERVALO_SEGUNDOS = 6 * 60 * 60
ARQUIVO_LOG = Path("logs/ingestion.jsonl")


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


def executar():
    logger = configurar_logger()
    producer = criar_produtor()
    token = None

    print("Coleta contínua iniciada. Use Ctrl+C para parar.")

    try:
        while True:
            horario = datetime.now(timezone.utc)
            inicio = time.perf_counter()

            status = "SUCCESS"
            quantidade = 0
            mensagem_erro = None

            try:
                if token is None:
                    token = get_access_token()

                dados = get_streams(token)
                quantidade = len(dados.get("data", []))

                save_json(dados)

                enviadas = enviar_kafka(dados, producer)
                print(f"{enviadas} transmissões enviadas ao Kafka.")

            except requests.HTTPError as erro:
                status = "ERROR"
                mensagem_erro = str(erro)

                if erro.response is not None and erro.response.status_code == 401:
                    token = None

                print(f"Erro na API da Twitch: {erro}")

            except (requests.RequestException, KafkaError) as erro:
                status = "ERROR"
                mensagem_erro = str(erro)

                print(f"Falha temporária: {erro}")

            except Exception as erro:
                status = "ERROR"
                mensagem_erro = f"{type(erro).__name__}: {erro}"

                print(f"Falha inesperada: {mensagem_erro}")

            finally:
                duracao = round(time.perf_counter() - inicio, 2)

                registro = {
                    "timestamp": horario.isoformat(timespec="seconds"),
                    "status": status,
                    "lives_coletadas": quantidade,
                    "duracao_segundos": duracao,
                    "erro": mensagem_erro
                }

                logger.info(json.dumps(registro, ensure_ascii=False))

            time.sleep(INTERVALO_SEGUNDOS)

    except KeyboardInterrupt:
        print("Coleta interrompida.")

    finally:
        producer.close()


if __name__ == "__main__":
    executar()