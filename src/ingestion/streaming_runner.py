import time
import requests
from kafka.errors import KafkaError
from src.ingestion.kafka_producer import criar_produtor, enviar_kafka
from src.ingestion.twitch_client import get_access_token, get_streams, save_json

INTERVALO_SEGUNDOS = 60


def executar():
    producer = criar_produtor()
    token = None

    print("Coleta contínua iniciada. Use Ctrl+C para parar.")
    try:
        while True:
            try:
                if token is None:
                    token = get_access_token()

                dados = get_streams(token)
                save_json(dados)
                quantidade = enviar_kafka(dados, producer)
                print(f"{quantidade} transmissões enviadas ao Kafka.")

            except requests.HTTPError as erro:
                if erro.response is not None and erro.response.status_code == 401:
                    token = None  # Renova o token na próxima tentativa.
                print(f"Erro na API da Twitch: {erro}")

            except (requests.RequestException, KafkaError) as erro:
                print(f"Falha temporária: {erro}")

            time.sleep(INTERVALO_SEGUNDOS)

    except KeyboardInterrupt:
        print("Coleta interrompida.")
    finally:
        producer.close()


if __name__ == "__main__":
    executar()
