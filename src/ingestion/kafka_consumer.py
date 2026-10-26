import json
from kafka import KafkaConsumer
from src.ingestion.kafka_producer import SERVIDOR, TOPICO

def executar():
    consumer = KafkaConsumer(
        TOPICO,
        bootstrap_servers=SERVIDOR,
        auto_offset_reset="latest",  # Aguarda novas mensagens (não as antigas).
        group_id=None,
    )

    print("Aguardando novas transmissões. Use Ctrl+C para parar.")
    try:
        for mensagem in consumer:
            evento = json.loads(mensagem.value.decode("utf-8"))
            live = evento["stream"]
            print(
                f'{live["user_name"]} | {live["game_name"]} | '
                f'{live["viewer_count"]} espectadores | '
                f'{evento["collected_at"]}'
            )
    except KeyboardInterrupt:
        print("Consumer encerrado.")
    finally:
        consumer.close()


if __name__ == "__main__":
    executar()
