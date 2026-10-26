import json
from datetime import datetime, timezone
from uuid import uuid4

from kafka import KafkaProducer

TOPICO = "twitch-streams"
SERVIDOR = "localhost:9092"


def criar_produtor():
    return KafkaProducer(bootstrap_servers=SERVIDOR, acks="all")


def enviar_kafka(dados, producer=None):

    criado_aqui = producer is None
    if criado_aqui:
        producer = criar_produtor()

    quantidade = 0
    try:
        for transmissao in dados["data"]:
            evento = {
                "event_id": str(uuid4()),
                "collected_at": datetime.now(timezone.utc).isoformat(),
                "source": "twitch",
                "stream": transmissao,
            }

            chave = str(transmissao["user_id"]).encode("utf-8")
            mensagem = json.dumps(evento, ensure_ascii=False).encode("utf-8")

            producer.send(TOPICO, key=chave, value=mensagem).get(timeout=10)
            quantidade += 1

        return quantidade
    finally:
        if criado_aqui:
            producer.close()
