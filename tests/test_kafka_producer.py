import json
from unittest.mock import Mock

from src.ingestion.kafka_producer import enviar_kafka


def test_uma_mensagem_por_transmissao():
    producer = Mock()
    dados = {"data": [
        {"user_id": "1", "user_name": "ana", "game_name": "Minecraft", "viewer_count": 30},
        {"user_id": "2", "user_name": "bia", "game_name": "Fortnite", "viewer_count": 20},
    ]}

    quantidade = enviar_kafka(dados, producer)

    assert quantidade == 2
    assert producer.send.call_count == 2
    producer.send.return_value.get.assert_called_with(timeout=10)

    primeira_chamada = producer.send.call_args_list[0]
    assert primeira_chamada.args[0] == "twitch-streams"
    assert primeira_chamada.kwargs["key"] == b"1"
    evento = json.loads(primeira_chamada.kwargs["value"].decode("utf-8"))
    assert evento["source"] == "twitch"
    assert evento["stream"]["game_name"] == "Minecraft"
    assert "event_id" in evento
    assert "collected_at" in evento


def test_lista_vazia():
    producer = Mock()
    assert enviar_kafka({"data": []}, producer) == 0
    producer.send.assert_not_called()
