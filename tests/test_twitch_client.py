
import json
import pytest
from unittest.mock import patch, Mock
from requests.exceptions import HTTPError

from src.ingestion.twitch_client import (
    get_access_token,
    get_streams,
    save_json
)


def test_get_access_token():

    with patch("src.ingestion.twitch_client.requests.post") as mock_post:
        resposta_falsa = Mock()
        resposta_falsa.status_code = 200
        resposta_falsa.json.return_value = {
            "access_token": "token_falso_123"
        }

        mock_post.return_value = resposta_falsa

        token = get_access_token()

        assert token == "token_falso_123"


def test_get_access_token_erro_401():

    with patch("src.ingestion.twitch_client.requests.post") as mock_post:
        resposta_falsa = Mock()
        resposta_falsa.status_code = 401
        resposta_falsa.raise_for_status.side_effect = HTTPError(
            "401 Unauthorized"
        )

        mock_post.return_value = resposta_falsa

        with pytest.raises(HTTPError):
            get_access_token()


def test_get_streams():

    with patch("src.ingestion.twitch_client.requests.get") as mock_get:
        resposta_falsa = Mock()
        resposta_falsa.status_code = 200
        resposta_falsa.json.return_value = {
            "data": [
                {
                    "game_name": "Minecraft",
                    "user_name": "streamer_teste",
                    "viewer_count": 1500
                }
            ]
        }

        mock_get.return_value = resposta_falsa

        dados = get_streams("token_falso_123")

        assert dados["data"][0]["game_name"] == "Minecraft"
        assert dados["data"][0]["viewer_count"] == 1500


def test_get_streams_erro_500():

    with patch("src.ingestion.twitch_client.requests.get") as mock_get:
        resposta_falsa = Mock()
        resposta_falsa.status_code = 500
        resposta_falsa.raise_for_status.side_effect = HTTPError(
            "500 Internal Server Error"
        )

        mock_get.return_value = resposta_falsa

        with pytest.raises(HTTPError):
            get_streams("token_falso_123")


def test_save_json(tmp_path, monkeypatch):

    monkeypatch.chdir(tmp_path)

    dados = {
        "data": [
            {
                "game_name": "Minecraft",
                "viewer_count": 1500
            }
        ]
    }

    save_json(dados)

    arquivo = tmp_path / "data" / "raw" / "twitch_streams.json"

    assert arquivo.exists()

    with open(arquivo, "r", encoding="utf-8") as f:
        conteudo = json.load(f)

    assert conteudo == dados
