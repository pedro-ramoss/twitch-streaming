from datetime import datetime, timezone
from unittest.mock import Mock

import pytest
import requests

from src.ingestion import streaming_runner as ingestao


def erro_http(status, headers=None):
    resposta = requests.Response()
    resposta.status_code = status
    resposta.headers.update(headers or {})
    return requests.HTTPError(f"HTTP {status}", response=resposta)


def test_erro_401_renova_token_e_tenta_novamente(monkeypatch):
    tokens = iter(["token-velho", "token-novo"])
    tokens_usados = []

    monkeypatch.setattr(ingestao, "get_access_token", lambda: next(tokens))
    monkeypatch.setattr(ingestao.time, "sleep", lambda segundos: None)

    def falso_get_streams(token):
        tokens_usados.append(token)
        if token == "token-velho":
            raise erro_http(401)
        return {"data": [{"id": "live-1"}]}

    monkeypatch.setattr(ingestao, "get_streams", falso_get_streams)
    registro = {}
    dados, token = ingestao.coletar_com_retry(None, registro)
    assert token == "token-novo"
    assert dados["data"][0]["id"] == "live-1"
    assert registro["tentativas_api"] == 2
    assert tokens_usados == ["token-velho", "token-novo"]


def test_erro_403_falha_sem_tentar_novamente(monkeypatch):
    chamadas = []
    monkeypatch.setattr(ingestao, "get_streams", lambda token: chamadas.append(1) or (_ for _ in ()).throw(erro_http(403)))
    monkeypatch.setattr(ingestao.time, "sleep", lambda segundos: pytest.fail("Nao deveria esperar para repetir HTTP 403"))
    registro = {}
    with pytest.raises(requests.HTTPError):
        ingestao.coletar_com_retry("token", registro)
    assert len(chamadas) == 1
    assert registro["tentativas_api"] == 1


def test_erro_500_repite_ate_limite(monkeypatch):
    chamadas = []
    esperas = []

    def falhar(token):
        chamadas.append(token)
        raise erro_http(500)

    monkeypatch.setattr(ingestao, "get_streams", falhar)
    monkeypatch.setattr(ingestao.time, "sleep", esperas.append)
    registro = {}
    with pytest.raises(requests.HTTPError):
        ingestao.coletar_com_retry("token", registro)
    assert len(chamadas) == ingestao.MAX_TENTATIVAS_API == 3
    assert esperas == [2, 4]
    assert registro["tentativas_api"] == 3


def test_429_respeita_retry_after(monkeypatch):
    monkeypatch.setattr(ingestao.time, "time", lambda: 1000)
    espera = ingestao.tempo_de_espera(erro_http(429, {"Retry-After": "12"}), tentativa=1)
    assert espera == 12


def test_salvar_historico_gera_arquivo_e_nao_deixa_temporario(tmp_path, monkeypatch):
    monkeypatch.setattr(ingestao, "PASTA_RAW", tmp_path)
    instante = datetime(2026, 10, 27, 12, 0, tzinfo=timezone.utc)
    dados = {"data": [{"id": "live-1"}]}
    caminho = ingestao.salvar_historico(dados, instante, "coleta-123")
    assert caminho.exists()
    assert caminho.name == "coleta-123.json"
    assert not (caminho.parent / "coleta-123.tmp").exists()
