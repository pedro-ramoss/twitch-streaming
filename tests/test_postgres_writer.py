import json
import uuid
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.processing import spark_reader as pg


def linha_mock(dedup_key="collection:coleta-1:live-1"):
    return SimpleNamespace(
        dedup_key=dedup_key,
        stream_id="live-1",
        user_id="user-1",
        user_name="Streamer",
        game_name="Counter-Strike",
        viewer_count=25,
        started_at=datetime(2026, 10, 27, 10, tzinfo=timezone.utc),
        collected_at=datetime(2026, 10, 27, 12, tzinfo=timezone.utc),
        raw_message=json.dumps({
            "collection_id": "coleta-1",
            "stream": {"id": "live-1", "user_id": "user-1", "viewer_count": 25,
                       "novidade_api": "preservar", "tags": ["fps"]},
        }),
        indice_stream=0,
    )


def test_uuid_do_mesmo_evento_e_estavel():
    primeiro = pg.valores_postgres(linha_mock())[0]
    segundo = pg.valores_postgres(linha_mock())[0]
    outro = pg.valores_postgres(linha_mock("collection:coleta-2:live-1"))[0]
    assert isinstance(primeiro, uuid.UUID)
    assert primeiro == segundo
    assert primeiro != outro


def test_json_original_preserva_campos_novos_da_api():
    valores = pg.valores_postgres(linha_mock())
    payload = valores[-1].obj
    assert payload["novidade_api"] == "preservar"
    assert payload["tags"] == ["fps"]
    assert payload["collection_id"] == "coleta-1"
    assert valores[5] == 25


@pytest.mark.parametrize("indice, esperado", [(0, "a"), (1, "b")])
def test_json_formato_legado_lista(indice, esperado):
    mensagem = json.dumps({"data": [{"id": "a"}, {"id": "b"}]})
    assert pg.recuperar_json_stream(mensagem, indice)["id"] == esperado


def test_json_invalido_para_conversao_gera_valueerror():
    with pytest.raises(ValueError):
        pg.recuperar_json_stream(json.dumps([1, 2, 3]), 0)


def test_senha_postgres_obrigatoria(monkeypatch):
    monkeypatch.delenv("POSTGRES_PASSWORD", raising=False)
    with pytest.raises(RuntimeError, match="POSTGRES_PASSWORD"):
        pg.config_postgres()


def test_configuracao_postgres_lida_de_variaveis_de_ambiente(monkeypatch):
    monkeypatch.setenv("POSTGRES_PASSWORD", "senha-ficticia")
    monkeypatch.setenv("POSTGRES_PORT", "5433")
    config = pg.config_postgres()
    assert config["port"] == 5433
    assert config["password"] == "senha-ficticia"


class FrameFalso:
    def __init__(self, quantidade):
        self.quantidade = quantidade

    def select(self, *campos):
        assert "dedup_key" in campos
        return self

    def toLocalIterator(self):
        for indice in range(self.quantidade):
            yield linha_mock(f"collection:coleta-{indice}:live-1")


def test_escrita_em_lotes_de_200_e_sql_idempotente(monkeypatch):
    """Verifica contrato da escrita; o ON CONFLICT real sera testado no PostgreSQL."""
    conexao = MagicMock()
    # psycopg.connect() e usado em um with: o __enter__ deve devolver
    # a mesma conexao configurada, nao um MagicMock diferente.
    conexao.__enter__.return_value = conexao
    cursor = conexao.cursor.return_value.__enter__.return_value
    cursor.rowcount = 5  # valor ilustrativo do driver; banco nao e contatado
    # O gravador limpa a lista pendentes apos cada chamada; capture uma copia
    # no momento do executemany para verificar o tamanho real de cada lote.
    lotes_enviados = []

    def registrar_lote(sql, parametros, returning=False):
        lotes_enviados.append((sql, tuple(parametros), returning))

    cursor.executemany.side_effect = registrar_lote
    conectou = []

    def conectar(**kwargs):
        conectou.append(kwargs)
        return conexao

    monkeypatch.setattr(pg.psycopg, "connect", conectar)
    monkeypatch.setenv("POSTGRES_PASSWORD", "senha-ficticia")

    candidatos, inseridos = pg.gravar_postgres(FrameFalso(205))
    assert candidatos == 205
    assert inseridos == 10  # somatorio dos dois rowcounts simulados
    assert cursor.executemany.call_count == 2
    primeiros, segundos = lotes_enviados
    assert len(primeiros[1]) == 200
    assert len(segundos[1]) == 5
    assert "ON CONFLICT (event_id) DO NOTHING" in primeiros[0]
    assert primeiros[2] is False
    assert conectou[0]["dbname"] == pg.config_postgres()["dbname"]


def test_erro_de_escrita_nao_e_silenciado(monkeypatch):
    conexao = MagicMock()
