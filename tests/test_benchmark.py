"""Testes DE-018, sem conexao com PostgreSQL e sem iniciar JVM."""
import json

from scripts import benchmark as bm


def test_resumo_tempos_vazio():
    assert bm.resumo_tempos([]) == {
        "amostras": 0, "min_ms": None, "p50_ms": None,
        "p95_ms": None, "max_ms": None,
    }


def test_resumo_tempos_mediana_e_percentil():
    metricas = bm.resumo_tempos([50, 10, 30, 20, 40])
    assert metricas == {
        "amostras": 5, "min_ms": 10.0, "p50_ms": 30.0,
        "p95_ms": 50.0, "max_ms": 50.0,
    }


def test_logs_linhas_incompletas_nao_quebram(tmp_path):
    arquivo = tmp_path / "ingestion.jsonl"
    arquivo.write_text('{"status":"SUCCESS","duracao_segundos":1.25}\n{incorreto\n42\n\n', encoding="utf-8")
    assert bm.carregar_jsonl(arquivo) == [{"status": "SUCCESS", "duracao_segundos": 1.25}]


def test_tempo_log_de_segundos_para_ms():
    entradas = [{"duracao_segundos": 0.5}, {"duracao_segundos": 1.25}, {"status": "ERROR"}]
    assert bm.tempos_log(entradas, "duracao_segundos")["p50_ms"] == 875.0


def test_plano_explain_analisa_nos():
    plano = {"Planning Time": 0.5, "Execution Time": 1.234,
             "Plan": {"Node Type": "Limit", "Plans": [{"Node Type": "Index Scan"}]}}
    metrica = bm.coletar_planos(plano)
    assert metrica["nos"] == ["Limit", "Index Scan"]
    assert metrica["execucao_ms"] == 1.234


def test_medidor_conta_aquecimentos_e_repeticoes():
    chamadas = []
    def executar():
        chamadas.append(True)
    resultado = bm.medir(3, 2, executar)
    assert len(chamadas) == 5
    assert resultado["amostras"] == 3


def test_relatorio_nao_divulga_sql_com_credenciais():
    exemplo = {
        "timestamp_utc": "2026-10-27T12:00:00+00:00",
        "logs": {"ingestao": {"sucesso": 0, "falhas": 0,
                             "duracao_total_ms": bm.resumo_tempos([])},
                 "spark": {"sucesso": 0, "falhas": 0,
                           "duracao_lote_ms": bm.resumo_tempos([]),
                           "duracao_postgres_ms": bm.resumo_tempos([])}},
        "erros": {},
    }
    saida = bm.gerar_markdown(exemplo)
    assert "Benchmark DE-018" in saida
    assert "Não mede ponta a ponta" in saida
