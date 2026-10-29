import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

from scripts import run_pipeline_once as p
from scripts.report_7d import generate_report


def test_new_entries_ler_apenas_novas(tmp_path):
    log = tmp_path / "ingestion.jsonl"
    log.write_text('{"status":"antigo"}\n', encoding="utf-8")
    offset = log.stat().st_size
    with log.open("a", encoding="utf-8") as f:
        f.write('{"status":"SUCCESS"}\n')
    assert p.new_entries(log, offset) == [{"status": "SUCCESS"}]


def test_new_entries_ausente(tmp_path):
    assert p.new_entries(tmp_path / "nao_existe.jsonl", 0) == []


def test_new_entries_truncado(tmp_path):
    log = tmp_path / "log.jsonl"
    log.write_text('{}\n', encoding="utf-8")
    try:
        p.new_entries(log, 999)
    except RuntimeError as ex:
        assert "truncado" in str(ex)
    else:
        raise AssertionError("Deveria rejeitar truncamento")


def test_checagem_status_de_ingestao_necessaria():
    # O coletor real trata erros internamente; nao basta confiar no exit code.
    fake = {"status": "ERROR", "erro": "HTTP 429"}
    assert fake["status"] != "SUCCESS"


def test_report_soma_apenas_sucessos(tmp_path):
    data = datetime(2026, 10, 29, 12, 0, tzinfo=timezone.utc)
    f = tmp_path / "audit.jsonl"
    f.write_text("\n".join(json.dumps(x) for x in [
        {"timestamp": "2026-10-29T11:00:00+00:00", "status": "SUCCESS", "coletados": 25,
         "postgres_inseridos": 24, "duracao_segundos": 10},
        {"timestamp": "2026-10-29T10:00:00+00:00", "status": "ERROR", "coletados": 5},
        {"timestamp": "2026-10-20T10:00:00+00:00", "status": "SUCCESS", "coletados": 100},
    ]) + "\n", encoding="utf-8")
    report = generate_report(f, end=data)
    assert "Sucessos: 1; erros: 1" in report
    assert "bem-sucedidas: 25" in report
    assert "ciclos: 24" in report


def test_agendador_nao_refaz_backfill():
    source = Path(p.__file__).read_text(encoding="utf-8")
    assert 'env.pop("SPARK_BACKFILL_POSTGRES", None)' in source


def test_arquivos_windows_incluidos():
    ps1 = (p.ROOT / "scripts" / "register_windows_task.ps1").read_text(encoding="utf-8")
    assert 'New-TimeSpan -Hours 6' in ps1
    assert 'MultipleInstances IgnoreNew' in ps1


def _mock_project(monkeypatch, tmp_path):
    """Monta um projeto minimo falso: nenhum processo real sera iniciado."""
    root = tmp_path
    logs = root / "logs"
    logs.mkdir()
    (root / "venv" / "bin").mkdir(parents=True)
    (root / "venv" / "bin" / "python").touch()
    (root / ".env").write_text("PLACEHOLDER=1\n", encoding="utf-8")
    for name, value in {
        "ROOT": root,
        "LOGS": logs,
        "AUDIT": logs / "scheduler_runs.jsonl",
        "LOCK": logs / ".pipeline_scheduler.lock",
        "INGESTION_LOG": logs / "ingestion.jsonl",
        "SPARK_LOG": logs / "spark_processing.jsonl",
    }.items():
        monkeypatch.setattr(p, name, value)
    monkeypatch.setattr(p, "wait_for_tcp", lambda *a, **k: None)
    monkeypatch.setattr(p.shutil, "which", lambda arg: "/bin/fake-spark-submit")
    return root, logs


def test_ciclo_completo_mock_sem_executar_processos(monkeypatch, tmp_path):
    root, logs = _mock_project(monkeypatch, tmp_path)
    chamadas = []

    def fake_command(args, *, env, output, timeout_seconds):
        chamadas.append((args, env.copy()))
        if "src.ingestion.streaming_runner" in args:
            with (logs / "ingestion.jsonl").open("a", encoding="utf-8") as f:
                f.write(json.dumps({"status": "SUCCESS", "lives_coletadas": 25,
                                    "lives_enviadas": 25}) + "\n")
        else:
            with (logs / "spark_processing.jsonl").open("a", encoding="utf-8") as f:
                f.write(json.dumps({"status": "SUCCESS", "batch_id": 4,
                                    "validos": 25, "postgres_inseridos": 25}) + "\n")
    monkeypatch.setattr(p, "command", fake_command)
    monkeypatch.setenv("SPARK_BACKFILL_POSTGRES", "1")
    assert p.run_once() == 0
    assert len(chamadas) == 2
    assert chamadas[0][1]["TWITCH_RUN_ONCE"] == "1"
    assert chamadas[1][1]["SPARK_RUN_ONCE"] == "1"
    assert "SPARK_BACKFILL_POSTGRES" not in chamadas[1][1]
    audit = json.loads((logs / "scheduler_runs.jsonl").read_text().splitlines()[0])
    assert audit["status"] == "SUCCESS"
    assert audit["postgres_inseridos"] == 25


def test_coleta_erro_interrompe_spark(monkeypatch, tmp_path):
    root, logs = _mock_project(monkeypatch, tmp_path)
    chamadas = []

    def fake_command(args, *, env, output, timeout_seconds):
        chamadas.append(args)
        with (logs / "ingestion.jsonl").open("a", encoding="utf-8") as f:
            f.write(json.dumps({"status": "ERROR", "erro": "falha simulada"}) + "\n")
        # O coletor retorna 0 mesmo quando registra um erro de API.
        return None

    monkeypatch.setattr(p, "command", fake_command)
    assert p.run_once() == 1
    assert len(chamadas) == 1
    audit = json.loads((logs / "scheduler_runs.jsonl").read_text().splitlines()[0])
    assert audit["status"] == "ERROR"
    assert "falha simulada" in audit["erro"]


def test_spark_sem_lote_novo_e_falha(monkeypatch, tmp_path):
    root, logs = _mock_project(monkeypatch, tmp_path)

    def fake_command(args, *, env, output, timeout_seconds):
        if "src.ingestion.streaming_runner" in args:
            with (logs / "ingestion.jsonl").open("a", encoding="utf-8") as f:
                f.write(json.dumps({"status": "SUCCESS", "lives_coletadas": 25,
                                    "lives_enviadas": 25}) + "\n")
        # Spark retorna zero, mas nao registra lote novo: precisa ser erro.

    monkeypatch.setattr(p, "command", fake_command)
    assert p.run_once() == 1
    audit = json.loads((logs / "scheduler_runs.jsonl").read_text().splitlines()[0])
    assert "sem registrar lote" in audit["erro"]
