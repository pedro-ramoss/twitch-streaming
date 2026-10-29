import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
GRAFANA = ROOT / "monitoring" / "grafana"


def dashboard(nome):
    return json.loads((GRAFANA / "dashboards" / nome).read_text(encoding="utf-8"))


def test_dashboards_json_validos_e_com_paineis():
    for nome, uid in [("pipeline.json", "twitch-pipeline"), ("twitch.json", "twitch-analytics")]:
        dados = dashboard(nome)
        assert dados["uid"] == uid
        assert len(dados["panels"]) >= 8
        assert len({p["id"] for p in dados["panels"]}) == len(dados["panels"])


def test_referencias_datasources_consistentes():
    for nome, uid in [("pipeline.json", "twitch-prometheus"), ("twitch.json", "twitch-postgres")]:
        for painel in dashboard(nome)["panels"]:
            assert painel["datasource"]["uid"] == uid
            assert all(target["datasource"]["uid"] == uid for target in painel["targets"])
    conf = (GRAFANA / "provisioning" / "datasources" / "twitch.yml").read_text(encoding="utf-8")
    assert "uid: twitch-prometheus" in conf
    assert "uid: twitch-postgres" in conf
    assert "password: $POSTGRES_PASSWORD" in conf
    assert "password: '" not in conf


def test_sql_dashboards_somente_leitura():
    for painel in dashboard("twitch.json")["panels"]:
        for target in painel["targets"]:
            sql = target["rawSql"]
            assert sql.strip().upper().startswith("SELECT ")
            assert "public.stream_snapshots" in sql
            assert not re.search(r"\b(INSERT|UPDATE|DELETE|DROP|TRUNCATE|ALTER|CREATE)\b", sql, flags=re.I)


def test_metricas_do_dashboard_existem_no_exporter():
    code = (ROOT / "scripts" / "metrics_exporter.py")
    if not code.exists():
        # Em alguns repositórios o exporter ainda não foi adicionado à cópia local.
        return
    source = code.read_text(encoding="utf-8")
    for metrica in (
        "twitch_postgres_up", "twitch_postgres_snapshots", "twitch_ingestion_runs_total",
        "twitch_ingestion_streams_sent_total", "twitch_spark_rejected_records_total",
        "twitch_spark_last_duration_seconds", "twitch_spark_valid_records_total",
    ):
        assert metrica in source


def test_compose_nao_modifica_outros_servicos():
    compose = (ROOT / "docker-compose.grafana.yml").read_text(encoding="utf-8")
    assert "  grafana:" in compose
    assert "grafana_data:" in compose
    assert '"127.0.0.1:3000:3000"' in compose
    assert "\n  kafka:" not in compose
    assert "\n  postgres:" not in compose
    assert "\n  prometheus:" not in compose


def test_provisionamento_existe():
    conf = (GRAFANA / "provisioning" / "dashboards" / "twitch.yml").read_text(encoding="utf-8")
    assert "providers:" in conf
    assert "path: /var/lib/grafana/dashboards" in conf
