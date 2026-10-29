"""Atualiza apenas a secao de operacao do README usando auditorias reais.

Uso (na raiz do projeto): python -m scripts.atualizar_readme
Nao altera dados, credenciais nem checkpoints.
"""
from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import mean

ROOT = Path(__file__).resolve().parents[1]
README = ROOT / "README.md"
AUDIT = ROOT / "logs" / "scheduler_runs.jsonl"
START = "<!-- 7D_START -->"
END = "<!-- 7D_END -->"


def carregar_registros(path: Path) -> list[dict]:
    if not path.exists():
        return []
    data = []
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            item = json.loads(line)
            timestamp = datetime.fromisoformat(str(item["timestamp"]).replace("Z", "+00:00"))
            if timestamp.tzinfo is None:
                continue
            item["_timestamp_utc"] = timestamp.astimezone(timezone.utc)
            data.append(item)
        except (ValueError, KeyError, TypeError, json.JSONDecodeError):
            continue
    return sorted(data, key=lambda x: x["_timestamp_utc"])


def criar_resumo(registros: list[dict], agora: datetime) -> str:
    agora = agora.astimezone(timezone.utc)
    inicio = agora - timedelta(days=7)
    recentes = [r for r in registros if inicio <= r["_timestamp_utc"] <= agora]
    total = len(recentes)
    c = Counter(str(r.get("status", "UNKNOWN")) for r in recentes)
    success = [r for r in recentes if r.get("status") == "SUCCESS"]
    coletas = sum(int(r.get("coletados") or 0) for r in success)
    insercoes = sum(int(r.get("postgres_inseridos") or 0) for r in success)
    duracoes = [float(r["duracao_segundos"]) for r in success
                if isinstance(r.get("duracao_segundos"), (int, float))]
    ultima = recentes[-1]["_timestamp_utc"].strftime("%d/%m/%Y %H:%M UTC") if recentes else "nenhuma"
    data_inicio = registros[0]["_timestamp_utc"] if registros else None
    sete_dias_transcorridos = data_inicio is not None and (agora - data_inicio) >= timedelta(days=7)
    status = ("**Janela de sete dias transcorrida; confira os horários efetivamente realizados.**"
              if sete_dias_transcorridos else
              "**Em acompanhamento: ainda não há sete dias desde o primeiro registro de auditoria.**")
    media = f"{mean(duracoes):.2f} s" if duracoes else "sem amostra"
    return "\n".join([
        status,
        "",
        f"*Período do relatório:* {inicio:%d/%m/%Y %H:%M} a {agora:%d/%m/%Y %H:%M} UTC (janela móvel).",
        f"- Execuções **registradas**: **{total}** (referência teórica: até 28 em sete dias, com PC disponível).",
        f"- Sucessos: **{c['SUCCESS']}**; erros: **{c['ERROR']}**; ignoradas por concorrência: **{c['SKIPPED']}**.",
        f"- Transmissões coletadas em ciclos bem-sucedidos: **{coletas}**.",
        f"- Novos registros inseridos no PostgreSQL nesses ciclos: **{insercoes}**.",
        f"- Duração média dos ciclos bem-sucedidos: **{media}**.",
        f"- Última auditoria registrada: **{ultima}**.",
        "",
        "*Leitura correta:* períodos em que o computador esteve desligado ou suspenso podem não gerar auditoria. "
        "Este resumo não afirma disponibilidade contínua nem latência ponta a ponta.",
    ])


def atualizar(readme: Path = README, audit: Path = AUDIT, *, agora: datetime | None = None) -> Path:
    if not readme.exists():
        raise FileNotFoundError(f"README nao encontrado: {readme}")
    texto = readme.read_text(encoding="utf-8")
    if texto.count(START) != 1 or texto.count(END) != 1:
        raise ValueError("Marcadores 7D_START / 7D_END ausentes ou duplicados")
    antes, restante = texto.split(START, 1)
    _, depois = restante.split(END, 1)
    resumo = criar_resumo(carregar_registros(audit), agora or datetime.now(timezone.utc))
    readme.write_text(antes + START + "\n" + resumo + "\n" + END + depois,
                      encoding="utf-8")
    return readme


if __name__ == "__main__":
    destino = atualizar()
    print(f"Secao de operacao atualizada no README: {destino}")
