from __future__ import annotations
import json
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LOG = ROOT / "logs" / "scheduler_runs.jsonl"

def generate_report(path: Path = LOG, *, end: datetime | None = None) -> str:
    end = end or datetime.now(timezone.utc)
    start = end - timedelta(days=7)
    valid = []
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            try:
                record = json.loads(line)
                stamp = datetime.fromisoformat(record["timestamp"].replace("Z", "+00:00"))
                if stamp.tzinfo and start <= stamp.astimezone(timezone.utc) <= end:
                    valid.append(record)
            except (ValueError, KeyError, TypeError):
                continue
    counts = Counter(r.get("status") for r in valid)
    durations = [float(r["duracao_segundos"]) for r in valid if r.get("status") == "SUCCESS" and isinstance(r.get("duracao_segundos"), (int, float))]
    collected = sum(int(r.get("coletados") or 0) for r in valid if r.get("status") == "SUCCESS")
    inserts = sum(int(r.get("postgres_inseridos") or 0) for r in valid if r.get("status") == "SUCCESS")
    lines = [
        "# Operacao Twitch Streaming: janela movel de 7 dias",
        f"Periodo (UTC): {start.isoformat()} ate {end.isoformat()}",
        f"Execucoes registradas: {len(valid)} (meta teorica: ate 28, se PC e Docker permanecerem disponiveis)",
        f"Sucessos: {counts['SUCCESS']}; erros: {counts['ERROR']}; ignoradas por concorrencia: {counts['SKIPPED']}",
        f"Transmissoes coletadas em execucoes bem-sucedidas: {collected}",
        f"Novos registros no PostgreSQL nesses ciclos: {inserts}",
        f"Tempo medio de ciclos bem-sucedidos: {sum(durations)/len(durations):.2f}s" if durations else "Sem tempos de ciclos bem-sucedidos.",
        "Observacao: execucoes perdidas com PC desligado ou suspenso podem nao gerar log; compare com os horarios previstos no Agendador de Tarefas.",
    ]
    return "\n".join(lines) + "\n"

if __name__ == "__main__":
    result = generate_report()
    print(result)
    output_dir = ROOT / "reports" / "operations"
    output_dir.mkdir(parents=True, exist_ok=True)
    destination = output_dir / "relatorio_ultimos_7_dias.md"
    destination.write_text(result, encoding="utf-8")
    print(f"Relatorio salvo em: {destination}")
