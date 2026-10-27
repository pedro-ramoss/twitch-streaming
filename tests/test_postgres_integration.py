import os
import uuid
from datetime import datetime, timezone

import pytest

from src.processing import spark_reader as pg


@pytest.mark.skipif(
    os.getenv("TWITCH_TEST_POSTGRES_REAL") != "1",
    reason="Teste real desativado: defina TWITCH_TEST_POSTGRES_REAL=1 explicitamente",
)
def test_on_conflict_em_transacao_sem_persistir_dados():
    """Insere o mesmo UUID duas vezes; a transacao e desfeita ao final."""
    from dotenv import load_dotenv
    from psycopg.types.json import Jsonb

    load_dotenv(pg.RAIZ / ".env", override=False)
    event_id = uuid.uuid4()
    agora = datetime.now(timezone.utc)
    linha = (
        event_id, "live-pytest", "user-pytest", "pytest", "Teste",
        0, agora, agora, Jsonb({"origem": "teste-automatico"}),
    )
    conn = pg.psycopg.connect(**pg.config_postgres())
    try:
        with conn.cursor() as cur:
            cur.execute(pg.INSERT_SNAPSHOT, linha)
            assert cur.rowcount == 1
            cur.execute(pg.INSERT_SNAPSHOT, linha)
            assert cur.rowcount == 0
            cur.execute(
                "SELECT COUNT(*) FROM public.stream_snapshots WHERE event_id = %s",
                (event_id,),
            )
            assert cur.fetchone()[0] == 1
    finally:
        conn.rollback()  # Evita criar registros permanentes, mesmo em caso de erro.
        conn.close()
