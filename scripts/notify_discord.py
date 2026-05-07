from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import httpx
import psycopg

DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "").strip()
DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql+psycopg://motor:motor@postgres:5432/motor_decisao",
)

# Janela de horas para buscar runs recentes
LOOKBACK_HOURS = 3

BRT = timezone(timedelta(hours=-3))

STATUS_EMOJI = {
    "completed": "✅",
    "completed_partial": "⚠️",
    "failed": "❌",
    "running": "🔄",
}

STATUS_COLOR = {
    "completed": 0x00C851,
    "completed_partial": 0xFFBB33,
    "failed": 0xFF4444,
    "running": 0x33B5E5,
}

SOURCE_EMOJI = {
    "shopee": "🛒",
    "amazon": "📦",
    "kabum": "🖥️",
    "terabyte": "💻",
    "pichau": "🔧",
    "magalu": "🏪",
    "aliexpress": "🌏",
    "buscape": "💰",
    "zoom": "🔍",
    "mercado_livre": "🟡",
}


def _psycopg_url(url: str) -> str:
    return url.replace("postgresql+psycopg://", "postgresql://")


def _query_recent_pipeline_runs(cur, hours: int) -> list:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    cur.execute(
        """
        SELECT id, pipeline_name, status, created_at, finished_at, error_message
        FROM control.pipeline_runs
        WHERE created_at >= %s
        ORDER BY created_at DESC
        """,
        (cutoff,),
    )
    return cur.fetchall()


def _query_source_runs(cur, pipeline_run_id) -> list:
    cur.execute(
        """
        SELECT source_name, status, records_extracted, records_loaded,
               records_skipped, error_message, metadata
        FROM control.source_runs
        WHERE pipeline_run_id = %s
        ORDER BY source_name
        """,
        (pipeline_run_id,),
    )
    return cur.fetchall()


def _duration_str(start: datetime, end: datetime | None) -> str:
    if not end:
        return "em andamento"
    secs = int((end - start).total_seconds())
    return f"{secs // 60}m {secs % 60}s"


def _build_source_field(row: tuple) -> dict:
    src_name, status, extracted, loaded, skipped, error, metadata = row
    extracted = extracted or 0
    loaded = loaded or 0
    skipped = skipped or 0

    blocked = 0
    if isinstance(metadata, dict):
        blocked = metadata.get("blocked", 0)

    emoji = SOURCE_EMOJI.get(src_name, "📌")
    st_emoji = STATUS_EMOJI.get(status, "❓")

    if status == "failed":
        line = f"{st_emoji} falhou"
        if error:
            line += f"\n> {error[:80]}"
    elif loaded == 0 and extracted == 0:
        line = "⚠️ nenhum item coletado"
    elif blocked > 0:
        line = f"{st_emoji} `{loaded}/{extracted}` novos · ⚠️ {blocked} bloqueados"
    else:
        line = f"{st_emoji} `{loaded}/{extracted}` novos"
        if skipped:
            line += f" · {skipped} já existiam"

    return {"name": f"{emoji} {src_name}", "value": line, "inline": True}


def _build_embed(run: tuple, source_rows: list) -> dict:
    run_id, name, status, created_at, finished_at, error_msg = run

    total_loaded = sum((r[3] or 0) for r in source_rows)
    any_failed = any(r[1] == "failed" or (r[2] or 0) == 0 for r in source_rows)

    # Override color se houver fontes com problema
    if status == "completed" and any_failed:
        color = STATUS_COLOR["completed_partial"]
        title_emoji = "⚠️"
    else:
        color = STATUS_COLOR.get(status, 0x888888)
        title_emoji = STATUS_EMOJI.get(status, "❓")

    started_brt = created_at.astimezone(BRT).strftime("%d/%m %H:%M")
    dur = _duration_str(created_at, finished_at)
    pipeline_label = name.replace("_", " ").title()

    description = f"**{total_loaded} registros novos** em {dur}"
    if error_msg:
        description += f"\n❌ `{error_msg[:120]}`"

    fields = [_build_source_field(r) for r in source_rows]

    return {
        "title": f"{title_emoji} {pipeline_label} — {started_brt} (BRT)",
        "description": description,
        "color": color,
        "fields": fields[:25],
        "timestamp": (finished_at or created_at).isoformat(),
        "footer": {"text": "motor_decisao · coleta automática"},
    }


def _build_payload(pipeline_runs: list, cur) -> dict:
    if not pipeline_runs:
        return {
            "embeds": [
                {
                    "title": "⚠️ Motor de Decisão — Sem coletas recentes",
                    "description": f"Nenhuma coleta nas últimas {LOOKBACK_HOURS}h.",
                    "color": 0xFFA500,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "footer": {"text": "motor_decisao · monitor"},
                }
            ]
        }

    embeds = []
    for run in pipeline_runs:
        sources = _query_source_runs(cur, run[0])
        embeds.append(_build_embed(run, sources))

    return {"embeds": embeds[:10]}


def _send(payload: dict, webhook_url: str) -> bool:
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "DiscordBot (motor_decisao, 1.0)",
    }
    try:
        resp = httpx.post(webhook_url, json=payload, headers=headers, timeout=10)
        return resp.status_code in (200, 204)
    except Exception as exc:
        print(f"Erro ao enviar para Discord: {exc}", file=sys.stderr)
        return False


def _send_error(msg: str, webhook_url: str) -> None:
    _send(
        {
            "embeds": [
                {
                    "title": "❌ Motor de Decisão — Erro de monitoramento",
                    "description": f"```{msg[:1000]}```",
                    "color": 0xFF4444,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "footer": {"text": "motor_decisao · monitor"},
                }
            ]
        },
        webhook_url,
    )


def main() -> int:
    webhook_url = DISCORD_WEBHOOK_URL
    if not webhook_url:
        print("DISCORD_WEBHOOK_URL não definido.", file=sys.stderr)
        return 1

    try:
        conn = psycopg.connect(_psycopg_url(DATABASE_URL))
    except Exception as exc:
        _send_error(f"Não foi possível conectar ao banco:\n{exc}", webhook_url)
        return 1

    with conn, conn.cursor() as cur:
        runs = _query_recent_pipeline_runs(cur, LOOKBACK_HOURS)
        payload = _build_payload(runs, cur)

    ok = _send(payload, webhook_url)
    if ok:
        print("Notificação enviada para o Discord.")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
