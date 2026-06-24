from __future__ import annotations

from typing import Any


def confidence_level(final_score: float, *, agreed: bool) -> str:
    if agreed and (final_score >= 0.75 or final_score <= 0.20):
        return "alta"
    if agreed or final_score >= 0.50:
        return "media"
    return "baixa"


def explain_prediction(
    *,
    heuristic_decision: str,
    ml_decision: str,
    final_decision: str,
    features: dict[str, float],
) -> str:
    signals: list[str] = []
    if features["net_margin_pct"] > 0:
        signals.append("margem estimada positiva")
    else:
        signals.append("margem estimada ausente ou negativa")
    if features["demand_score"] >= 60:
        signals.append("demanda observada forte")
    elif features["demand_score"] < 30:
        signals.append("demanda observada limitada")
    if features["match_confidence"] >= 75:
        signals.append("match com boa confianca")
    elif features["match_confidence"] < 58:
        signals.append("match ainda fraco")
    if features["market_source_count"] < 2:
        signals.append("pouca diversidade de fontes")
    if features["risk_flag_count"] >= 3:
        signals.append("multiplos sinais de risco")

    signal_text = ", ".join(signals[:4]) or "sinais insuficientes"
    if heuristic_decision == ml_decision:
        return (
            f"Heuristica e ML concordam em {final_decision}. "
            f"Principais sinais: {signal_text}."
        )
    return (
        f"Heuristica indicou {heuristic_decision}, enquanto o ML indicou {ml_decision}. "
        f"A regra hibrida manteve {final_decision} para revisao auditavel. "
        f"Principais sinais: {signal_text}."
    )
