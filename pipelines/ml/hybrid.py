from __future__ import annotations

from dataclasses import dataclass

from pipelines.ml.config import MlConfig
from pipelines.ml.explain_predictions import confidence_level


@dataclass(frozen=True)
class HybridDecision:
    ml_decision: str
    final_score: float
    final_decision: str
    decision_source: str
    confidence_level: str


def combine_decisions(
    *,
    heuristic_score: float,
    heuristic_decision: str,
    ml_score: float,
    config: MlConfig,
) -> HybridDecision:
    ml_decision = "revisar" if ml_score >= config.review_threshold else "ignorar"
    final_score = (
        (config.heuristic_weight * heuristic_score) + (config.ml_weight * ml_score)
        if config.use_hybrid_score
        else heuristic_score
    )
    final_score = max(0.0, min(1.0, final_score))

    # Safety guardrail: ML supports and challenges the baseline, but it cannot
    # autonomously promote an ignored or review item to comprar_teste.
    if (
        heuristic_decision == "comprar_teste"
        and ml_decision == "revisar"
        and final_score >= config.opportunity_threshold
    ):
        final_decision = "comprar_teste"
        decision_source = "heuristica_confirmada_ml"
    elif heuristic_decision in {"comprar_teste", "revisar"} or final_score >= config.review_threshold:
        final_decision = "revisar"
        decision_source = (
            "hibrido_revisao"
            if heuristic_decision != ml_decision
            else "heuristica_confirmada_ml"
        )
    else:
        final_decision = "ignorar"
        decision_source = "heuristica_confirmada_ml" if ml_decision == "ignorar" else "hibrido_revisao"

    agreed = heuristic_decision == final_decision or (
        heuristic_decision == "comprar_teste" and final_decision == "comprar_teste"
    )
    return HybridDecision(
        ml_decision=ml_decision,
        final_score=round(final_score, 4),
        final_decision=final_decision,
        decision_source=decision_source,
        confidence_level=confidence_level(final_score, agreed=agreed),
    )
