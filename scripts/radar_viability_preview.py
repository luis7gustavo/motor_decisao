"""Run the isolated synthetic viability fixture without network or database."""

import json
from dataclasses import asdict
from pathlib import Path

from app.viability_preview import evaluate, load_synthetic_fixture


FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "radar_demo" / "viability_base.json"


def main():
    scenario, today = load_synthetic_fixture(FIXTURE)
    result = evaluate(scenario, today=today)
    print(json.dumps({**asdict(result), "synthetic_fixture": str(FIXTURE.name),
                      "pncp_network_calls": 0, "database_writes": 0},
                     default=str, sort_keys=True, ensure_ascii=False))


if __name__ == "__main__":
    main()
