from __future__ import annotations

import argparse
import asyncio
import json
import sys
from typing import Any


def _print(payload: Any) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="sillo", description="CLI operacional da plataforma SILLO")
    areas = parser.add_subparsers(dest="area", required=True)
    collect = areas.add_parser("collect", help="Executa e inspeciona coletas")
    commands = collect.add_subparsers(dest="command", required=True)

    run = commands.add_parser("run", help="Executa uma fonte ou perfil")
    target = run.add_mutually_exclusive_group(required=True)
    target.add_argument("--source", help="ID da fonte declarada no registry")
    target.add_argument("--profile", choices=["market", "suppliers", "local", "full"])
    run.add_argument("--canary", action="store_true", help="Limita a execucao para validacao segura")
    run.add_argument("--max-results", type=int, default=None)
    run.add_argument("--prefect", action="store_true", help="Submete ao worker Prefect")

    list_command = commands.add_parser("list", help="Lista fontes e perfis")
    list_command.add_argument("--all", action="store_true", help="Inclui fontes desabilitadas")
    list_command.add_argument("--profile", choices=["market", "suppliers", "local", "full"])

    status = commands.add_parser("status", help="Exibe execucoes recentes ou uma execucao")
    status.add_argument("run_id", nargs="?")
    status.add_argument("--limit", type=int, default=25)
    return parser


async def _run(args: argparse.Namespace) -> Any:
    if args.prefect:
        from prefect.deployments import run_deployment

        if args.source:
            name = "sillo-collect-source/manual"
            parameters = {"source_id": args.source, "canary": args.canary, "max_results": args.max_results}
        else:
            name = "sillo-collect-profile/manual"
            parameters = {"profile": args.profile, "canary": args.canary, "max_results": args.max_results}
        flow_run = await run_deployment(name=name, parameters=parameters, timeout=0)
        return {"submitted": True, "deployment": name, "flow_run_id": str(flow_run.id)}

    from pipelines.collection.runner import CollectionRunner

    runner = CollectionRunner()
    if args.source:
        result = await runner.run_source_guarded(
            args.source,
            canary=args.canary,
            max_results=args.max_results,
        )
        return result.model_dump(mode="json")
    return await runner.run_profile(
        args.profile,
        canary=args.canary,
        max_results=args.max_results,
    )


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "list":
            from app.core.settings import get_settings
            from pipelines.collection.registry import SourceRegistry

            registry = SourceRegistry.from_directory(get_settings().source_registry_path)
            sources = registry.list(profile=args.profile, enabled_only=not args.all)
            _print(
                {
                    "sources": [
                        {
                            "source_id": source.source_id,
                            "enabled": source.enabled,
                            "strategy": source.strategy.value,
                            "access": source.access.value,
                            "profiles": sorted(source.profiles),
                        }
                        for source in sources
                    ]
                }
            )
        elif args.command == "status":
            from pipelines.collection.runner import collection_status

            _print(collection_status(args.run_id, limit=args.limit))
        else:
            _print(asyncio.run(_run(args)))
        return 0
    except (ValueError, RuntimeError) as error:
        print(f"erro: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
