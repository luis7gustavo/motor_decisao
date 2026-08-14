from __future__ import annotations

from abc import ABC, abstractmethod

from pipelines.collection.models import CollectionContext, CollectionResult, SourceConfig


class Collector(ABC):
    """Contrato comum para qualquer fonte estruturada ou web.

    O orquestrador conhece apenas este contrato. Fetch, parse, validacao e
    persistencia permanecem responsabilidades da implementacao concreta e de
    seus colaboradores injetados.
    """

    def __init__(self, source_config: SourceConfig) -> None:
        self.source_config = source_config

    @property
    def source_id(self) -> str:
        return self.source_config.source_id

    @abstractmethod
    async def collect(
        self,
        run_id: str,
        context: CollectionContext,
    ) -> CollectionResult:
        """Executa uma coleta rastreavel e retorna contadores normalizados."""
