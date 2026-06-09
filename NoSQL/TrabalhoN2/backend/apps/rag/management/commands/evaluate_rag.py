from pathlib import Path

from django.core.management.base import BaseCommand

from apps.rag.evaluation import RAGEvaluator, load_dataset, write_report
from apps.rag.services import build_rag_query_service, build_services


class Command(BaseCommand):
    help = "Avalia retrieval e, opcionalmente, geracao do sistema RAG."

    def add_arguments(self, parser) -> None:
        parser.add_argument(
            "--dataset",
            default="data/evaluation/rag_cases.json",
        )
        parser.add_argument(
            "--output",
            default="outputs/evaluation",
        )
        parser.add_argument("--top-k", type=int, default=5)
        parser.add_argument("--with-generation", action="store_true")

    def handle(self, *args, **options) -> None:
        _, search_service = build_services()
        query_service = build_rag_query_service() if options["with_generation"] else None
        result = RAGEvaluator(search_service, query_service).evaluate(
            load_dataset(Path(options["dataset"])),
            options["top_k"],
        )
        write_report(result, Path(options["output"]))
        self.stdout.write(self.style.SUCCESS(f"Avaliacao salva em {options['output']}"))
