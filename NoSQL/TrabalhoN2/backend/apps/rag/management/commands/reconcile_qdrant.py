from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from qdrant_client import QdrantClient

from apps.documents.models import Document, DocumentChunk, DocumentStatus
from apps.rag.services import build_services
from apps.rag.vector_store import QdrantVectorStore, VectorReconciliationReport


class Command(BaseCommand):
    help = "Reconcilia vetores do Qdrant usando chunks indexados no PostgreSQL."

    def add_arguments(self, parser) -> None:
        parser.add_argument(
            "--apply",
            action="store_true",
            help="Remove pontos orfaos. Sem esta flag, executa somente dry-run.",
        )
        parser.add_argument(
            "--reindex-missing",
            action="store_true",
            help="Reindexa documentos com chunks ausentes; exige --apply.",
        )
        parser.add_argument("--batch-size", type=int, default=256)

    def handle(self, *args, **options) -> None:
        if options["reindex_missing"] and not options["apply"]:
            raise CommandError("--reindex-missing exige --apply.")

        expected_ids = {
            str(chunk_id)
            for chunk_id in DocumentChunk.objects.filter(
                document__status=DocumentStatus.INDEXED
            ).values_list("id", flat=True)
        }
        vector_store = QdrantVectorStore(
            client=QdrantClient(url=settings.QDRANT_URL),
            collection_name=settings.QDRANT_COLLECTION,
            vector_size=settings.EMBEDDING_DIMENSION,
        )
        report = vector_store.reconcile(
            expected_ids,
            apply=options["apply"],
            batch_size=options["batch_size"],
        )
        self._write_report(report, applied=options["apply"])

        if options["reindex_missing"] and report.missing_chunk_ids:
            document_ids = DocumentChunk.objects.filter(
                id__in=report.missing_chunk_ids
            ).values_list("document_id", flat=True).distinct()
            indexing_service, _ = build_services()
            for document in Document.objects.filter(id__in=document_ids):
                indexing_service.index(document)
                self.stdout.write(f"Documento reindexado: {document.id}")

            final_report = vector_store.reconcile(
                expected_ids,
                batch_size=options["batch_size"],
            )
            self.stdout.write("Estado apos reindexacao:")
            self._write_report(final_report, applied=False)

    def _write_report(
        self,
        report: VectorReconciliationReport,
        *,
        applied: bool,
    ) -> None:
        mode = "APLICADO" if applied else "DRY-RUN"
        self.stdout.write(
            f"{mode}: esperados={report.expected_chunks}, "
            f"escaneados={report.scanned_points}, "
            f"orfaos={len(report.orphan_point_ids)}, "
            f"ausentes={len(report.missing_chunk_ids)}, "
            f"removidos={report.deleted_points}"
        )
        if report.consistent:
            self.stdout.write(self.style.SUCCESS("PostgreSQL e Qdrant consistentes."))
        elif not applied:
            self.stdout.write(
                self.style.WARNING(
                    "Divergencias encontradas. Revise e execute novamente com --apply."
                )
            )
