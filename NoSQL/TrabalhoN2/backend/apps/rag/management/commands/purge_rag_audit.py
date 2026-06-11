from datetime import timedelta

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from apps.rag.models import RAGQueryRecord


class Command(BaseCommand):
    help = "Remove registros de auditoria RAG anteriores a politica de retencao."

    def add_arguments(self, parser) -> None:
        parser.add_argument("--days", type=int, default=settings.AUDIT_RETENTION_DAYS)
        parser.add_argument(
            "--apply",
            action="store_true",
            help="Executa a remocao. Sem esta flag, realiza somente dry-run.",
        )

    def handle(self, *args, **options) -> None:
        days = options["days"]
        if days < 1:
            raise CommandError("--days deve ser maior que zero.")
        cutoff = timezone.now() - timedelta(days=days)
        records = RAGQueryRecord.objects.filter(created_at__lt=cutoff)
        count = records.count()
        if options["apply"]:
            records.delete()
        mode = "APLICADO" if options["apply"] else "DRY-RUN"
        self.stdout.write(f"{mode}: retencao_dias={days}, registros={count}")
