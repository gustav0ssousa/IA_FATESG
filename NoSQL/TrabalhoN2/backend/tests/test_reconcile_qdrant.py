from io import StringIO
from unittest.mock import patch

import pytest
from django.core.management import CommandError, call_command

from apps.rag.vector_store import VectorReconciliationReport

pytestmark = pytest.mark.django_db


class FakeVectorStore:
    def __init__(self, **kwargs):
        pass

    def reconcile(self, expected_chunk_ids, *, apply=False, batch_size=256):
        return VectorReconciliationReport(
            expected_chunks=len(expected_chunk_ids),
            scanned_points=1,
            orphan_point_ids=("orphan",),
            missing_chunk_ids=(),
            deleted_points=1 if apply else 0,
        )


def test_reconcile_command_runs_as_dry_run_by_default() -> None:
    output = StringIO()
    with patch(
        "apps.rag.management.commands.reconcile_qdrant.QdrantVectorStore",
        FakeVectorStore,
    ):
        call_command("reconcile_qdrant", stdout=output)

    assert "DRY-RUN" in output.getvalue()
    assert "removidos=0" in output.getvalue()


def test_reconcile_command_requires_apply_to_reindex() -> None:
    with pytest.raises(CommandError, match="exige --apply"):
        call_command("reconcile_qdrant", reindex_missing=True)
