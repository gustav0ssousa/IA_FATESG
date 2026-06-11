import django.db.models.deletion
from django.conf import settings
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ("rag", "0002_ragqueryrecord_ragquerysource"),
    ]

    operations = [
        migrations.AlterField(
            model_name="ragqueryrecord",
            name="question",
            field=models.CharField(blank=True, max_length=4000),
        ),
        migrations.AddField(
            model_name="ragqueryrecord",
            name="authentication_method",
            field=models.CharField(blank=True, db_index=True, max_length=30),
        ),
        migrations.AddField(
            model_name="ragqueryrecord",
            name="filters",
            field=models.JSONField(blank=True, default=dict),
        ),
        migrations.AddField(
            model_name="ragqueryrecord",
            name="question_hash",
            field=models.CharField(blank=True, db_index=True, max_length=64),
        ),
        migrations.AddField(
            model_name="ragqueryrecord",
            name="user",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="rag_queries",
                to=settings.AUTH_USER_MODEL,
            ),
        ),
        migrations.AddField(
            model_name="ragquerysource",
            name="chunk_id",
            field=models.CharField(blank=True, db_index=True, max_length=64),
        ),
        migrations.AddField(
            model_name="ragquerysource",
            name="metadata",
            field=models.JSONField(blank=True, default=dict),
        ),
        migrations.AddField(
            model_name="ragquerysource",
            name="page_number",
            field=models.PositiveIntegerField(blank=True, null=True),
        ),
    ]
