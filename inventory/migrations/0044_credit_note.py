from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion
import django.utils.timezone


class Migration(migrations.Migration):

    dependencies = [
        ("inventory", "0043_avg_cost_include_vat"),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name="CreditNote",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("date", models.DateField(default=django.utils.timezone.localdate)),
                ("notes", models.CharField(blank=True, default="", max_length=255)),
                ("total", models.DecimalField(decimal_places=2, default="0.00", max_digits=12)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("customer", models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, related_name="credit_notes", to="inventory.customer")),
                ("sale", models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name="credit_notes", to="inventory.sale")),
                ("user", models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, related_name="credit_notes", to=settings.AUTH_USER_MODEL)),
            ],
            options={"ordering": ["-date", "-id"]},
        ),
        migrations.CreateModel(
            name="CreditNoteItem",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("quantity", models.DecimalField(decimal_places=2, max_digits=12)),
                ("unit_price", models.DecimalField(decimal_places=2, max_digits=12)),
                ("line_total", models.DecimalField(decimal_places=2, max_digits=12)),
                ("credit_note", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="items", to="inventory.creditnote")),
                ("product", models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, related_name="credit_note_items", to="inventory.product")),
            ],
            options={"ordering": ["credit_note__id", "id"]},
        ),
    ]
