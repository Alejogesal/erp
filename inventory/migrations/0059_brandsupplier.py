from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("inventory", "0058_reassign_principal_supplier_to_cheapest"),
    ]

    operations = [
        migrations.CreateModel(
            name="BrandSupplier",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("group", models.CharField(help_text="Marca / grupo", max_length=100, unique=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "supplier",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="principal_brands",
                        to="inventory.supplier",
                    ),
                ),
            ],
            options={
                "ordering": ["group"],
            },
        ),
    ]
