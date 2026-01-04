from django.db import migrations, models
import django.utils.timezone


class Migration(migrations.Migration):
    dependencies = [
        ("inventory", "0013_merge_20251226_0343"),
    ]

    operations = [
        migrations.CreateModel(
            name="TaxExpense",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("description", models.CharField(max_length=255)),
                ("amount", models.DecimalField(decimal_places=2, default=0, max_digits=12)),
                ("paid_at", models.DateField(default=django.utils.timezone.now)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
            ],
            options={
                "ordering": ["-paid_at", "-id"],
            },
        ),
    ]
