from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("inventory", "0017_product_sku_optional"),
    ]

    operations = [
        migrations.CreateModel(
            name="MercadoLibreNotification",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("topic", models.CharField(blank=True, default="", max_length=100)),
                ("resource", models.CharField(blank=True, default="", max_length=255)),
                ("user_id", models.CharField(blank=True, default="", max_length=50)),
                ("application_id", models.CharField(blank=True, default="", max_length=50)),
                ("raw_payload", models.TextField(blank=True, default="")),
                ("received_at", models.DateTimeField(auto_now_add=True)),
            ],
            options={
                "ordering": ["-received_at"],
            },
        ),
    ]
