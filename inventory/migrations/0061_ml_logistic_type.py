from django.db import migrations, models


class Migration(migrations.Migration):
    """Canal logístico de MercadoLibre en la venta (Full / Flex / Colecta / ...).

    Todo aditivo con default vacío: las ventas ya cargadas quedan con
    ml_logistic_type="" y se muestran como "Sin dato" hasta que se resincronicen.
    """

    dependencies = [
        ("inventory", "0060_sale_is_cancelled"),
    ]

    operations = [
        migrations.AddField(
            model_name="mercadolibreitem",
            name="has_flex",
            field=models.BooleanField(
                default=False,
                help_text="La publicación tiene Envíos Flex activo (tag self_service_in). Con logistic_type=fulfillment indica convivencia Full/Flex.",
            ),
        ),
        migrations.AddField(
            model_name="sale",
            name="ml_logistic_type",
            field=models.CharField(
                blank=True,
                choices=[
                    ("fulfillment", "Full"),
                    ("self_service", "Flex"),
                    ("cross_docking", "Colecta"),
                    ("xd_drop_off", "Places"),
                    ("drop_off", "Correo"),
                    ("custom", "Envío propio"),
                    ("not_specified", "A convenir"),
                ],
                default="",
                help_text="Tipo logístico de la venta ML (Full/Flex/Colecta/...). Vacío en ventas propias o ventas ML viejas sin resincronizar.",
                max_length=30,
            ),
        ),
        migrations.AddField(
            model_name="sale",
            name="ml_shipment_id",
            field=models.CharField(blank=True, default="", max_length=50),
        ),
        migrations.AddIndex(
            model_name="sale",
            index=models.Index(fields=["ml_logistic_type"], name="sale_ml_logistic_idx"),
        ),
    ]
