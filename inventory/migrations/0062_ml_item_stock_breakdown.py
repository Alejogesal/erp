from django.db import migrations, models


class Migration(migrations.Migration):
    """Desglose del stock de una publicación: en Full vs. en el depósito propio.

    Un solo número mezclaba las dos ubicaciones y hacía parecer desfasajes
    diferencias que son normales (el stock en Full no tiene por qué coincidir
    con el del ERP). Se llenan en el próximo sync.
    """

    dependencies = [
        ("inventory", "0061_ml_logistic_type"),
    ]

    operations = [
        migrations.AddField(
            model_name="mercadolibreitem",
            name="full_quantity",
            field=models.IntegerField(
                default=0,
                help_text="Unidades en el depósito de MercadoLibre (meli_facility). Solo aplica a publicaciones Full.",
            ),
        ),
        migrations.AddField(
            model_name="mercadolibreitem",
            name="flex_quantity",
            field=models.IntegerField(
                default=0,
                help_text="Unidades publicadas del depósito propio (selling_address). Es la que debe coincidir con el stock COMUN.",
            ),
        ),
    ]
