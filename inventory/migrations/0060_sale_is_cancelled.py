from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("inventory", "0059_brandsupplier"),
    ]

    operations = [
        migrations.AddField(
            model_name="sale",
            name="is_cancelled",
            field=models.BooleanField(
                default=False,
                help_text="Venta cancelada/expirada: queda en el historial pero no cuenta en ventas ni ganancias.",
            ),
        ),
        migrations.AddField(
            model_name="sale",
            name="cancelled_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
    ]
