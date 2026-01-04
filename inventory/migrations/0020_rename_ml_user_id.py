from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("inventory", "0019_mercadolibre_connection_item"),
    ]

    operations = [
        migrations.RenameField(
            model_name="mercadolibreconnection",
            old_name="user_id",
            new_name="ml_user_id",
        ),
    ]
