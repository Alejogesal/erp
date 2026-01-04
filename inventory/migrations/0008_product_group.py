from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("inventory", "0007_add_vat_fields"),
    ]

    operations = [
        migrations.AddField(
            model_name="product",
            name="group",
            field=models.CharField(
                blank=True,
                default="",
                help_text="Marca o grupo",
                max_length=100,
            ),
        ),
    ]

