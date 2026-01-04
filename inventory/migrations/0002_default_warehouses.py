from django.db import migrations


def create_default_warehouses(apps, schema_editor):
    Warehouse = apps.get_model("inventory", "Warehouse")
    defaults = [
        ("MERCADOLIBRE", "MercadoLibre"),
        ("COMUN", "Comun"),
    ]
    for code, name in defaults:
        Warehouse.objects.get_or_create(type=code, defaults={"name": name})


def remove_default_warehouses(apps, schema_editor):
    Warehouse = apps.get_model("inventory", "Warehouse")
    Warehouse.objects.filter(type__in=["MERCADOLIBRE", "COMUN"]).delete()


class Migration(migrations.Migration):
    dependencies = [
        ("inventory", "0001_initial"),
    ]

    operations = [
        migrations.RunPython(create_default_warehouses, remove_default_warehouses),
    ]
