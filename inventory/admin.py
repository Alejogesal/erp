from django.contrib import admin

from .models import Product, Stock, StockMovement, Warehouse


@admin.register(Warehouse)
class WarehouseAdmin(admin.ModelAdmin):
    list_display = ("name", "type")
    search_fields = ("name", "type")


@admin.register(Product)
class ProductAdmin(admin.ModelAdmin):
    list_display = (
        "sku",
        "name",
        "avg_cost",
        "margin_consumer",
        "margin_barber",
        "margin_distributor",
        "target_margin",
    )
    search_fields = ("sku", "name")


@admin.register(Stock)
class StockAdmin(admin.ModelAdmin):
    list_display = ("product", "warehouse", "quantity")
    list_filter = ("warehouse",)
    search_fields = ("product__sku", "product__name")


@admin.register(StockMovement)
class StockMovementAdmin(admin.ModelAdmin):
    list_display = (
        "product",
        "movement_type",
        "from_warehouse",
        "to_warehouse",
        "quantity",
        "unit_cost",
        "user",
        "created_at",
    )
    list_filter = ("movement_type", "from_warehouse", "to_warehouse", "user")
    search_fields = ("product__sku", "reference")
    readonly_fields = ("created_at",)
