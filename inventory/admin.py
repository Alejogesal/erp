from django.contrib import admin

from .models import (
    AuditLog,
    Company,
    MercadoLibreConnection,
    MercadoLibreItem,
    Product,
    Stock,
    StockMovement,
    Warehouse,
)


@admin.register(Company)
class CompanyAdmin(admin.ModelAdmin):
    list_display = ("name", "cuit", "fiscal_condition", "is_active")
    search_fields = ("name", "cuit")


@admin.register(MercadoLibreConnection)
class MercadoLibreConnectionAdmin(admin.ModelAdmin):
    list_display = ("company", "nickname", "ml_user_id", "user", "last_sync_at")
    list_filter = ("company",)
    readonly_fields = ("access_token", "refresh_token")


@admin.register(MercadoLibreItem)
class MercadoLibreItemAdmin(admin.ModelAdmin):
    list_display = ("item_id", "title", "company", "status", "logistic_type", "available_quantity")
    list_filter = ("company", "status", "logistic_type")
    search_fields = ("item_id", "title")


@admin.register(Warehouse)
class WarehouseAdmin(admin.ModelAdmin):
    list_display = ("name", "type", "company")
    list_filter = ("company",)
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


@admin.register(AuditLog)
class AuditLogAdmin(admin.ModelAdmin):
    list_display = ("timestamp", "action", "model_name", "object_repr", "user")
    list_filter = ("action", "model_name")
    search_fields = ("object_repr", "model_name")
    readonly_fields = ("timestamp", "action", "model_name", "object_id", "object_repr", "changes", "user")

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False


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
