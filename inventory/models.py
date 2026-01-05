from decimal import Decimal

from django.conf import settings
from django.db import models
from django.utils import timezone


class Warehouse(models.Model):
    class WarehouseType(models.TextChoices):
        MERCADOLIBRE = "MERCADOLIBRE", "MercadoLibre"
        COMUN = "COMUN", "Comun"

    name = models.CharField(max_length=100)
    type = models.CharField(max_length=20, choices=WarehouseType.choices, unique=True)

    class Meta:
        ordering = ["name"]

    def __str__(self) -> str:
        return f"{self.name} ({self.type})"


class Product(models.Model):
    sku = models.CharField(max_length=64, unique=True, blank=True, null=True)
    name = models.CharField(max_length=255)
    group = models.CharField(max_length=100, default="", blank=True, help_text="Marca o grupo")
    avg_cost = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))
    price_consumer = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))
    price_barber = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))
    price_distributor = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))
    vat_percent = models.DecimalField(
        max_digits=5, decimal_places=2, default=Decimal("0.00"), blank=True, help_text="IVA %"
    )
    margin_consumer = models.DecimalField(
        max_digits=5, decimal_places=2, default=Decimal("25.00"), help_text="Margen % consumidor final"
    )
    margin_barber = models.DecimalField(
        max_digits=5, decimal_places=2, default=Decimal("20.00"), help_text="Margen % peluquerías/barberías"
    )
    margin_distributor = models.DecimalField(
        max_digits=5, decimal_places=2, default=Decimal("15.00"), help_text="Margen % distribuidores"
    )
    ml_commission_percent = models.DecimalField(
        max_digits=5,
        decimal_places=2,
        null=True,
        blank=True,
        help_text="Comisión MercadoLibre %",
    )
    target_margin = models.DecimalField(
        max_digits=5,
        decimal_places=2,
        default=Decimal("0.00"),
        help_text="Desired margin percentage (e.g. 25.00 for 25%)",
    )
    default_supplier = models.ForeignKey(
        "Supplier",
        on_delete=models.SET_NULL,
        related_name="default_products",
        null=True,
        blank=True,
        help_text="Proveedor preferido",
    )
    suppliers = models.ManyToManyField(
        "Supplier",
        through="SupplierProduct",
        related_name="products",
        blank=True,
    )

    class Meta:
        ordering = ["sku"]

    def __str__(self) -> str:
        sku = self.sku or "Sin SKU"
        return f"{sku} - {self.name}"

    @property
    def suggested_price(self) -> Decimal:
        multiplier = Decimal("1.00") + (self.target_margin or Decimal("0.00")) / Decimal("100.00")
        return (self.avg_cost or Decimal("0.00")) * multiplier

    def _price_with_margin(self, margin: Decimal) -> Decimal:
        multiplier = Decimal("1.00") + (margin or Decimal("0.00")) / Decimal("100.00")
        return (self.avg_cost or Decimal("0.00")) * multiplier

    @property
    def consumer_price(self) -> Decimal:
        return self._price_with_margin(self.margin_consumer)

    @property
    def barber_price(self) -> Decimal:
        return self._price_with_margin(self.margin_barber)

    @property
    def distributor_price(self) -> Decimal:
        return self._price_with_margin(self.margin_distributor)

    def last_purchase_cost(self) -> Decimal:
        supplier_product = self.supplier_products.order_by("-last_purchase_at").first()
        if supplier_product and supplier_product.last_cost is not None:
            return supplier_product.last_cost
        last_entry = self.movements.filter(movement_type=StockMovement.MovementType.ENTRY).order_by("-created_at").first()
        if last_entry:
            return last_entry.unit_cost
        return Decimal("0.00")

    def last_purchase_cost_display(self) -> str:
        return f"{self.last_purchase_cost():.2f}"


class Customer(models.Model):
    class Audience(models.TextChoices):
        CONSUMER = "CONSUMER", "Consumidor final"
        BARBER = "BARBER", "Peluquerías/Barberías"
        DISTRIBUTOR = "DISTRIBUTOR", "Distribuidor"

    name = models.CharField(max_length=255)
    email = models.EmailField(blank=True)
    audience = models.CharField(max_length=20, choices=Audience.choices, default=Audience.CONSUMER)

    class Meta:
        ordering = ["name"]

    def __str__(self) -> str:
        return self.name


class CustomerProductDiscount(models.Model):
    customer = models.ForeignKey(Customer, on_delete=models.CASCADE, related_name="discounts")
    product = models.ForeignKey(Product, on_delete=models.CASCADE, related_name="customer_discounts")
    discount_percent = models.DecimalField(max_digits=5, decimal_places=2, default=Decimal("0.00"))

    class Meta:
        unique_together = ("customer", "product")
        ordering = ["customer__name", "product__sku"]

    def __str__(self) -> str:
        return f"{self.customer} - {self.product.sku} ({self.discount_percent}%)"


class CustomerGroupDiscount(models.Model):
    customer = models.ForeignKey(Customer, on_delete=models.CASCADE, related_name="group_discounts")
    group = models.CharField(max_length=100)
    discount_percent = models.DecimalField(max_digits=5, decimal_places=2, default=Decimal("0.00"))

    class Meta:
        unique_together = ("customer", "group")
        ordering = ["customer__name", "group"]

    def __str__(self) -> str:
        return f"{self.customer} - {self.group} ({self.discount_percent}%)"


class Stock(models.Model):
    product = models.ForeignKey(Product, on_delete=models.CASCADE, related_name="stocks")
    warehouse = models.ForeignKey(Warehouse, on_delete=models.CASCADE, related_name="stocks")
    quantity = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))

    class Meta:
        unique_together = ("product", "warehouse")
        ordering = ["product__sku", "warehouse__name"]

    def __str__(self) -> str:
        return f"{self.product.sku} @ {self.warehouse.type}: {self.quantity}"


class StockMovement(models.Model):
    class MovementType(models.TextChoices):
        ENTRY = "ENTRY", "Entrada"
        EXIT = "EXIT", "Salida"
        TRANSFER = "TRANSFER", "Transferencia"
        ADJUSTMENT = "ADJUSTMENT", "Ajuste"

    product = models.ForeignKey(Product, on_delete=models.CASCADE, related_name="movements")
    sale = models.ForeignKey("Sale", on_delete=models.SET_NULL, null=True, blank=True, related_name="movements")
    purchase = models.ForeignKey("Purchase", on_delete=models.SET_NULL, null=True, blank=True, related_name="movements")
    movement_type = models.CharField(max_length=20, choices=MovementType.choices)
    from_warehouse = models.ForeignKey(
        Warehouse, on_delete=models.CASCADE, related_name="outgoing_movements", null=True, blank=True
    )
    to_warehouse = models.ForeignKey(
        Warehouse, on_delete=models.CASCADE, related_name="incoming_movements", null=True, blank=True
    )
    quantity = models.DecimalField(max_digits=12, decimal_places=2)
    unit_cost = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))
    sale_price = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))
    sale_net = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))
    ml_commission_percent = models.DecimalField(max_digits=5, decimal_places=2, default=Decimal("0.00"))
    retention_percent = models.DecimalField(max_digits=5, decimal_places=2, default=Decimal("0.00"))
    profit = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))
    vat_percent = models.DecimalField(
        max_digits=5, decimal_places=2, default=Decimal("0.00"), blank=True, help_text="IVA % aplicado"
    )
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT, related_name="stock_movements")
    reference = models.CharField(max_length=255, blank=True, default="")
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-created_at", "-id"]

    def __str__(self) -> str:
        return f"{self.movement_type} - {self.product.sku} ({self.quantity})"


class Supplier(models.Model):
    name = models.CharField(max_length=255, unique=True)
    phone = models.CharField(max_length=50, blank=True, default="")
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["name"]

    def __str__(self) -> str:
        return self.name


class SupplierProduct(models.Model):
    supplier = models.ForeignKey(Supplier, on_delete=models.CASCADE, related_name="supplier_products")
    product = models.ForeignKey(Product, on_delete=models.CASCADE, related_name="supplier_products")
    last_cost = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))
    last_purchase_at = models.DateTimeField(null=True, blank=True, default=None)

    class Meta:
        unique_together = ("supplier", "product")
        ordering = ["supplier__name", "product__sku"]

    def __str__(self) -> str:
        return f"{self.supplier} - {self.product.sku}"


class MercadoLibreNotification(models.Model):
    topic = models.CharField(max_length=100, blank=True, default="")
    resource = models.CharField(max_length=255, blank=True, default="")
    ml_user_id = models.CharField(max_length=50, blank=True, default="")
    application_id = models.CharField(max_length=50, blank=True, default="")
    raw_payload = models.TextField(blank=True, default="")
    received_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-received_at"]

    def __str__(self) -> str:
        return f"{self.topic or 'notification'} @ {self.received_at:%Y-%m-%d %H:%M:%S}"


class MercadoLibreConnection(models.Model):
    user = models.OneToOneField(settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name="ml_connection")
    access_token = models.TextField(blank=True, default="")
    refresh_token = models.TextField(blank=True, default="")
    expires_at = models.DateTimeField(null=True, blank=True)
    ml_user_id = models.CharField(max_length=50, blank=True, default="")
    nickname = models.CharField(max_length=100, blank=True, default="")
    last_sync_at = models.DateTimeField(null=True, blank=True)
    last_metrics = models.TextField(blank=True, default="")
    last_metrics_at = models.DateTimeField(null=True, blank=True)
    connected_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-connected_at"]

    def __str__(self) -> str:
        return self.nickname or f"ML {self.ml_user_id or 'sin usuario'}"


class MercadoLibreItem(models.Model):
    item_id = models.CharField(max_length=50, unique=True)
    title = models.CharField(max_length=255, blank=True, default="")
    status = models.CharField(max_length=50, blank=True, default="")
    permalink = models.URLField(blank=True, default="")
    available_quantity = models.IntegerField(default=0)
    product = models.ForeignKey(Product, null=True, blank=True, on_delete=models.SET_NULL, related_name="ml_items")
    matched_name = models.CharField(max_length=255, blank=True, default="")
    last_sold_at = models.DateTimeField(null=True, blank=True)
    units_sold_30d = models.IntegerField(default=0)
    last_synced = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-last_synced"]

    def __str__(self) -> str:
        return f"{self.item_id} - {self.title}"


class Purchase(models.Model):
    supplier = models.ForeignKey(Supplier, on_delete=models.SET_NULL, null=True, blank=True, related_name="purchases")
    warehouse = models.ForeignKey(Warehouse, on_delete=models.PROTECT, related_name="purchases")
    total = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))
    reference = models.CharField(max_length=255, blank=True, default="")
    created_at = models.DateTimeField(auto_now_add=True)
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT, related_name="purchases")

    class Meta:
        ordering = ["-created_at", "-id"]

    def __str__(self) -> str:
        return f"Purchase #{self.pk}"

    @property
    def invoice_number(self) -> str:
        return f"00001-{self.pk:08d}" if self.pk else ""


class PurchaseItem(models.Model):
    purchase = models.ForeignKey(Purchase, on_delete=models.CASCADE, related_name="items")
    product = models.ForeignKey(Product, on_delete=models.PROTECT, related_name="purchase_items")
    quantity = models.DecimalField(max_digits=12, decimal_places=2)
    unit_cost = models.DecimalField(max_digits=12, decimal_places=2)
    vat_percent = models.DecimalField(max_digits=5, decimal_places=2, default=Decimal("0.00"))

    class Meta:
        ordering = ["purchase__id", "id"]

    def __str__(self) -> str:
        return f"{self.product.sku} x {self.quantity}"


class Sale(models.Model):
    customer = models.ForeignKey(Customer, on_delete=models.SET_NULL, null=True, blank=True, related_name="sales")
    warehouse = models.ForeignKey(Warehouse, on_delete=models.PROTECT, related_name="sales")
    audience = models.CharField(max_length=20, choices=Customer.Audience.choices, default=Customer.Audience.CONSUMER)
    total = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))
    discount_total = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))
    reference = models.CharField(max_length=255, blank=True, default="")
    ml_order_id = models.CharField(max_length=50, blank=True, default="")
    ml_commission_total = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))
    ml_tax_total = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))
    created_at = models.DateTimeField(auto_now_add=True)
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT, related_name="sales")

    class Meta:
        ordering = ["-created_at", "-id"]

    def __str__(self) -> str:
        return f"Sale #{self.pk}"

    @property
    def invoice_number(self) -> str:
        return f"00001-{self.pk:08d}" if self.pk else ""


class SaleItem(models.Model):
    sale = models.ForeignKey(Sale, on_delete=models.CASCADE, related_name="items")
    product = models.ForeignKey(Product, on_delete=models.PROTECT, related_name="sale_items")
    quantity = models.DecimalField(max_digits=12, decimal_places=2)
    unit_price = models.DecimalField(max_digits=12, decimal_places=2)  # price before discount
    discount_percent = models.DecimalField(max_digits=5, decimal_places=2, default=Decimal("0.00"))
    final_unit_price = models.DecimalField(max_digits=12, decimal_places=2)
    line_total = models.DecimalField(max_digits=12, decimal_places=2)
    vat_percent = models.DecimalField(max_digits=5, decimal_places=2, default=Decimal("0.00"))

    class Meta:
        ordering = ["sale__id", "id"]

    def __str__(self) -> str:
        return f"{self.product.sku} x {self.quantity}"


class TaxExpense(models.Model):
    description = models.CharField(max_length=255)
    amount = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))
    paid_at = models.DateField(default=timezone.now)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-paid_at", "-id"]

    def __str__(self) -> str:
        return f"{self.description} - {self.amount}"
