from decimal import Decimal, ROUND_HALF_UP

from django.db import transaction
from django.db.models import Sum

from django.utils import timezone

from .models import Product, Stock, StockMovement, SupplierProduct, Warehouse


class StockError(Exception):
    """Base error for stock operations."""


class NegativeStockError(StockError):
    """Raised when an operation would result in negative stock."""


class InvalidMovementError(StockError):
    """Raised when a movement request is invalid."""


def _to_decimal(value: Decimal | float | int | str) -> Decimal:
    dec = value if isinstance(value, Decimal) else Decimal(str(value))
    return dec.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _get_stock_for_update(product: Product, warehouse: Warehouse) -> Stock:
    stock, _ = Stock.objects.select_for_update().get_or_create(
        product=product, warehouse=warehouse, defaults={"quantity": Decimal("0.00")}
    )
    return stock


def _total_stock_quantity(product: Product) -> Decimal:
    total = (
        Stock.objects.select_for_update()
        .filter(product=product)
        .aggregate(total=Sum("quantity"))
        .get("total")
    )
    return total if total is not None else Decimal("0.00")


def _weighted_average(current_avg: Decimal, current_qty: Decimal, unit_cost: Decimal, quantity: Decimal) -> Decimal:
    if quantity <= 0:
        return current_avg or Decimal("0.00")
    total_cost = (current_avg or Decimal("0.00")) * current_qty + unit_cost * quantity
    new_total_qty = current_qty + quantity
    if new_total_qty <= 0:
        return Decimal("0.00")
    return (total_cost / new_total_qty).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


@transaction.atomic
def register_entry(
    product: Product,
    warehouse: Warehouse,
    quantity: Decimal,
    unit_cost: Decimal,
    user,
    reference: str = "",
    vat_percent: Decimal | float | int | str | None = None,
    supplier=None,
    purchase=None,
) -> StockMovement:
    qty = _to_decimal(quantity)
    cost_with_vat = _to_decimal(unit_cost)
    vat = _to_decimal(vat_percent) if vat_percent is not None else Decimal("0.00")
    if qty <= 0:
        raise InvalidMovementError("Entry quantity must be positive")

    if vat > 0:
        vat_factor = Decimal("1.00") + (vat / Decimal("100.00"))
        cost_base = (cost_with_vat / vat_factor).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    else:
        cost_base = cost_with_vat

    stock = _get_stock_for_update(product, warehouse)
    current_total = _total_stock_quantity(product)

    # Use last purchase cost as the product cost (not weighted average).
    product.avg_cost = cost_base
    update_fields = ["avg_cost"]
    if vat_percent is not None:
        product.vat_percent = vat
        update_fields.append("vat_percent")
    product.save(update_fields=update_fields)

    stock.quantity = (stock.quantity + qty).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    stock.save(update_fields=["quantity"])

    movement = StockMovement.objects.create(
        product=product,
        purchase=purchase,
        movement_type=StockMovement.MovementType.ENTRY,
        to_warehouse=warehouse,
        quantity=qty,
        unit_cost=cost_with_vat,
        vat_percent=vat,
        user=user,
        reference=reference or "",
    )
    if supplier:
        supplier_product, _ = SupplierProduct.objects.get_or_create(
            supplier=supplier, product=product, defaults={"last_cost": cost_with_vat, "last_purchase_at": timezone.now()}
        )
        supplier_product.last_cost = cost_with_vat
        supplier_product.last_purchase_at = timezone.now()
        supplier_product.save(update_fields=["last_cost", "last_purchase_at"])
        if product.default_supplier_id is None:
            product.default_supplier = supplier
            product.save(update_fields=["default_supplier"])
    return movement


@transaction.atomic
def register_exit(
    product: Product,
    warehouse: Warehouse,
    quantity: Decimal,
    user,
    reference: str = "",
    allow_negative: bool = False,
    sale_price: Decimal | None = None,
    vat_percent: Decimal | float | int | str | None = None,
    sale=None,
) -> StockMovement:
    qty = _to_decimal(quantity)
    vat = _to_decimal(vat_percent) if vat_percent is not None else Decimal("0.00")
    if qty <= 0:
        raise InvalidMovementError("Exit quantity must be positive")

    stock = _get_stock_for_update(product, warehouse)
    if not allow_negative and stock.quantity - qty < 0:
        raise NegativeStockError("Stock cannot go negative")

    stock.quantity = (stock.quantity - qty).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    stock.save(update_fields=["quantity"])

    return StockMovement.objects.create(
        product=product,
        sale=sale,
        movement_type=StockMovement.MovementType.EXIT,
        from_warehouse=warehouse,
        quantity=qty,
        unit_cost=product.avg_cost,
        sale_price=_to_decimal(sale_price) if sale_price is not None else Decimal("0.00"),
        vat_percent=vat,
        user=user,
        reference=reference or "",
    )


@transaction.atomic
def register_transfer(
    product: Product,
    from_warehouse: Warehouse,
    to_warehouse: Warehouse,
    quantity: Decimal,
    user,
    reference: str = "",
    allow_negative: bool = False,
) -> StockMovement:
    if from_warehouse == to_warehouse:
        raise InvalidMovementError("Transfer must involve different warehouses")

    qty = _to_decimal(quantity)
    if qty <= 0:
        raise InvalidMovementError("Transfer quantity must be positive")

    source_stock = _get_stock_for_update(product, from_warehouse)
    if not allow_negative and source_stock.quantity - qty < 0:
        raise NegativeStockError("Stock cannot go negative")
    source_stock.quantity = (source_stock.quantity - qty).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    source_stock.save(update_fields=["quantity"])

    return StockMovement.objects.create(
        product=product,
        movement_type=StockMovement.MovementType.TRANSFER,
        from_warehouse=from_warehouse,
        to_warehouse=to_warehouse,
        quantity=qty,
        unit_cost=product.avg_cost,
        user=user,
        reference=reference or "",
    )


@transaction.atomic
def register_adjustment(
    product: Product,
    warehouse: Warehouse,
    quantity: Decimal,
    user,
    unit_cost: Decimal | None = None,
    reference: str = "",
    allow_negative: bool = False,
) -> StockMovement:
    qty = _to_decimal(quantity)
    if qty == 0:
        raise InvalidMovementError("Adjustment quantity cannot be zero")

    if qty > 0:
        cost = _to_decimal(unit_cost if unit_cost is not None else product.avg_cost)
        stock = _get_stock_for_update(product, warehouse)
        current_total = _total_stock_quantity(product)
        new_avg = _weighted_average(product.avg_cost, current_total, cost, qty)
        product.avg_cost = new_avg
        product.save(update_fields=["avg_cost"])
        stock.quantity = (stock.quantity + qty).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        stock.save(update_fields=["quantity"])
        movement_kwargs = {"to_warehouse": warehouse, "unit_cost": cost}
    else:
        stock = _get_stock_for_update(product, warehouse)
        if not allow_negative and stock.quantity + qty < 0:
            raise NegativeStockError("Stock cannot go negative")
        stock.quantity = (stock.quantity + qty).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        stock.save(update_fields=["quantity"])
        movement_kwargs = {"from_warehouse": warehouse, "unit_cost": product.avg_cost}

    return StockMovement.objects.create(
        product=product,
        movement_type=StockMovement.MovementType.ADJUSTMENT,
        quantity=abs(qty),
        user=user,
        reference=reference or "",
        **movement_kwargs,
    )
