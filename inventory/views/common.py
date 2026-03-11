"""Shared helper functions used by multiple view modules."""
from decimal import Decimal, ROUND_HALF_UP
import re
import unicodedata
import json

from django.db.models import DecimalField, Value, Subquery, OuterRef
from django.db.models.functions import Coalesce

from ..models import (
    Customer,
    CustomerGroupDiscount,
    CustomerProductDiscount,
    CustomerProductPrice,
    Product,
    StockMovement,
    SupplierProduct,
)


def _products_with_last_cost_queryset():
    supplier_cost = (
        SupplierProduct.objects.filter(product=OuterRef("pk"))
        .order_by("-last_purchase_at", "-id")
        .values("last_cost")[:1]
    )
    movement_cost = (
        StockMovement.objects.filter(product=OuterRef("pk"), movement_type=StockMovement.MovementType.ENTRY)
        .order_by("-created_at", "-id")
        .values("unit_cost")[:1]
    )
    return Product.objects.annotate(
        last_purchase_cost_value=Coalesce(
            Subquery(supplier_cost, output_field=DecimalField(max_digits=12, decimal_places=2)),
            Subquery(movement_cost, output_field=DecimalField(max_digits=12, decimal_places=2)),
            Value(Decimal("0.00")),
            output_field=DecimalField(max_digits=12, decimal_places=2),
        )
    )


def _product_label_with_last_cost(obj: Product) -> str:
    if getattr(obj, "is_kit", False):
        last_cost = obj.cost_with_vat()
    else:
        last_cost = getattr(obj, "last_purchase_cost_value", None)
        if last_cost is None:
            last_cost = obj.last_purchase_cost()
    return f"{obj.sku or 'Sin SKU'} - {obj.name} (último costo: {last_cost:.2f})"


def _resolve_sale_item_pricing(
    *,
    product: Product,
    audience: str,
    customer: Customer | None,
    requested_discount: Decimal | None = None,
) -> tuple[Decimal, Decimal, Decimal | None]:
    base_price = {
        Customer.Audience.CONSUMER: product.consumer_price,
        Customer.Audience.BARBER: product.barber_price,
        Customer.Audience.DISTRIBUTOR: product.distributor_price,
    }.get(audience, product.consumer_price)
    if customer:
        custom_price = CustomerProductPrice.objects.filter(customer=customer, product=product).first()
        if custom_price:
            base_price = custom_price.unit_price if custom_price.unit_price is not None else base_price
            discount = requested_discount if requested_discount is not None else Decimal("0.00")
            return base_price, discount, custom_price.unit_cost
    if requested_discount is not None:
        return base_price, requested_discount, None
    discount = Decimal("0.00")
    if customer:
        discount_obj = CustomerProductDiscount.objects.filter(customer=customer, product=product).first()
        if discount_obj:
            discount = discount_obj.discount_percent
        else:
            group_key = (product.group or "").strip()
            if group_key:
                group_discount = CustomerGroupDiscount.objects.filter(customer=customer, group__iexact=group_key).first()
                if group_discount:
                    discount = group_discount.discount_percent
    return base_price, discount, None


def _apply_product_queryset_to_formset(formset, products_qs):
    for form in formset.forms:
        if "product" in form.fields:
            form.fields["product"].queryset = products_qs
    if hasattr(formset, "empty_form") and "product" in formset.empty_form.fields:
        formset.empty_form.fields["product"].queryset = Product.objects.none()


def _extract_product_ids_from_payload(raw_payload: str | None) -> set[int]:
    if not raw_payload:
        return set()
    try:
        items_raw = json.loads(raw_payload)
    except Exception:
        return set()
    if not isinstance(items_raw, list):
        return set()
    ids: set[int] = set()
    for entry in items_raw:
        if not isinstance(entry, dict):
            continue
        product_id = entry.get("product_id")
        if not product_id:
            continue
        try:
            ids.add(int(product_id))
        except Exception:
            continue
    return ids


def _shipping_cost_per_unit(shipping_cost: Decimal, total_units: Decimal) -> Decimal:
    if shipping_cost <= 0 or total_units <= 0:
        return Decimal("0.00")
    return (shipping_cost / total_units).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)


def _normalize_lookup_text(value: str) -> str:
    cleaned = unicodedata.normalize("NFKD", (value or "").strip().lower())
    cleaned = "".join(ch for ch in cleaned if not unicodedata.combining(ch))
    cleaned = re.sub(r"[^a-z0-9]+", " ", cleaned)
    return re.sub(r"\s+", " ", cleaned).strip()
