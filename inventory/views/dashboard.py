"""Dashboard view."""
from decimal import Decimal
from datetime import datetime, time

from django.contrib.auth.decorators import login_required
from django.db.models import Sum
from django.shortcuts import render
from django.utils import timezone

from ..models import (
    Purchase,
    Sale,
    SaleItem,
    TaxExpense,
    Warehouse,
)


@login_required
def dashboard(request):
    start_date = request.GET.get("start_date") or ""
    end_date = request.GET.get("end_date") or ""
    start_dt = None
    end_dt = None
    start_date_obj = None
    end_date_obj = None
    if start_date:
        try:
            parsed = datetime.strptime(start_date, "%Y-%m-%d")
            start_dt = timezone.make_aware(datetime.combine(parsed, time.min))
            start_date_obj = parsed.date()
        except ValueError:
            start_dt = None
    if end_date:
        try:
            parsed = datetime.strptime(end_date, "%Y-%m-%d")
            end_dt = timezone.make_aware(datetime.combine(parsed, time.max))
            end_date_obj = parsed.date()
        except ValueError:
            end_dt = None

    purchase_qs = Purchase.objects.all()
    sale_item_qs = SaleItem.objects.all()
    sales_qs = Sale.objects.select_related("warehouse").prefetch_related("items__product", "items__variant")
    tax_qs = TaxExpense.objects.all()
    if start_dt:
        purchase_qs = purchase_qs.filter(created_at__gte=start_dt)
        sale_item_qs = sale_item_qs.filter(sale__created_at__gte=start_dt)
        sales_qs = sales_qs.filter(created_at__gte=start_dt)
    if start_date_obj:
        tax_qs = tax_qs.filter(paid_at__gte=start_date_obj)
    if end_dt:
        purchase_qs = purchase_qs.filter(created_at__lte=end_dt)
        sale_item_qs = sale_item_qs.filter(sale__created_at__lte=end_dt)
        sales_qs = sales_qs.filter(created_at__lte=end_dt)
    if end_date_obj:
        tax_qs = tax_qs.filter(paid_at__lte=end_date_obj)

    purchase_total = purchase_qs.aggregate(total=Sum("total")).get("total") or Decimal("0.00")
    sale_total = sale_item_qs.aggregate(total=Sum("line_total")).get("total") or Decimal("0.00")
    tax_total = tax_qs.aggregate(total=Sum("amount")).get("total") or Decimal("0.00")

    def _resolve_cost(item) -> Decimal:
        c = item.cost_unit
        if not c or c <= 0:
            c = item.product.last_purchase_cost()
        if not c or c <= 0:
            c = item.product.cost_with_vat()
        return c or Decimal("0.00")

    margin_ml = Decimal("0.00")
    margin_comun = Decimal("0.00")
    for sale in sales_qs:
        cost_total = Decimal("0.00")
        for item in sale.items.all():
            cost_total += item.quantity * _resolve_cost(item)
        if sale.warehouse.type == Warehouse.WarehouseType.MERCADOLIBRE:
            net_total = (sale.total or Decimal("0.00")) - (sale.ml_commission_total or Decimal("0.00")) - (
                sale.ml_tax_total or Decimal("0.00")
            )
            margin_ml += net_total - cost_total
        else:
            margin_comun += (sale.total or Decimal("0.00")) - cost_total

    net_margin = (margin_ml + margin_comun) - tax_total

    ranking_map = {}
    for sale in sales_qs:
        items = list(sale.items.all())
        if not items:
            continue
        items_total = sum((item.line_total or Decimal("0.00")) for item in items)
        if items_total <= 0:
            continue
        if sale.warehouse.type == Warehouse.WarehouseType.MERCADOLIBRE:
            revenue_total = (sale.total or Decimal("0.00")) - (sale.ml_commission_total or Decimal("0.00")) - (
                sale.ml_tax_total or Decimal("0.00")
            )
        else:
            revenue_total = sale.total or Decimal("0.00")
        for item in items:
            line_total = item.line_total or Decimal("0.00")
            revenue_share = (revenue_total * line_total / items_total) if items_total else Decimal("0.00")
            cost_total = item.quantity * _resolve_cost(item)
            profit = (revenue_share - cost_total).quantize(Decimal("0.01"))
            key = (item.product_id, item.variant_id)
            unit_cost = _resolve_cost(item)
            if key not in ranking_map:
                ranking_map[key] = {
                    "product_id": item.product_id,
                    "sku": item.product.sku,
                    "name": item.product.name,
                    "variant": item.variant.name if item.variant else None,
                    "quantity": Decimal("0.00"),
                    "profit": Decimal("0.00"),
                    "zero_cost": unit_cost <= Decimal("0.00"),
                }
            ranking_map[key]["quantity"] += item.quantity
            ranking_map[key]["profit"] += profit
            if unit_cost > Decimal("0.00"):
                ranking_map[key]["zero_cost"] = False

    ranking = sorted(ranking_map.values(), key=lambda item: item["profit"], reverse=True)

    customer_ranking_map: dict = {}
    for sale in sales_qs:
        key = sale.customer_id
        name = sale.customer.name if sale.customer else "Consumidor final"
        cost_total = Decimal("0.00")
        for item in sale.items.all():
            cost_total += item.quantity * _resolve_cost(item)
        if sale.warehouse.type == Warehouse.WarehouseType.MERCADOLIBRE:
            net = (sale.total or Decimal("0.00")) - (sale.ml_commission_total or Decimal("0.00")) - (sale.ml_tax_total or Decimal("0.00"))
            profit = net - cost_total
        else:
            profit = (sale.total or Decimal("0.00")) - cost_total
        if key not in customer_ranking_map:
            customer_ranking_map[key] = {"customer_id": key, "name": name, "profit": Decimal("0.00"), "sale_count": 0}
        customer_ranking_map[key]["profit"] += profit
        customer_ranking_map[key]["sale_count"] += 1

    customer_ranking = sorted(
        (r for r in customer_ranking_map.values() if r["customer_id"] is not None),
        key=lambda x: x["profit"],
        reverse=True,
    )

    context = {
        "purchase_total": purchase_total,
        "sale_total": sale_total,
        "gross_margin": net_margin,
        "gross_margin_pct": (net_margin / sale_total * Decimal("100.00")) if sale_total else None,
        "margin_ml": margin_ml,
        "margin_comun": margin_comun,
        "ranking": ranking,
        "customer_ranking": customer_ranking,
        "start_date": start_date,
        "end_date": end_date,
        "tax_total": tax_total,
    }
    return render(request, "inventory/dashboard.html", context)
