"""Dashboard view."""
import json
from decimal import Decimal
from datetime import datetime, time, timedelta

from django.contrib.auth.decorators import login_required
from django.db.models import Sum, Count
from django.db.models.functions import TruncDate
from django.shortcuts import render
from django.utils import timezone

from ..models import (
    Product,
    Purchase,
    Sale,
    SaleItem,
    Stock,
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
            customer_ranking_map[key] = {"customer_id": key, "name": name, "profit": Decimal("0.00"), "total": Decimal("0.00"), "sale_count": 0}
        customer_ranking_map[key]["profit"] += profit
        customer_ranking_map[key]["total"] += sale.total or Decimal("0.00")
        customer_ranking_map[key]["sale_count"] += 1

    customer_ranking = sorted(
        (r for r in customer_ranking_map.values() if r["customer_id"] is not None),
        key=lambda x: x["profit"],
        reverse=True,
    )

    # Low stock alerts: products with min_stock set and COMUN stock below threshold
    comun_wh = Warehouse.objects.filter(type=Warehouse.WarehouseType.COMUN).first()
    low_stock_alerts = []
    if comun_wh:
        products_with_min = Product.objects.filter(min_stock__isnull=False).order_by("name")
        stock_map = {
            s.product_id: s.quantity
            for s in Stock.objects.filter(warehouse=comun_wh, product__in=products_with_min)
        }
        for p in products_with_min:
            qty = stock_map.get(p.id, Decimal("0.00"))
            if qty < Decimal(str(p.min_stock)):
                low_stock_alerts.append({
                    "product_id": p.id,
                    "sku": p.sku or "",
                    "name": p.name,
                    "group": p.group or "",
                    "current": qty,
                    "min": p.min_stock,
                    "diff": p.min_stock - int(qty),
                })

    # --- Chart data: daily revenue last 30 days ---
    chart_from = timezone.now() - timedelta(days=29)
    daily_qs = (
        Sale.objects.filter(created_at__gte=chart_from)
        .annotate(day=TruncDate("created_at"))
        .values("day", "warehouse__type")
        .annotate(revenue=Sum("total"))
        .order_by("day")
    )
    # Build date range
    all_days = [(chart_from + timedelta(days=i)).date() for i in range(30)]
    ml_by_day = {}
    comun_by_day = {}
    for row in daily_qs:
        d = row["day"]
        rev = float(row["revenue"] or 0)
        if row["warehouse__type"] == Warehouse.WarehouseType.MERCADOLIBRE:
            ml_by_day[d] = ml_by_day.get(d, 0) + rev
        else:
            comun_by_day[d] = comun_by_day.get(d, 0) + rev
    chart_labels = [d.strftime("%-d/%-m") for d in all_days]
    chart_ml = [round(ml_by_day.get(d, 0), 2) for d in all_days]
    chart_comun = [round(comun_by_day.get(d, 0), 2) for d in all_days]

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
        "low_stock_alerts": low_stock_alerts,
        "chart_labels": json.dumps(chart_labels),
        "chart_ml": json.dumps(chart_ml),
        "chart_comun": json.dumps(chart_comun),
    }
    return render(request, "inventory/dashboard.html", context)
