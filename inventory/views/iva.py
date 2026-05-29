"""IVA position view."""
from datetime import datetime, time
from decimal import Decimal

from django.contrib.auth.decorators import login_required
from django.shortcuts import render
from django.utils import timezone

from ..models import Purchase, PurchaseItem, SaleItem


@login_required
def iva_position(request):
    start_date = (request.GET.get("start_date") or "").strip()
    end_date = (request.GET.get("end_date") or "").strip()
    start_dt = None
    end_dt = None

    if start_date:
        try:
            parsed = datetime.strptime(start_date, "%Y-%m-%d")
            start_dt = timezone.make_aware(datetime.combine(parsed, time.min))
        except ValueError:
            start_dt = None
    if end_date:
        try:
            parsed = datetime.strptime(end_date, "%Y-%m-%d")
            end_dt = timezone.make_aware(datetime.combine(parsed, time.max))
        except ValueError:
            end_dt = None

    # ── Crédito fiscal (compras) ──────────────────────────────────────────────
    purchase_items_qs = (
        PurchaseItem.objects
        .filter(vat_percent__gt=0)
        .select_related("purchase", "product")
        .order_by("purchase__created_at", "purchase__id")
    )
    if start_dt:
        purchase_items_qs = purchase_items_qs.filter(purchase__created_at__gte=start_dt)
    if end_dt:
        purchase_items_qs = purchase_items_qs.filter(purchase__created_at__lte=end_dt)

    credito_rows = []
    credito_total = Decimal("0.00")
    for item in purchase_items_qs:
        net = (item.quantity * item.unit_cost * (1 - item.discount_percent / 100)).quantize(Decimal("0.01"))
        vat_amount = (net * item.vat_percent / 100).quantize(Decimal("0.01"))
        credito_rows.append({
            "date": item.purchase.created_at,
            "comprobante": item.purchase.invoice_number,
            "product": item.product.name,
            "net": net,
            "vat_percent": item.vat_percent,
            "vat_amount": vat_amount,
        })
        credito_total += vat_amount

    # ── Débito fiscal (ventas) ────────────────────────────────────────────────
    sale_items_qs = (
        SaleItem.objects
        .filter(vat_percent__gt=0)
        .select_related("sale", "sale__warehouse", "product")
        .order_by("sale__created_at", "sale__id")
    )
    if start_dt:
        sale_items_qs = sale_items_qs.filter(sale__created_at__gte=start_dt)
    if end_dt:
        sale_items_qs = sale_items_qs.filter(sale__created_at__lte=end_dt)

    debito_rows = []
    debito_total = Decimal("0.00")
    for item in sale_items_qs:
        vat_amount = (item.line_total * item.vat_percent / 100).quantize(Decimal("0.01"))
        debito_rows.append({
            "date": item.sale.created_at,
            "comprobante": item.sale.ml_order_id or item.sale.invoice_number,
            "deposito": item.sale.warehouse.name,
            "product": item.product.name,
            "net": item.line_total,
            "vat_percent": item.vat_percent,
            "vat_amount": vat_amount,
        })
        debito_total += vat_amount

    posicion = debito_total - credito_total

    return render(request, "inventory/iva_position.html", {
        "credito_rows": credito_rows,
        "debito_rows": debito_rows,
        "credito_total": credito_total,
        "debito_total": debito_total,
        "posicion": posicion,
        "start_date": start_date,
        "end_date": end_date,
    })
