"""Taxes view."""
from datetime import datetime, time
from decimal import Decimal

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.shortcuts import redirect, render
from django.utils import timezone

from ..models import PurchaseItem, SaleItem, TaxExpense, Warehouse
from .forms import TaxExpenseForm


@login_required
def taxes_view(request):
    tax_form = TaxExpenseForm()
    if request.method == "POST":
        action = request.POST.get("action") or ""
        if action == "delete_tax":
            tax_id = request.POST.get("tax_id")
            deleted, _ = TaxExpense.objects.filter(id=tax_id).delete()
            if deleted:
                messages.success(request, "Impuesto eliminado.")
            else:
                messages.error(request, "No se encontró el impuesto.")
            return redirect("inventory_taxes")

        tax_form = TaxExpenseForm(request.POST)
        if tax_form.is_valid():
            tax_form.save()
            messages.success(request, "Impuesto registrado.")
            return redirect("inventory_taxes")
        messages.error(request, "Revisá los datos del impuesto.")

    taxes = TaxExpense.objects.order_by("-paid_at", "-id")

    # ── Posición IVA ──────────────────────────────────────────────────────────
    iva_start = (request.GET.get("iva_start") or "").strip()
    iva_end = (request.GET.get("iva_end") or "").strip()
    start_dt = end_dt = None
    if iva_start:
        try:
            start_dt = timezone.make_aware(datetime.combine(datetime.strptime(iva_start, "%Y-%m-%d"), time.min))
        except ValueError:
            pass
    if iva_end:
        try:
            end_dt = timezone.make_aware(datetime.combine(datetime.strptime(iva_end, "%Y-%m-%d"), time.max))
        except ValueError:
            pass

    purchase_items_qs = (
        PurchaseItem.objects.filter(vat_percent__gt=0)
        .select_related("purchase", "product")
        .order_by("purchase__created_at", "purchase__id")
    )
    # ML: always 21% | Common: use stored vat_percent (only > 0)
    ml_items_qs = (
        SaleItem.objects
        .filter(sale__warehouse__type=Warehouse.WarehouseType.MERCADOLIBRE)
        .select_related("sale", "sale__warehouse", "product")
        .order_by("sale__created_at", "sale__id")
    )
    common_items_qs = (
        SaleItem.objects
        .filter(sale__warehouse__type=Warehouse.WarehouseType.COMUN, vat_percent__gt=0)
        .select_related("sale", "sale__warehouse", "product")
        .order_by("sale__created_at", "sale__id")
    )
    if start_dt:
        purchase_items_qs = purchase_items_qs.filter(purchase__created_at__gte=start_dt)
        ml_items_qs = ml_items_qs.filter(sale__created_at__gte=start_dt)
        common_items_qs = common_items_qs.filter(sale__created_at__gte=start_dt)
    if end_dt:
        purchase_items_qs = purchase_items_qs.filter(purchase__created_at__lte=end_dt)
        ml_items_qs = ml_items_qs.filter(sale__created_at__lte=end_dt)
        common_items_qs = common_items_qs.filter(sale__created_at__lte=end_dt)

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
            "source": "compra",
        })
        credito_total += vat_amount

    # IVA de gastos/impuestos
    expenses_with_vat = TaxExpense.objects.filter(vat_amount__gt=0).order_by("paid_at", "id")
    if start_dt:
        from django.utils.timezone import make_aware
        expenses_with_vat = expenses_with_vat.filter(paid_at__gte=start_dt.date())
    if end_dt:
        expenses_with_vat = expenses_with_vat.filter(paid_at__lte=end_dt.date())
    for expense in expenses_with_vat:
        credito_rows.append({
            "date": expense.paid_at,
            "comprobante": "-",
            "product": expense.description,
            "net": expense.amount,
            "vat_percent": None,
            "vat_amount": expense.vat_amount,
            "source": "gasto",
        })
        credito_total += expense.vat_amount
    credito_rows.sort(key=lambda x: x["date"] if hasattr(x["date"], "date") else x["date"])

    debito_rows = []
    debito_total = Decimal("0.00")
    for item in ml_items_qs:
        vat_amount = (item.line_total * Decimal("21") / 100).quantize(Decimal("0.01"))
        debito_rows.append({
            "date": item.sale.created_at,
            "comprobante": item.sale.ml_order_id or item.sale.invoice_number,
            "deposito": item.sale.warehouse.name,
            "product": item.product.name,
            "net": item.line_total,
            "vat_percent": Decimal("21"),
            "vat_amount": vat_amount,
        })
        debito_total += vat_amount
    for item in common_items_qs:
        vat_amount = (item.line_total * item.vat_percent / 100).quantize(Decimal("0.01"))
        debito_rows.append({
            "date": item.sale.created_at,
            "comprobante": item.sale.invoice_number,
            "deposito": item.sale.warehouse.name,
            "product": item.product.name,
            "net": item.line_total,
            "vat_percent": item.vat_percent,
            "vat_amount": vat_amount,
        })
        debito_total += vat_amount
    debito_rows.sort(key=lambda x: x["date"])

    posicion_iva = debito_total - credito_total

    return render(
        request,
        "inventory/taxes.html",
        {
            "tax_form": tax_form,
            "taxes": taxes,
            "credito_rows": credito_rows,
            "debito_rows": debito_rows,
            "credito_total": credito_total,
            "debito_total": debito_total,
            "posicion_iva": posicion_iva,
            "iva_start": iva_start,
            "iva_end": iva_end,
        },
    )
