"""Taxes view."""
from datetime import date, datetime, time
from decimal import Decimal

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.shortcuts import redirect, render
from django.utils import timezone

from django.db import models
from ..models import AFIPInvoice, IVAPayment, PurchaseItem, Sale, SaleItem, TaxExpense, Warehouse
from .forms import IVAPaymentForm, TaxExpenseForm
from .afip import _parse_afip_xlsx

# Inicio de la ventana de crédito fiscal (régimen de transición RI: 12 meses
# anteriores a la inscripción, según listado de la contadora). Los comprobantes
# anteriores a esta fecha no computan crédito.
CREDITO_START_DATE = date(2025, 5, 1)
# A partir de esta fecha aplica débito fiscal (IVA en ventas)
DEBITO_START_DATE = date(2026, 6, 1)
IVA_FACTOR = Decimal("21") / Decimal("121")


def _calc_credito(purchase_qs, expenses_qs, ml_sales_qs):
    """Calcula crédito fiscal desde el ERP (compras + gastos + comisiones ML)."""
    rows = []
    total = Decimal("0.00")

    for item in purchase_qs:
        net = (item.quantity * item.unit_cost * (1 - item.discount_percent / 100)).quantize(Decimal("0.01"))
        vat = (net * item.vat_percent / 100).quantize(Decimal("0.01"))
        rows.append({
            "date": item.purchase.created_at,
            "comprobante": item.purchase.invoice_number,
            "product": item.product.name,
            "net": net,
            "vat_percent": item.vat_percent,
            "vat_amount": vat,
            "source": "compra",
        })
        total += vat

    for expense in expenses_qs:
        rows.append({
            "date": expense.paid_at,
            "comprobante": "-",
            "product": expense.description,
            "net": expense.amount,
            "vat_percent": None,
            "vat_amount": expense.vat_amount,
            "source": "gasto",
        })
        total += expense.vat_amount

    for sale in ml_sales_qs:
        vat = (sale.ml_commission_total * IVA_FACTOR).quantize(Decimal("0.01"))
        net = (sale.ml_commission_total - vat).quantize(Decimal("0.01"))
        rows.append({
            "date": sale.created_at,
            "comprobante": sale.ml_order_id or sale.invoice_number,
            "product": "Comisión ML",
            "net": net,
            "vat_percent": Decimal("21"),
            "vat_amount": vat,
            "source": "comision_ml",
        })
        total += vat

    rows.sort(key=lambda x: x["date"] if hasattr(x["date"], "date") else x["date"])
    subtotals = {
        "compras": sum(r["vat_amount"] for r in rows if r["source"] == "compra"),
        "gastos": sum(r["vat_amount"] for r in rows if r["source"] == "gasto"),
        "comisiones_ml": sum(r["vat_amount"] for r in rows if r["source"] == "comision_ml"),
    }
    return rows, total, subtotals


def _calc_credito_afip(afip_qs, expenses_qs):
    """Calcula crédito fiscal usando comprobantes AFIP + gastos manuales."""
    rows = []
    total = Decimal("0.00")

    for inv in afip_qs:
        credito = inv.credito_fiscal
        rows.append({
            "date": inv.date,
            "comprobante": inv.comprobante_str,
            "product": inv.razon_social,
            "net": -inv.neto_total if inv.is_nota_credito else inv.neto_total,
            "vat_percent": None,
            "vat_amount": credito,
            "source": "afip",
        })
        total += credito

    for expense in expenses_qs:
        rows.append({
            "date": expense.paid_at,
            "comprobante": "-",
            "product": expense.description,
            "net": expense.amount,
            "vat_percent": None,
            "vat_amount": expense.vat_amount,
            "source": "gasto",
        })
        total += expense.vat_amount

    rows.sort(key=lambda x: x["date"] if hasattr(x["date"], "date") else datetime.combine(x["date"], time.min))
    subtotals = {
        "afip": sum(r["vat_amount"] for r in rows if r["source"] == "afip"),
        "gastos": sum(r["vat_amount"] for r in rows if r["source"] == "gasto"),
        "comisiones_ml": Decimal("0.00"),
    }
    return rows, total, subtotals


def _calc_debito(ml_items_qs, common_items_qs):
    """Calcula débito fiscal total y devuelve (rows, total)."""
    rows = []
    total = Decimal("0.00")

    for item in ml_items_qs:
        vat = (item.line_total * Decimal("21") / 100).quantize(Decimal("0.01"))
        rows.append({
            "date": item.sale.created_at,
            "comprobante": item.sale.ml_order_id or item.sale.invoice_number,
            "deposito": item.sale.warehouse.name,
            "product": item.product.name,
            "net": item.line_total,
            "vat_percent": Decimal("21"),
            "vat_amount": vat,
        })
        total += vat

    for item in common_items_qs:
        vat = (item.line_total * item.vat_percent / 100).quantize(Decimal("0.01"))
        rows.append({
            "date": item.sale.created_at,
            "comprobante": item.sale.invoice_number,
            "deposito": item.sale.warehouse.name,
            "product": item.product.name,
            "net": item.line_total,
            "vat_percent": item.vat_percent,
            "vat_amount": vat,
        })
        total += vat

    rows.sort(key=lambda x: x["date"])
    return rows, total


@login_required
def taxes_view(request):
    tax_form = TaxExpenseForm()
    iva_payment_form = IVAPaymentForm()
    afip_import_msg = None
    if request.method == "POST":
        action = request.POST.get("action") or ""

        if action == "delete_afip_invoice":
            inv_id = request.POST.get("invoice_id")
            deleted, _ = AFIPInvoice.objects.filter(id=inv_id).delete()
            if deleted:
                messages.success(request, "Comprobante eliminado.")
            else:
                messages.error(request, "No se encontró el comprobante.")
            return redirect("inventory_taxes")

        if action == "delete_all_afip":
            count, _ = AFIPInvoice.objects.all().delete()
            messages.success(request, f"Se eliminaron {count} comprobantes AFIP.")
            return redirect("inventory_taxes")

        if action == "delete_tax":
            tax_id = request.POST.get("tax_id")
            deleted, _ = TaxExpense.objects.filter(id=tax_id).delete()
            if deleted:
                messages.success(request, "Gasto eliminado.")
            else:
                messages.error(request, "No se encontró el gasto.")
            return redirect("inventory_taxes")

        if action == "delete_iva_payment":
            pk = request.POST.get("payment_id")
            deleted, _ = IVAPayment.objects.filter(id=pk).delete()
            if deleted:
                messages.success(request, "Pago de IVA eliminado.")
            else:
                messages.error(request, "No se encontró el pago.")
            return redirect("inventory_taxes")

        if action == "add_iva_payment":
            iva_payment_form = IVAPaymentForm(request.POST)
            if iva_payment_form.is_valid():
                iva_payment_form.save()
                messages.success(request, "Pago de IVA registrado.")
                return redirect("inventory_taxes")
            messages.error(request, "Revisá los datos del pago de IVA.")

        elif action == "import_afip":
            upload = request.FILES.get("file")
            if not upload:
                afip_import_msg = ("error", "Seleccioná un archivo .xlsx.")
            else:
                created, duplicates, filtered, errors, file_err = _parse_afip_xlsx(upload)
                if file_err:
                    afip_import_msg = ("error", file_err)
                else:
                    parts = [f"{created} comprobante{'s' if created != 1 else ''} nuevo{'s' if created != 1 else ''} importado{'s' if created != 1 else ''}"]
                    if duplicates:
                        parts.append(f"{duplicates} ya estaban guardados")
                    if filtered:
                        parts.append(f"{filtered} de tipo B/C omitidos")
                    if errors:
                        parts.append(f"{errors} con error")
                    afip_import_msg = ("ok" if not errors else "error", " — ".join(parts) + ".")

        else:
            tax_form = TaxExpenseForm(request.POST)
            if tax_form.is_valid():
                tax_form.save()
                messages.success(request, "Gasto registrado.")
                return redirect("inventory_taxes")
            messages.error(request, "Revisá los datos del gasto.")

    taxes = TaxExpense.objects.order_by("-paid_at", "-id")
    iva_payments = IVAPayment.objects.all()

    # ── AFIP: resumen de comprobantes importados ──────────────────────────────
    afip_invoices = AFIPInvoice.objects.filter(
        tipo_codigo__in=AFIPInvoice.CREDITO_TIPOS
    ).order_by("-date")
    use_afip = afip_invoices.exists()
    afip_count = afip_invoices.count()
    afip_total_iva = sum(inv.credito_fiscal for inv in afip_invoices)

    # ── Posición IVA global ───────────────────────────────────────────────────
    credito_start_dt = timezone.make_aware(datetime.combine(CREDITO_START_DATE, time.min))
    debito_start_dt = timezone.make_aware(datetime.combine(DEBITO_START_DATE, time.min))

    g_expenses = (
        TaxExpense.objects.filter(vat_amount__gt=0, paid_at__gte=CREDITO_START_DATE)
        .order_by("paid_at", "id")
    )
    g_ml_items = (
        SaleItem.objects
        .filter(sale__warehouse__type=Warehouse.WarehouseType.MERCADOLIBRE,
                sale__created_at__gte=debito_start_dt)
        .select_related("sale", "sale__warehouse", "product")
        .order_by("sale__created_at", "sale__id")
    )
    g_common_items = (
        SaleItem.objects
        .filter(sale__warehouse__type=Warehouse.WarehouseType.COMUN,
                vat_percent__gt=0,
                sale__created_at__gte=debito_start_dt)
        .select_related("sale", "sale__warehouse", "product")
        .order_by("sale__created_at", "sale__id")
    )

    if use_afip:
        g_afip_qs = AFIPInvoice.objects.filter(
            tipo_codigo__in=AFIPInvoice.CREDITO_TIPOS,
            date__gte=CREDITO_START_DATE,
        ).order_by("date")
        _, credito_global, _ = _calc_credito_afip(g_afip_qs, g_expenses)
    else:
        g_purchases = (
            PurchaseItem.objects.filter(vat_percent__gt=0, purchase__created_at__gte=credito_start_dt)
            .select_related("purchase", "product")
            .order_by("purchase__created_at", "purchase__id")
        )
        g_ml_sales_credito = (
            Sale.objects
            .filter(warehouse__type=Warehouse.WarehouseType.MERCADOLIBRE,
                    ml_commission_total__gt=0,
                    created_at__gte=credito_start_dt)
            .order_by("created_at", "id")
        )
        _, credito_global, _ = _calc_credito(g_purchases, g_expenses, g_ml_sales_credito)

    _, debito_global = _calc_debito(g_ml_items, g_common_items)
    pagos_total = IVAPayment.objects.aggregate(
        total=models.Sum("amount")
    )["total"] or Decimal("0.00")
    posicion_global = debito_global - credito_global - pagos_total

    # ── Posición IVA filtrada (para el detalle) ───────────────────────────────
    iva_start = (request.GET.get("iva_start") or "").strip()
    iva_end = (request.GET.get("iva_end") or "").strip()
    start_dt = end_dt = None
    start_d = end_d = None
    if iva_start:
        try:
            start_d = datetime.strptime(iva_start, "%Y-%m-%d").date()
            start_dt = timezone.make_aware(datetime.combine(start_d, time.min))
        except ValueError:
            pass
    if iva_end:
        try:
            end_d = datetime.strptime(iva_end, "%Y-%m-%d").date()
            end_dt = timezone.make_aware(datetime.combine(end_d, time.max))
        except ValueError:
            pass

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
        ml_items_qs = ml_items_qs.filter(sale__created_at__gte=start_dt)
        common_items_qs = common_items_qs.filter(sale__created_at__gte=start_dt)
    if end_dt:
        ml_items_qs = ml_items_qs.filter(sale__created_at__lte=end_dt)
        common_items_qs = common_items_qs.filter(sale__created_at__lte=end_dt)

    expenses_qs = TaxExpense.objects.filter(vat_amount__gt=0).order_by("paid_at", "id")
    if start_d:
        expenses_qs = expenses_qs.filter(paid_at__gte=start_d)
    if end_d:
        expenses_qs = expenses_qs.filter(paid_at__lte=end_d)

    if use_afip:
        f_afip_qs = AFIPInvoice.objects.filter(
            tipo_codigo__in=AFIPInvoice.CREDITO_TIPOS
        ).order_by("date")
        if start_d:
            f_afip_qs = f_afip_qs.filter(date__gte=start_d)
        if end_d:
            f_afip_qs = f_afip_qs.filter(date__lte=end_d)
        credito_rows, credito_total, credito_subtotals = _calc_credito_afip(f_afip_qs, expenses_qs)
    else:
        purchase_items_qs = (
            PurchaseItem.objects.filter(vat_percent__gt=0)
            .select_related("purchase", "product")
            .order_by("purchase__created_at", "purchase__id")
        )
        ml_sales_qs = (
            Sale.objects
            .filter(warehouse__type=Warehouse.WarehouseType.MERCADOLIBRE, ml_commission_total__gt=0)
            .order_by("created_at", "id")
        )
        if start_dt:
            purchase_items_qs = purchase_items_qs.filter(purchase__created_at__gte=start_dt)
            ml_sales_qs = ml_sales_qs.filter(created_at__gte=start_dt)
        if end_dt:
            purchase_items_qs = purchase_items_qs.filter(purchase__created_at__lte=end_dt)
            ml_sales_qs = ml_sales_qs.filter(created_at__lte=end_dt)
        credito_rows, credito_total, credito_subtotals = _calc_credito(purchase_items_qs, expenses_qs, ml_sales_qs)

    debito_rows, debito_total = _calc_debito(ml_items_qs, common_items_qs)
    posicion_iva = debito_total - credito_total

    return render(
        request,
        "inventory/taxes.html",
        {
            "tax_form": tax_form,
            "taxes": taxes,
            "iva_payment_form": iva_payment_form,
            "iva_payments": iva_payments,
            # AFIP
            "use_afip": use_afip,
            "afip_invoices": afip_invoices[:100],
            "afip_count": afip_count,
            "afip_total_iva": afip_total_iva,
            "afip_import_msg": afip_import_msg,
            # Posición global
            "credito_global": credito_global,
            "debito_global": debito_global,
            "pagos_total": pagos_total,
            "posicion_global": posicion_global,
            "credito_start_date": CREDITO_START_DATE,
            "debito_start_date": DEBITO_START_DATE,
            # Detalle filtrado
            "credito_rows": credito_rows,
            "debito_rows": debito_rows,
            "credito_total": credito_total,
            "credito_compras": credito_subtotals.get("compras", Decimal("0.00")),
            "credito_gastos": credito_subtotals["gastos"],
            "credito_comisiones_ml": credito_subtotals["comisiones_ml"],
            "credito_afip": credito_subtotals.get("afip", Decimal("0.00")),
            "debito_total": debito_total,
            "posicion_iva": posicion_iva,
            "iva_start": iva_start,
            "iva_end": iva_end,
        },
    )
