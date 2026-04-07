"""Supplier views."""
from datetime import datetime, time
from decimal import Decimal

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.db.models import Sum
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone
from django.views.decorators.http import require_http_methods

from ..models import (
    Product,
    Purchase,
    Supplier,
    SupplierPayment,
    SupplierProduct,
)
from .forms import (
    SupplierForm,
    SupplierGroupForm,
    SupplierPaymentForm,
    SupplierProductForm,
    SupplierUnlinkGroupForm,
)


@login_required
@require_http_methods(["GET", "POST"])
def suppliers(request):
    supplier_form = SupplierForm()
    link_form = SupplierProductForm()
    link_group_form = SupplierGroupForm()
    unlink_group_form = SupplierUnlinkGroupForm()
    suppliers_qs = Supplier.objects.prefetch_related("supplier_products__product")

    if request.method == "POST":
        action = request.POST.get("action")
        if action == "create_supplier":
            supplier_form = SupplierForm(request.POST)
            if supplier_form.is_valid():
                supplier_form.save()
                messages.success(request, "Proveedor creado.")
                return redirect("inventory_suppliers")
        elif action == "link_supplier":
            link_form = SupplierProductForm(request.POST)
            if link_form.is_valid():
                supplier = link_form.cleaned_data["supplier"]
                product = link_form.cleaned_data["product"]
                last_cost = link_form.cleaned_data.get("last_cost") or product.avg_cost
                SupplierProduct.objects.update_or_create(
                    supplier=supplier,
                    product=product,
                    defaults={"last_cost": last_cost, "last_purchase_at": timezone.now()},
                )
                if product.default_supplier_id is None:
                    product.default_supplier = supplier
                    product.save(update_fields=["default_supplier"])
                messages.success(request, "Proveedor vinculado al producto.")
                return redirect("inventory_suppliers")
        elif action == "link_supplier_group":
            link_group_form = SupplierGroupForm(request.POST)
            if link_group_form.is_valid():
                supplier = link_group_form.cleaned_data["supplier"]
                group = (link_group_form.cleaned_data["group"] or "").strip()
                override_last_cost = link_group_form.cleaned_data.get("last_cost")
                products = Product.objects.filter(group__iexact=group).order_by("id")
                linked_count = 0
                default_updated_count = 0
                for product in products:
                    last_cost = override_last_cost if override_last_cost is not None else product.avg_cost
                    _, created = SupplierProduct.objects.update_or_create(
                        supplier=supplier,
                        product=product,
                        defaults={"last_cost": last_cost, "last_purchase_at": timezone.now()},
                    )
                    linked_count += 1 if created else 0
                    if product.default_supplier_id is None:
                        product.default_supplier = supplier
                        product.save(update_fields=["default_supplier"])
                        default_updated_count += 1
                if products.exists():
                    messages.success(
                        request,
                        (
                            f"Proveedor vinculado a {products.count()} productos de la marca/grupo '{group}'. "
                            f"Nuevos vínculos: {linked_count}. "
                            f"Proveedor principal actualizado en {default_updated_count}."
                        ),
                    )
                else:
                    messages.warning(request, f"No hay productos para la marca/grupo '{group}'.")
                return redirect("inventory_suppliers")
        elif action == "remove_supplier_group":
            unlink_group_form = SupplierUnlinkGroupForm(request.POST)
            if unlink_group_form.is_valid():
                supplier = unlink_group_form.cleaned_data["supplier"]
                group = (unlink_group_form.cleaned_data["group"] or "").strip()
                products = Product.objects.filter(group__iexact=group).order_by("id")
                if not products.exists():
                    messages.warning(request, f"No hay productos para la marca/grupo '{group}'.")
                    return redirect("inventory_suppliers")

                links_qs = SupplierProduct.objects.filter(supplier=supplier, product__in=products)
                removed_links = links_qs.count()
                links_qs.delete()

                default_cleared_count = 0
                default_reassigned_count = 0
                affected_products = products.filter(default_supplier=supplier)
                for product in affected_products:
                    replacement_supplier_id = (
                        SupplierProduct.objects.filter(product=product)
                        .exclude(supplier=supplier)
                        .order_by("-last_purchase_at", "-id")
                        .values_list("supplier_id", flat=True)
                        .first()
                    )
                    if replacement_supplier_id:
                        product.default_supplier_id = replacement_supplier_id
                        default_reassigned_count += 1
                    else:
                        product.default_supplier = None
                        default_cleared_count += 1
                    product.save(update_fields=["default_supplier"])

                messages.success(
                    request,
                    (
                        f"Se eliminaron {removed_links} vínculos del proveedor en la marca/grupo '{group}'. "
                        f"Proveedor principal reasignado en {default_reassigned_count} y limpiado en {default_cleared_count}."
                    ),
                )
                return redirect("inventory_suppliers")
        elif action == "delete_supplier":
            supplier_id = request.POST.get("supplier_id")
            Supplier.objects.filter(pk=supplier_id).delete()
            messages.success(request, "Proveedor eliminado.")
            return redirect("inventory_suppliers")
        elif action == "remove_link":
            link_id = request.POST.get("link_id")
            SupplierProduct.objects.filter(pk=link_id).delete()
            messages.success(request, "Vínculo eliminado.")
            return redirect("inventory_suppliers")

    context = {
        "supplier_form": supplier_form,
        "link_form": link_form,
        "link_group_form": link_group_form,
        "unlink_group_form": unlink_group_form,
        "suppliers": suppliers_qs,
    }
    purchases_totals = {
        row["supplier_id"]: row["total"] or Decimal("0.00")
        for row in Purchase.objects.filter(supplier__isnull=False)
        .values("supplier_id")
        .annotate(total=Sum("total"))
    }
    payments_totals = {
        row["supplier_id"]: row["total"] or Decimal("0.00")
        for row in SupplierPayment.objects.filter(kind=SupplierPayment.Kind.PAYMENT)
        .values("supplier_id")
        .annotate(total=Sum("amount"))
    }
    adjustments_totals = {
        row["supplier_id"]: row["total"] or Decimal("0.00")
        for row in SupplierPayment.objects.filter(kind=SupplierPayment.Kind.ADJUSTMENT)
        .values("supplier_id")
        .annotate(total=Sum("amount"))
    }
    supplier_rows = []
    debtors = []
    total_debt = Decimal("0.00")
    for supplier in suppliers_qs.order_by("name"):
        purchases_total = purchases_totals.get(supplier.id, Decimal("0.00"))
        payments_total = payments_totals.get(supplier.id, Decimal("0.00"))
        adjustments_total = adjustments_totals.get(supplier.id, Decimal("0.00"))
        balance = purchases_total - payments_total + adjustments_total
        supplier_rows.append({"supplier": supplier, "balance": balance})
        if balance > 0:
            debtors.append({"supplier": supplier, "balance": balance})
            total_debt += balance
    debtors.sort(key=lambda item: item["balance"], reverse=True)
    debtors = debtors[:8]
    context["supplier_rows"] = supplier_rows
    context["debtors"] = debtors
    context["total_debt"] = total_debt
    return render(request, "inventory/suppliers.html", context)


@login_required
def supplier_history_view(request, supplier_id):
    supplier = get_object_or_404(Supplier, id=supplier_id)
    payment_form = SupplierPaymentForm(supplier=supplier)

    if request.method == "POST":
        action = request.POST.get("action") or ""
        if action == "add_payment":
            payment_form = SupplierPaymentForm(request.POST, supplier=supplier)
            if payment_form.is_valid():
                payment = payment_form.save(commit=False)
                payment.supplier = supplier
                payment.save()
                messages.success(request, "Pago registrado.")
                return redirect("inventory_supplier_history", supplier_id=supplier.id)
            messages.error(request, "Revisá los datos del pago.")
        elif action == "delete_payment":
            payment_id = request.POST.get("payment_id")
            SupplierPayment.objects.filter(pk=payment_id, supplier=supplier).delete()
            messages.success(request, "Pago eliminado.")
            return redirect("inventory_supplier_history", supplier_id=supplier.id)

    purchases = list(
        Purchase.objects.filter(supplier=supplier)
        .select_related("warehouse")
        .order_by("-created_at", "-id")
    )
    payments = list(
        SupplierPayment.objects.filter(supplier=supplier)
        .select_related("purchase")
        .order_by("-paid_at", "-id")
    )

    payment_by_purchase = {}
    for payment in payments:
        if not payment.purchase_id:
            continue
        purchase_id = payment.purchase_id
        info = payment_by_purchase.setdefault(
            purchase_id,
            {"paid_total": Decimal("0.00"), "methods": []},
        )
        signed = payment.amount if payment.kind == SupplierPayment.Kind.PAYMENT else -payment.amount
        info["paid_total"] += signed
        info["methods"].append(payment.get_method_display())

    purchase_rows = []
    for purchase in purchases:
        paid_info = payment_by_purchase.get(purchase.id) or {"paid_total": Decimal("0.00"), "methods": []}
        paid_total = paid_info["paid_total"]
        balance = purchase.total - paid_total
        methods = ", ".join(dict.fromkeys([m for m in paid_info["methods"] if m])) or "-"
        purchase_rows.append(
            {
                "purchase": purchase,
                "paid_total": paid_total,
                "methods": methods,
                "balance": balance,
            }
        )

    ledger_entries = []
    for purchase in purchases:
        purchase_date = purchase.created_at
        if timezone.is_naive(purchase_date):
            purchase_date = timezone.make_aware(purchase_date, timezone.get_current_timezone())
        ledger_entries.append(
            {
                "date": purchase_date,
                "date_display": purchase.created_at,
                "kind": "PURCHASE",
                "label": purchase.invoice_number,
                "detail": f"Compra ({purchase.warehouse.name})",
                "debit": purchase.total,
                "credit": Decimal("0.00"),
            }
        )
    for payment in payments:
        is_payment = payment.kind == SupplierPayment.Kind.PAYMENT
        debit = Decimal("0.00") if is_payment else payment.amount
        credit = payment.amount if is_payment else Decimal("0.00")
        label = payment.get_method_display()
        detail_parts = [label]
        if payment.purchase_id:
            detail_parts.append(payment.purchase.invoice_number)
        if payment.notes:
            detail_parts.append(payment.notes)
        payment_date = datetime.combine(payment.paid_at, time.min)
        payment_date = timezone.make_aware(payment_date, timezone.get_current_timezone())
        ledger_entries.append(
            {
                "date": payment_date,
                "date_display": payment.paid_at,
                "kind": "PAYMENT" if is_payment else "ADJUSTMENT",
                "label": "Pago" if is_payment else "Ajuste/Devolución",
                "detail": " · ".join([part for part in detail_parts if part]),
                "debit": debit,
                "credit": credit,
                "payment_id": payment.id,
            }
        )

    ledger_entries.sort(key=lambda item: item["date"])
    balance = Decimal("0.00")
    for entry in ledger_entries:
        balance += entry["debit"]
        balance -= entry["credit"]
        entry["balance"] = balance

    total_purchases = sum((purchase.total for purchase in purchases), Decimal("0.00"))
    total_payments = sum(
        (payment.amount for payment in payments if payment.kind == SupplierPayment.Kind.PAYMENT),
        Decimal("0.00"),
    )
    total_adjustments = sum(
        (payment.amount for payment in payments if payment.kind == SupplierPayment.Kind.ADJUSTMENT),
        Decimal("0.00"),
    )
    current_balance = total_purchases - total_payments + total_adjustments

    return render(
        request,
        "inventory/supplier_history.html",
        {
            "supplier": supplier,
            "purchase_rows": purchase_rows,
            "payments": payments,
            "payment_form": payment_form,
            "ledger_entries": ledger_entries,
            "total_purchases": total_purchases,
            "total_payments": total_payments,
            "total_adjustments": total_adjustments,
            "current_balance": current_balance,
        },
    )
