"""Customer views."""
from datetime import datetime, time
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.db import transaction
from django.db.models import Sum
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone

from ..models import (
    CreditNote,
    CreditNoteItem,
    Customer,
    CustomerGroupDiscount,
    CustomerPayment,
    CustomerProductDiscount,
    CustomerProductPrice,
    Product,
    Sale,
)
from .forms import (
    CustomerCreditNoteForm,
    CustomerDiscountForm,
    CustomerForm,
    CustomerGroupDiscountForm,
    CustomerPaymentForm,
    CustomerProductPriceForm,
)


@login_required
def customers_view(request):
    customer_form = CustomerForm()
    discount_form = CustomerDiscountForm()
    group_discount_form = CustomerGroupDiscountForm()
    custom_price_form = CustomerProductPriceForm()

    if request.method == "POST":
        action = request.POST.get("action")
        if action == "create_customer":
            customer_form = CustomerForm(request.POST)
            if customer_form.is_valid():
                customer_form.save()
                messages.success(request, "Cliente creado.")
                return redirect("inventory_customers")
            else:
                messages.error(request, "Revisá los datos del cliente.")
        elif action == "create_discount":
            discount_form = CustomerDiscountForm(request.POST)
            if discount_form.is_valid():
                customer = discount_form.cleaned_data["customer"]
                product = discount_form.cleaned_data["product"]
                discount = discount_form.cleaned_data["discount_percent"]
                CustomerProductDiscount.objects.update_or_create(
                    customer=customer,
                    product=product,
                    defaults={"discount_percent": discount},
                )
                messages.success(request, "Descuento asignado.")
                return redirect("inventory_customers")
            else:
                messages.error(request, "Revisá los datos del descuento.")
        elif action == "create_group_discount":
            group_discount_form = CustomerGroupDiscountForm(request.POST)
            if group_discount_form.is_valid():
                customer = group_discount_form.cleaned_data["customer"]
                group = (group_discount_form.cleaned_data["group"] or "").strip()
                discount = group_discount_form.cleaned_data["discount_percent"]
                CustomerGroupDiscount.objects.update_or_create(
                    customer=customer,
                    group=group,
                    defaults={"discount_percent": discount},
                )
                messages.success(request, "Descuento por grupo asignado.")
                return redirect("inventory_customers")
            messages.error(request, "Revisá los datos del descuento por grupo.")
        elif action == "create_custom_price":
            custom_price_form = CustomerProductPriceForm(request.POST)
            if custom_price_form.is_valid():
                customer = custom_price_form.cleaned_data["customer"]
                product = custom_price_form.cleaned_data["product"]
                unit_price = custom_price_form.cleaned_data["unit_price"]
                unit_cost = custom_price_form.cleaned_data["unit_cost"]
                if unit_price is None and unit_cost is None:
                    CustomerProductPrice.objects.filter(customer=customer, product=product).delete()
                    messages.success(request, "Regla específica eliminada. Se vuelve al precio/costo predeterminado.")
                    return redirect("inventory_customers")
                CustomerProductPrice.objects.update_or_create(
                    customer=customer,
                    product=product,
                    defaults={"unit_price": unit_price, "unit_cost": unit_cost},
                )
                messages.success(request, "Precio/costo específico asignado.")
                return redirect("inventory_customers")
            messages.error(request, "Revisá los datos del precio/costo específico.")
        elif action == "update_customer_audience":
            customer_id = request.POST.get("customer_id")
            audience = request.POST.get("audience")
            valid_audiences = {choice[0] for choice in Customer.Audience.choices}
            if customer_id and audience in valid_audiences:
                Customer.objects.filter(id=customer_id).update(audience=audience)
                messages.success(request, "Tipo de cliente actualizado.")
                return redirect("inventory_customers")
            messages.error(request, "Revisá el tipo de cliente.")
        elif action == "update_customer_phone":
            customer_id = request.POST.get("customer_id")
            phone = (request.POST.get("phone") or "").strip()
            if customer_id:
                Customer.objects.filter(id=customer_id).update(email=phone)
                messages.success(request, "Teléfono actualizado.")
                return redirect("inventory_customers")
            messages.error(request, "No se pudo actualizar el teléfono.")

    customers = Customer.objects.prefetch_related("discounts__product", "group_discounts", "custom_prices__product").order_by("name")
    sales_totals = {
        row["customer_id"]: row["total"] or Decimal("0.00")
        for row in Sale.objects.filter(customer__isnull=False)
        .values("customer_id")
        .annotate(total=Sum("total"))
    }
    payments_totals = {
        row["customer_id"]: row["total"] or Decimal("0.00")
        for row in CustomerPayment.objects.filter(kind=CustomerPayment.Kind.PAYMENT)
        .values("customer_id")
        .annotate(total=Sum("amount"))
    }
    refunds_totals = {
        row["customer_id"]: row["total"] or Decimal("0.00")
        for row in CustomerPayment.objects.filter(kind=CustomerPayment.Kind.REFUND)
        .values("customer_id")
        .annotate(total=Sum("amount"))
    }
    credit_notes_totals = {
        row["customer_id"]: row["total"] or Decimal("0.00")
        for row in CustomerPayment.objects.filter(kind=CustomerPayment.Kind.CREDIT_NOTE)
        .values("customer_id")
        .annotate(total=Sum("amount"))
    }
    debtors = []
    total_debt = Decimal("0.00")
    for customer in customers:
        sales_total = sales_totals.get(customer.id, Decimal("0.00"))
        payments_total = payments_totals.get(customer.id, Decimal("0.00"))
        refunds_total = refunds_totals.get(customer.id, Decimal("0.00"))
        credit_notes_total = credit_notes_totals.get(customer.id, Decimal("0.00"))
        balance = sales_total - payments_total + refunds_total - credit_notes_total
        if balance > 0:
            debtors.append({"customer": customer, "balance": balance})
            total_debt += balance
    debtors.sort(key=lambda item: item["balance"], reverse=True)
    debtors = debtors[:8]
    group_options = list(
        Product.objects.exclude(group__exact="")
        .values_list("group", flat=True)
        .distinct()
        .order_by("group")
    )
    return render(
        request,
        "inventory/customers.html",
        {
            "customer_form": customer_form,
            "discount_form": discount_form,
            "group_discount_form": group_discount_form,
            "custom_price_form": custom_price_form,
            "customers": customers,
            "audience_choices": Customer.Audience.choices,
            "group_options": group_options,
            "total_debt": total_debt,
            "debtors": debtors,
        },
    )


@login_required
def customer_history_view(request, customer_id):
    customer = get_object_or_404(Customer, id=customer_id)
    payment_form = CustomerPaymentForm(customer=customer)
    credit_note_form = CustomerCreditNoteForm(customer=customer)

    if request.method == "POST":
        action = request.POST.get("action") or ""
        if action == "add_payment":
            payment_form = CustomerPaymentForm(request.POST, customer=customer)
            if payment_form.is_valid():
                payment = payment_form.save(commit=False)
                payment.customer = customer
                payment.save()
                messages.success(request, "Pago registrado.")
                return redirect("inventory_customer_history", customer_id=customer.id)
            messages.error(request, "Revisá los datos del pago.")
        elif action == "add_credit_note":
            credit_note_form = CustomerCreditNoteForm(request.POST, customer=customer)
            if credit_note_form.is_valid():
                note = credit_note_form.save(commit=False)
                note.customer = customer
                note.kind = CustomerPayment.Kind.CREDIT_NOTE
                note.method = CustomerPayment.Method.OTHER
                note.save()
                messages.success(request, "Nota de crédito registrada.")
                return redirect("inventory_customer_history", customer_id=customer.id)
            messages.error(request, "Revisá los datos de la nota de crédito.")

    sales = list(
        Sale.objects.filter(customer=customer)
        .select_related("warehouse")
        .order_by("-created_at", "-id")
    )
    payments = list(
        CustomerPayment.objects.filter(customer=customer)
        .select_related("sale")
        .order_by("-paid_at", "-id")
    )

    payment_by_sale = {}
    for payment in payments:
        if not payment.sale_id:
            continue
        sale_id = payment.sale_id
        info = payment_by_sale.setdefault(
            sale_id,
            {"paid_total": Decimal("0.00"), "methods": []},
        )
        signed = payment.amount if payment.kind == CustomerPayment.Kind.PAYMENT else -payment.amount
        info["paid_total"] += signed
        info["methods"].append(payment.get_method_display())

    sales_rows = []
    for sale in sales:
        paid_info = payment_by_sale.get(sale.id) or {"paid_total": Decimal("0.00"), "methods": []}
        paid_total = paid_info["paid_total"]
        balance = sale.total - paid_total
        methods = ", ".join(dict.fromkeys([m for m in paid_info["methods"] if m])) or "-"
        sales_rows.append(
            {
                "sale": sale,
                "paid_total": paid_total,
                "methods": methods,
                "balance": balance,
            }
        )

    ledger_entries = []
    for sale in sales:
        sale_date = sale.created_at
        if timezone.is_naive(sale_date):
            sale_date = timezone.make_aware(sale_date, timezone.get_current_timezone())
        ledger_entries.append(
            {
                "date": sale_date,
                "date_display": sale.created_at,
                "kind": "SALE",
                "label": sale.ml_order_id or sale.invoice_number,
                "detail": f"Venta ({sale.warehouse.name})",
                "debit": sale.total,
                "credit": Decimal("0.00"),
            }
        )
    for payment in payments:
        kind = payment.kind
        is_credit = kind in (CustomerPayment.Kind.PAYMENT, CustomerPayment.Kind.CREDIT_NOTE)
        debit = Decimal("0.00") if is_credit else payment.amount
        credit = payment.amount if is_credit else Decimal("0.00")
        if kind == CustomerPayment.Kind.PAYMENT:
            label = "Pago"
            detail_prefix = payment.get_method_display()
        elif kind == CustomerPayment.Kind.CREDIT_NOTE:
            label = "Nota de crédito"
            detail_prefix = "NC"
        else:
            label = "Devolución/Ajuste"
            detail_prefix = payment.get_method_display()
        detail_parts = [detail_prefix]
        if payment.sale_id:
            detail_parts.append(payment.sale.ml_order_id or payment.sale.invoice_number)
        if payment.notes:
            detail_parts.append(payment.notes)
        payment_date = datetime.combine(payment.paid_at, time.min)
        payment_date = timezone.make_aware(payment_date, timezone.get_current_timezone())
        ledger_entries.append(
            {
                "date": payment_date,
                "date_display": payment.paid_at,
                "kind": kind,
                "label": label,
                "detail": " · ".join([part for part in detail_parts if part]),
                "debit": debit,
                "credit": credit,
            }
        )

    ledger_entries.sort(key=lambda item: item["date"])
    balance = Decimal("0.00")
    for entry in ledger_entries:
        balance += entry["debit"]
        balance -= entry["credit"]
        entry["balance"] = balance

    total_sales = sum((sale.total for sale in sales), Decimal("0.00"))
    total_payments = sum(
        (p.amount for p in payments if p.kind == CustomerPayment.Kind.PAYMENT),
        Decimal("0.00"),
    )
    total_refunds = sum(
        (p.amount for p in payments if p.kind == CustomerPayment.Kind.REFUND),
        Decimal("0.00"),
    )
    total_credit_notes = sum(
        (p.amount for p in payments if p.kind == CustomerPayment.Kind.CREDIT_NOTE),
        Decimal("0.00"),
    )
    current_balance = total_sales - total_payments + total_refunds - total_credit_notes

    return render(
        request,
        "inventory/customer_history.html",
        {
            "customer": customer,
            "sales_rows": sales_rows,
            "payments": payments,
            "payment_form": payment_form,
            "credit_note_form": credit_note_form,
            "ledger_entries": ledger_entries,
            "total_sales": total_sales,
            "total_payments": total_payments,
            "total_refunds": total_refunds,
            "total_credit_notes": total_credit_notes,
            "current_balance": current_balance,
        },
    )


@login_required
def create_credit_note(request, customer_id):
    customer = get_object_or_404(Customer, id=customer_id)
    sales = Sale.objects.filter(customer=customer).order_by("-created_at")

    selected_sale = None
    sale_items_data = []
    step = "select_sale"
    today = timezone.localdate().isoformat()

    if request.method == "POST":
        action = request.POST.get("action") or ""

        if action == "load_items":
            sale_id = request.POST.get("sale_id")
            if sale_id:
                selected_sale = get_object_or_404(Sale, id=sale_id, customer=customer)
                sale_items_data = list(selected_sale.items.select_related("product").all())
                step = "select_items"
            else:
                messages.error(request, "Seleccioná una venta.")

        elif action == "create":
            sale_id = request.POST.get("sale_id")
            selected_sale = get_object_or_404(Sale, id=sale_id, customer=customer)
            sale_items_data = list(selected_sale.items.select_related("product").all())
            date_str = request.POST.get("date") or today
            notes = (request.POST.get("notes") or "").strip()

            cn_items = []
            total = Decimal("0.00")
            for item in sale_items_data:
                qty_str = (request.POST.get(f"qty_{item.id}") or "0").replace(",", ".")
                try:
                    qty = Decimal(qty_str).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                except InvalidOperation:
                    qty = Decimal("0.00")
                if qty <= 0:
                    continue
                qty = min(qty, item.quantity)
                line_total = (qty * item.final_unit_price).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                cn_items.append({
                    "product": item.product,
                    "quantity": qty,
                    "unit_price": item.final_unit_price,
                    "line_total": line_total,
                })
                total += line_total

            if not cn_items:
                messages.error(request, "Seleccioná al menos un ítem con cantidad mayor a 0.")
                step = "select_items"
            else:
                with transaction.atomic():
                    cn = CreditNote.objects.create(
                        customer=customer,
                        sale=selected_sale,
                        date=date_str,
                        notes=notes,
                        total=total,
                        user=request.user,
                    )
                    for cn_item in cn_items:
                        CreditNoteItem.objects.create(
                            credit_note=cn,
                            product=cn_item["product"],
                            quantity=cn_item["quantity"],
                            unit_price=cn_item["unit_price"],
                            line_total=cn_item["line_total"],
                        )
                    CustomerPayment.objects.create(
                        customer=customer,
                        sale=selected_sale,
                        amount=total,
                        method=CustomerPayment.Method.OTHER,
                        kind=CustomerPayment.Kind.CREDIT_NOTE,
                        paid_at=date_str,
                        notes=f"NC #{cn.id}" + (f" · {notes}" if notes else ""),
                    )
                messages.success(request, f"Nota de crédito #{cn.id} emitida.")
                return redirect("inventory_customer_history", customer_id=customer.id)

    return render(
        request,
        "inventory/credit_note_create.html",
        {
            "customer": customer,
            "sales": sales,
            "selected_sale": selected_sale,
            "sale_items_data": sale_items_data,
            "step": step,
            "today": today,
        },
    )
