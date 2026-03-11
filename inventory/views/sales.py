"""Sale views."""
import csv
import io
import os
import re
import unicodedata
from datetime import datetime, time
from decimal import Decimal

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.paginator import Paginator
from django.db import transaction
from django.db.models import Q
from django.forms import formset_factory
from django.http import HttpResponse, JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone
from django.views.decorators.http import require_http_methods

from .. import services
from .. import mercadolibre as ml
from ..models import (
    Customer,
    KitComponent,
    MercadoLibreItem,
    Product,
    ProductVariant,
    Sale,
    SaleItem,
    Stock,
    StockMovement,
    Warehouse,
)
from .common import _resolve_sale_item_pricing
from .forms import SaleHeaderForm, SaleItemForm
from .koda import _koda_sync_common_with_variants
from .utils_xlsx import _read_ml_sales_xlsx_rows


@login_required
def register_sale(request):
    return sales_list(request)


@login_required
def sale_receipt(request, sale_id: int):
    sale = get_object_or_404(
        Sale.objects.select_related("customer", "warehouse").prefetch_related("items__product", "items__variant"),
        pk=sale_id,
    )
    items = list(sale.items.all())
    subtotal = sum((item.unit_price * item.quantity for item in items), Decimal("0.00"))
    discount_total = sale.discount_total or Decimal("0.00")
    per_item_discount = sum(((item.unit_price * item.quantity) - item.line_total for item in items), Decimal("0.00"))
    extra_discount_amount = discount_total - per_item_discount
    if extra_discount_amount < 0:
        extra_discount_amount = Decimal("0.00")
    extra_discount_percent = (
        (extra_discount_amount / subtotal * Decimal("100.00")).quantize(Decimal("0.01"))
        if subtotal > 0
        else Decimal("0.00")
    )
    is_ml = sale.warehouse.type == Warehouse.WarehouseType.MERCADOLIBRE
    subtotal_display = subtotal
    total_display = sale.total if is_ml and sale.total is not None else (subtotal - discount_total)
    commission_total = sale.ml_commission_total or Decimal("0.00")
    tax_total = sale.ml_tax_total or Decimal("0.00")
    net_total = (total_display - commission_total - tax_total) if is_ml else None
    invoice_number = sale.ml_order_id or sale.invoice_number
    context = {
        "sale": sale,
        "items": items,
        "subtotal": subtotal_display,
        "discount_total": discount_total,
        "extra_discount_percent": extra_discount_percent,
        "total": total_display,
        "is_ml": is_ml,
        "commission_total": commission_total,
        "tax_total": tax_total,
        "net_total": net_total,
        "invoice_number": invoice_number,
    }
    return render(request, "inventory/sale_receipt.html", context)


@login_required
def sale_receipt_pdf(request, sale_id: int):
    sale = get_object_or_404(
        Sale.objects.select_related("customer", "warehouse").prefetch_related("items__product", "items__variant"),
        pk=sale_id,
    )
    items = list(sale.items.all())
    subtotal = sum((item.unit_price * item.quantity for item in items), Decimal("0.00"))
    discount_total = sale.discount_total or Decimal("0.00")
    per_item_discount = sum(((item.unit_price * item.quantity) - item.line_total for item in items), Decimal("0.00"))
    extra_discount_amount = discount_total - per_item_discount
    if extra_discount_amount < 0:
        extra_discount_amount = Decimal("0.00")
    extra_discount_percent = (
        (extra_discount_amount / subtotal * Decimal("100.00")).quantize(Decimal("0.01"))
        if subtotal > 0
        else Decimal("0.00")
    )
    is_ml = sale.warehouse.type == Warehouse.WarehouseType.MERCADOLIBRE
    subtotal_display = subtotal
    total_display = sale.total if is_ml and sale.total is not None else (subtotal - discount_total)
    commission_total = sale.ml_commission_total or Decimal("0.00")
    tax_total = sale.ml_tax_total or Decimal("0.00")
    net_total = (total_display - commission_total - tax_total) if is_ml else None
    full_page_size = 27
    last_page_size = 20
    pages = []
    remaining = list(items)
    while len(remaining) > full_page_size + last_page_size:
        pages.append({"items": remaining[:full_page_size]})
        remaining = remaining[full_page_size:]
    if len(remaining) > last_page_size:
        split_size = len(remaining) - last_page_size
        pages.append({"items": remaining[:split_size]})
        remaining = remaining[split_size:]
    pages.append({"items": remaining})
    for idx, page in enumerate(pages):
        page["show_totals"] = idx == len(pages) - 1
        page["page_number"] = idx + 1
        page["page_count"] = len(pages)
    from django.template.loader import render_to_string

    html = render_to_string(
        "inventory/sale_receipt_pdf.html",
        {
            "sale": sale,
            "items": items,
            "pages": pages,
            "subtotal": subtotal_display,
            "discount_total": discount_total,
            "extra_discount_percent": extra_discount_percent,
            "total": total_display,
            "is_ml": is_ml,
            "commission_total": commission_total,
            "tax_total": tax_total,
            "net_total": net_total,
            "invoice_number": sale.ml_order_id or sale.invoice_number,
        },
        request=request,
    )
    try:
        from weasyprint import HTML
    except Exception:
        return HttpResponse(
            "WeasyPrint no está instalado. Instalalo con 'pip install weasyprint' para generar PDF.",
            status=500,
        )

    pdf_bytes = HTML(string=html, base_url=request.build_absolute_uri("/")).write_pdf()
    invoice_raw = sale.ml_order_id or sale.invoice_number
    if isinstance(invoice_raw, str) and "-" in invoice_raw:
        invoice_raw = invoice_raw.split("-", 1)[1]
    invoice_number = str(invoice_raw).lstrip("0") or str(sale.id)
    response = HttpResponse(pdf_bytes, content_type="application/pdf")
    response["Content-Disposition"] = f'attachment; filename="FACT-{invoice_number}.pdf"'
    return response


@login_required
def sale_edit(request, sale_id: int):
    sale = get_object_or_404(
        Sale.objects.select_related("customer", "warehouse").prefetch_related("items__product", "items__variant", "movements"),
        pk=sale_id,
    )
    SaleItemFormSet = formset_factory(SaleItemForm, extra=0, can_delete=True)
    customer_audiences = {str(customer.id): customer.audience for customer in Customer.objects.only("id", "audience")}

    if request.method == "POST":
        post_data = request.POST.copy()
        for key in list(post_data.keys()):
            if not key.endswith("-product_text"):
                continue
            prefix = key[:-len("product_text")]
            product_key = f"{prefix}product"
            if post_data.get(product_key):
                continue
            product_text = (post_data.get(key) or "").strip()
            if not product_text:
                continue
            label = product_text.split(" (", 1)[0].strip()
            sku_candidate = ""
            name_candidate = label
            if " - " in label:
                sku_candidate, name_candidate = [part.strip() for part in label.split(" - ", 1)]
            product = None
            if sku_candidate and sku_candidate.lower() != "sin sku":
                product = Product.objects.filter(sku__iexact=sku_candidate).first()
            if not product and name_candidate:
                product = (
                    Product.objects.filter(name__iexact=name_candidate).first()
                    or Product.objects.filter(name__icontains=name_candidate).first()
                )
            if product:
                post_data[product_key] = str(product.id)
        header_form = SaleHeaderForm(post_data)
        formset = SaleItemFormSet(post_data)
        for form in formset.forms:
            prefix = form.prefix
            product_raw = (post_data.get(f"{prefix}-product") or "").strip()
            product_text = (post_data.get(f"{prefix}-product_text") or "").strip()
            qty_raw = (post_data.get(f"{prefix}-quantity") or "").strip()
            qty_value = None
            if qty_raw:
                try:
                    qty_value = Decimal(str(qty_raw).replace(",", "."))
                except Exception:
                    qty_value = None
            qty_zeroish = qty_raw in {"", "0", "0.0", "0.00", "0,00"} or (qty_value is not None and qty_value <= 0)
            if not product_raw and not product_text and qty_zeroish:
                form.empty_permitted = True
            elif not form.has_changed():
                form.empty_permitted = True
        if header_form.is_valid() and formset.is_valid():
            warehouse = header_form.cleaned_data["warehouse"]
            require_variants = warehouse.type == Warehouse.WarehouseType.COMUN
            items = []
            for form in formset:
                if form.cleaned_data.get("DELETE"):
                    continue
                product = form.cleaned_data.get("product")
                if not product:
                    continue
                variant_id_raw = (post_data.get(f"{form.prefix}-variant_id") or "").strip()
                variant = None
                if variant_id_raw:
                    variant = ProductVariant.objects.filter(id=variant_id_raw, product=product).first()
                    if not variant:
                        form.add_error(None, "Variedad inválida.")
                if require_variants and ProductVariant.objects.filter(product=product).exists() and not variant:
                    form.add_error(None, "Seleccioná una variedad.")
                items.append({**form.cleaned_data, "variant": variant})
            if any(form.errors for form in formset):
                messages.error(request, "Revisá los campos de la venta.")
                item_rows = [
                    {
                        "form": form,
                        "variant_id": (post_data.get(f"{form.prefix}-variant_id") or "").strip(),
                    }
                    for form in formset.forms
                ]
                variant_data = {}
                for row in ProductVariant.objects.values("id", "product_id", "name").order_by("name", "id"):
                    variant_data.setdefault(str(row["product_id"]), []).append({"id": row["id"], "name": row["name"]})
                return render(
                    request,
                    "inventory/sale_edit.html",
                    {
                        "sale": sale,
                        "form": header_form,
                        "formset": formset,
                        "item_rows": item_rows,
                        "customer_audiences": customer_audiences,
                        "variant_data": variant_data,
                    },
                )
            if not items:
                messages.error(request, "Agregá al menos un producto.")
                return redirect("inventory_sale_edit", sale_id=sale.id)
            comun_wh = Warehouse.objects.filter(type=Warehouse.WarehouseType.COMUN).first()
            audience = header_form.cleaned_data.get("audiencia") or Customer.Audience.CONSUMER
            customer = header_form.cleaned_data.get("cliente")
            total_venta = header_form.cleaned_data.get("total_venta")
            sale_date = header_form.cleaned_data.get("sale_date")
            extra_discount_percent = header_form.cleaned_data.get("descuento_total") or Decimal("0.00")
            comision_ml = header_form.cleaned_data.get("comision_ml") or Decimal("0.00")
            impuestos_ml = header_form.cleaned_data.get("impuestos_ml") or Decimal("0.00")
            delivery_status = header_form.cleaned_data.get("delivery_status") or Sale.DeliveryStatus.NOT_DELIVERED
            if warehouse.type == Warehouse.WarehouseType.MERCADOLIBRE:
                customer = None
                audience = Customer.Audience.CONSUMER
                delivery_status = Sale.DeliveryStatus.NOT_DELIVERED
            elif customer:
                audience = customer.audience
            try:
                with transaction.atomic():
                    is_ml_sale = (
                        sale.ml_order_id
                        or sale.reference.startswith("ML ORDER")
                        or sale.reference.startswith("GS ORDER")
                        or warehouse.type == Warehouse.WarehouseType.MERCADOLIBRE
                    )
                    previous_items = list(sale.items.select_related("variant", "product"))
                    if sale.warehouse.type == Warehouse.WarehouseType.COMUN:
                        for prev_item in previous_items:
                            if prev_item.variant_id:
                                variant = (
                                    ProductVariant.objects.select_for_update()
                                    .filter(id=prev_item.variant_id, product=prev_item.product)
                                    .first()
                                )
                                if variant:
                                    variant.quantity = (variant.quantity + prev_item.quantity).quantize(Decimal("0.01"))
                                    variant.save(update_fields=["quantity"])
                                    if comun_wh:
                                        _koda_sync_common_with_variants(prev_item.product, comun_wh)
                    for movement in sale.movements.select_for_update():
                        if not is_ml_sale and movement.movement_type == StockMovement.MovementType.EXIT and movement.from_warehouse:
                            stock, _ = Stock.objects.select_for_update().get_or_create(
                                product=movement.product,
                                warehouse=movement.from_warehouse,
                                defaults={"quantity": Decimal("0.00")},
                            )
                            stock.quantity = (stock.quantity + movement.quantity).quantize(Decimal("0.01"))
                            stock.save(update_fields=["quantity"])
                        movement.delete()
                    sale.items.all().delete()

                    sale.customer = customer
                    sale.warehouse = warehouse
                    sale.audience = audience
                    sale.delivery_status = delivery_status
                    sale.ml_commission_total = (
                        comision_ml if warehouse.type == Warehouse.WarehouseType.MERCADOLIBRE else Decimal("0.00")
                    )
                    sale.ml_tax_total = (
                        impuestos_ml if warehouse.type == Warehouse.WarehouseType.MERCADOLIBRE else Decimal("0.00")
                    )
                    update_fields = [
                        "customer",
                        "warehouse",
                        "audience",
                        "delivery_status",
                        "ml_commission_total",
                        "ml_tax_total",
                    ]
                    if sale_date:
                        created_at = datetime.combine(sale_date, time(12, 0))
                        if timezone.is_naive(created_at):
                            created_at = timezone.make_aware(created_at)
                        sale.created_at = created_at
                        update_fields.append("created_at")
                    sale.save(update_fields=update_fields)

                    total = Decimal("0.00")
                    discount_total = Decimal("0.00")
                    base_subtotal = Decimal("0.00")
                    for data in items:
                        base_price, discount, custom_cost = _resolve_sale_item_pricing(
                            product=data["product"],
                            audience=audience,
                            customer=customer,
                            requested_discount=data.get("discount_percent"),
                        )
                        manual_unit_price = data.get("unit_price_override")
                        if manual_unit_price is not None:
                            base_price = manual_unit_price
                        if discount < 0 or discount > 100:
                            raise ValueError("Descuento por ítem inválido (0 a 100).")
                        vat_value = data.get("vat_percent") or Decimal("0.00")
                        if vat_value < 0 or vat_value > 100:
                            raise ValueError("IVA % inválido (0 a 100).")
                        final_price = base_price * (Decimal("1.00") - discount / Decimal("100.00"))
                        qty = Decimal(data["quantity"])
                        line_total = (qty * final_price).quantize(Decimal("0.01"))
                        discount_amount = (qty * (base_price - final_price)).quantize(Decimal("0.01"))
                        base_subtotal += (qty * base_price).quantize(Decimal("0.01"))
                        variant = data.get("variant")
                        if warehouse.type == Warehouse.WarehouseType.COMUN and variant and not data["product"].is_kit:
                            variant = (
                                ProductVariant.objects.select_for_update()
                                .filter(id=variant.id, product=data["product"])
                                .first()
                            )
                            if variant:
                                variant.quantity = (variant.quantity - qty).quantize(Decimal("0.01"))
                                variant.save(update_fields=["quantity"])
                                if comun_wh:
                                    _koda_sync_common_with_variants(data["product"], comun_wh)
                        default_cost = data["product"].cost_with_vat()
                        manual_cost = data.get("cost_unit_override")
                        cost_unit = (
                            manual_cost
                            if manual_cost is not None
                            else (custom_cost if custom_cost is not None else default_cost)
                        )
                        SaleItem.objects.create(
                            sale=sale,
                            product=data["product"],
                            variant=variant,
                            quantity=qty,
                            unit_price=base_price,
                            cost_unit=cost_unit,
                            discount_percent=discount,
                            final_unit_price=final_price,
                            line_total=line_total,
                            vat_percent=vat_value,
                        )
                        total += line_total
                        discount_total += discount_amount
                        if warehouse.type != Warehouse.WarehouseType.MERCADOLIBRE:
                            if data["product"].is_kit:
                                for component in KitComponent.objects.select_related("component").filter(kit=data["product"]):
                                    services.register_exit(
                                        product=component.component,
                                        warehouse=warehouse,
                                        quantity=(qty * component.quantity),
                                        user=request.user,
                                        reference=f"Venta kit {audience} #{sale.id}",
                                        sale_price=final_price,
                                        vat_percent=vat_value,
                                        sale=sale,
                                        allow_negative=True,
                                    )
                            else:
                                services.register_exit(
                                    product=data["product"],
                                    warehouse=warehouse,
                                    quantity=data["quantity"],
                                    user=request.user,
                                    reference=f"Venta {audience} #{sale.id}",
                                    sale_price=final_price,
                                    vat_percent=vat_value,
                                    sale=sale,
                                    allow_negative=True,
                                )
                    gross_total = (
                        total_venta
                        if warehouse.type == Warehouse.WarehouseType.MERCADOLIBRE and total_venta is not None
                        else total
                    )
                    discount_base = total_venta if total_venta is not None else base_subtotal
                    extra_discount_amount = (discount_base * extra_discount_percent / Decimal("100.00")).quantize(Decimal("0.01"))
                    sale.total = (gross_total - extra_discount_amount).quantize(Decimal("0.01"))
                    sale.discount_total = (discount_total + extra_discount_amount).quantize(Decimal("0.01"))
                    sale.save(update_fields=["total", "discount_total"])
                messages.success(request, "Venta actualizada.")
                if warehouse.type == Warehouse.WarehouseType.MERCADOLIBRE:
                    return redirect("inventory_sales_list")
                return redirect("inventory_sale_receipt", sale_id=sale.id)
            except services.NegativeStockError:
                messages.error(request, "No se puede actualizar: el stock quedaría negativo.")
            except services.InvalidMovementError as exc:
                messages.error(request, str(exc))
            except Exception as exc:
                messages.error(request, f"No se pudo actualizar la venta: {exc}")
        else:
            messages.error(request, "Revisá los campos de la venta.")
            item_rows = [
                {
                    "form": form,
                    "variant_id": (post_data.get(f"{form.prefix}-variant_id") or "").strip(),
                }
                for form in formset.forms
            ]
            variant_data = {}
            for row in ProductVariant.objects.values("id", "product_id", "name").order_by("name", "id"):
                variant_data.setdefault(str(row["product_id"]), []).append({"id": row["id"], "name": row["name"]})
            return render(
                request,
                "inventory/sale_edit.html",
                {
                    "sale": sale,
                    "form": header_form,
                    "formset": formset,
                    "item_rows": item_rows,
                    "customer_audiences": customer_audiences,
                    "variant_data": variant_data,
                },
            )
        return redirect("inventory_sale_edit", sale_id=sale.id)
    else:
        items_for_discount = list(sale.items.all())
        subtotal_base = sum((item.unit_price * item.quantity for item in items_for_discount), Decimal("0.00"))
        per_item_discount = sum(
            ((item.unit_price * item.quantity) - item.line_total for item in items_for_discount),
            Decimal("0.00"),
        )
        extra_discount_amount = (sale.discount_total or Decimal("0.00")) - per_item_discount
        if extra_discount_amount < 0:
            extra_discount_amount = Decimal("0.00")
        discount_percent = (
            (extra_discount_amount / subtotal_base * Decimal("100.00")).quantize(Decimal("0.01"))
            if subtotal_base > 0
            else Decimal("0.00")
        )
        header_form = SaleHeaderForm(
            initial={
                "warehouse": sale.warehouse,
                "sale_date": timezone.localtime(sale.created_at).date() if sale.created_at else None,
                "audiencia": sale.audience,
                "cliente": sale.customer,
                "delivery_status": sale.delivery_status,
                "total_venta": sale.total if sale.warehouse.type == Warehouse.WarehouseType.MERCADOLIBRE else None,
                "descuento_total": discount_percent,
                "comision_ml": sale.ml_commission_total or Decimal("0.00"),
                "impuestos_ml": sale.ml_tax_total or Decimal("0.00"),
            }
        )
        items = list(sale.items.select_related("variant", "product"))
        initial = [
            {
                "product": item.product,
                "quantity": int(item.quantity),
                "unit_price_override": item.unit_price,
                "cost_unit_override": item.cost_unit,
                "discount_percent": item.discount_percent,
                "vat_percent": item.vat_percent,
            }
            for item in items
        ]
        formset = SaleItemFormSet(initial=initial)
        item_rows = [
            {"form": form, "variant_id": item.variant_id}
            for form, item in zip(formset.forms, items)
        ]

    variant_data = {}
    for row in ProductVariant.objects.values("id", "product_id", "name").order_by("name", "id"):
        variant_data.setdefault(str(row["product_id"]), []).append({"id": row["id"], "name": row["name"]})

    return render(
        request,
        "inventory/sale_edit.html",
        {
            "sale": sale,
            "form": header_form,
            "formset": formset,
            "item_rows": item_rows,
            "customer_audiences": customer_audiences,
            "variant_data": variant_data,
        },
    )


@login_required
def sales_list(request):
    SaleItemFormSet = formset_factory(SaleItemForm, extra=1, can_delete=False)
    default_wh = Warehouse.objects.filter(type=Warehouse.WarehouseType.COMUN).first()
    if not default_wh:
        default_wh = Warehouse.objects.first()
    customer_audiences = {
        str(customer.id): customer.audience
        for customer in Customer.objects.only("id", "audience")
    }
    search_query = (request.GET.get("q") or "").strip()
    start_date_raw = (request.GET.get("start_date") or "").strip()
    end_date_raw = (request.GET.get("end_date") or "").strip()
    include_comun = request.GET.get("wh_comun") == "1"
    include_ml = request.GET.get("wh_ml") == "1"
    show_history = request.GET.get("show_history") == "1"
    customers = Customer.objects.order_by("name")
    action = request.POST.get("action") if request.method == "POST" else ""
    if action == "import_ml_sales_xlsx":
        upload = request.FILES.get("file")
        if not upload:
            messages.error(request, "Subí el archivo XLSX con las ventas.")
            return redirect("inventory_sales_list")
        rows, error = _read_ml_sales_xlsx_rows(upload)
        if error:
            messages.error(request, error)
            return redirect("inventory_sales_list")
        if not rows:
            messages.error(request, "El archivo no tiene filas válidas.")
            return redirect("inventory_sales_list")

        def parse_datetime(value: object) -> datetime | None:
            if value is None or value == "":
                return None
            if isinstance(value, datetime):
                parsed = value
            elif hasattr(value, "year") and hasattr(value, "month") and hasattr(value, "day"):
                parsed = datetime(value.year, value.month, value.day)
            else:
                raw = str(value).strip()
                for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d/%m/%Y %H:%M", "%d/%m/%Y"):
                    try:
                        parsed = datetime.strptime(raw, fmt)
                        break
                    except ValueError:
                        parsed = None
                if parsed is None:
                    return None
            if timezone.is_naive(parsed):
                return timezone.make_aware(parsed)
            return parsed

        ml_wh = Warehouse.objects.filter(type=Warehouse.WarehouseType.MERCADOLIBRE).first()
        if not ml_wh:
            messages.error(request, "Falta el depósito MercadoLibre.")
            return redirect("inventory_sales_list")

        ml_items = list(MercadoLibreItem.objects.select_related("product"))
        ml_title_index = {}
        for ml_item in ml_items:
            key = ml._normalize(ml_item.title or "")
            if key:
                ml_title_index.setdefault(key, []).append(ml_item)

        created_sales = 0
        skipped = 0
        unmatched = 0
        unmatched_refs: list[str] = []
        for idx, row in enumerate(rows, start=1):
            title = row["title"]
            qty = row["quantity"]
            price_total = row["price_total"]
            commission = row["commission"]
            taxes = row["taxes"]
            created_at = parse_datetime(row.get("created_at"))
            order_id = (row.get("order_id") or "").strip()

            product = None
            title_norm = ml._normalize(title)
            if title_norm in ml_title_index:
                product = ml_title_index[title_norm][0].product
            if not product and title_norm:
                for ml_item in ml_items:
                    item_norm = ml._normalize(ml_item.title or "")
                    if not item_norm:
                        continue
                    if title_norm in item_norm or item_norm in title_norm:
                        product = ml_item.product
                        if product:
                            break
            if not product:
                unmatched += 1
                ref_label = order_id or f"fila {idx}"
                unmatched_refs.append(f"{ref_label}: {title}")
                product = (
                    Product.objects.filter(name=title, group="MercadoLibre (sin match)").first()
                    or Product.objects.create(
                        name=title,
                        group="MercadoLibre (sin match)",
                        avg_cost=Decimal("0.00"),
                        vat_percent=Decimal("0.00"),
                    )
                )

            if order_id:
                reference = f"XLSX ML {order_id}"
                if Sale.objects.filter(ml_order_id=order_id).exists():
                    skipped += 1
                    continue
            else:
                reference = f"XLSX ML {created_at.date() if created_at else 'SIN-FECHA'} #{idx}"
                if Sale.objects.filter(reference=reference).exists():
                    skipped += 1
                    continue

            with transaction.atomic():
                sale = Sale.objects.create(
                    warehouse=ml_wh,
                    audience=Customer.Audience.CONSUMER,
                    total=price_total.quantize(Decimal("0.01")),
                    reference=reference,
                    ml_order_id=order_id,
                    ml_commission_total=commission.quantize(Decimal("0.01")),
                    ml_tax_total=taxes.quantize(Decimal("0.01")),
                    user=request.user,
                )
                if created_at:
                    Sale.objects.filter(pk=sale.pk).update(created_at=created_at)
                unit_price = (price_total / qty).quantize(Decimal("0.01"))
                line_total = (unit_price * qty).quantize(Decimal("0.01"))
                SaleItem.objects.create(
                    sale=sale,
                    product=product,
                    quantity=qty,
                    unit_price=unit_price,
                    cost_unit=product.last_purchase_cost(),
                    discount_percent=Decimal("0.00"),
                    final_unit_price=unit_price,
                    line_total=line_total,
                    vat_percent=product.vat_percent or Decimal("0.00"),
                )
                created_sales += 1

        messages.success(
            request,
            f"Ventas importadas: {created_sales}, omitidas: {skipped}, sin match: {unmatched}.",
        )
        if unmatched_refs:
            preview = ", ".join(unmatched_refs[:10])
            more = "" if len(unmatched_refs) <= 10 else f" (+{len(unmatched_refs) - 10} más)"
            messages.warning(
                request,
                "Sin match en comprobantes: "
                f"{preview}{more}. Se registraron con el nombre del XLSX. Editá la venta para asignar el producto.",
            )
        return redirect("inventory_sales_list")
    if action in {"sync_google_sales", "reset_google_sales"}:
        if action == "reset_google_sales":
            ml_sales = Sale.objects.filter(
                Q(reference__startswith="ML ORDER ")
                | Q(reference__startswith="GS ORDER ")
                | Q(ml_order_id__gt="")
            )
            StockMovement.objects.filter(sale__in=ml_sales).delete()
            deleted_count = ml_sales.count()
            ml_sales.delete()
            messages.info(request, f"Ventas ML eliminadas: {deleted_count}.")
        sheet_url = (request.POST.get("sheet_url") or "").strip()
        if not sheet_url:
            sheet_url = os.environ.get("GOOGLE_SHEETS_SALES_URL", "")
        if not sheet_url:
            messages.error(request, "Falta GOOGLE_SHEETS_SALES_URL en el entorno.")
            return redirect("inventory_sales_list")

        def normalize_header(value: str) -> str:
            value = (value or "").strip().lower()
            value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
            return re.sub(r"\s+", "", value)

        def parse_decimal(value: str) -> Decimal:
            raw = (value or "").strip()
            raw = raw.replace("$", "").replace(" ", "")
            if not raw:
                return Decimal("0.00")
            if "," in raw and "." in raw:
                raw = raw.replace(".", "").replace(",", ".")
            elif "," in raw:
                raw = raw.replace(",", ".")
            try:
                return Decimal(raw)
            except Exception:
                return Decimal("0.00")

        def parse_datetime_gs(value: str | None) -> datetime | None:
            if not value:
                return None
            raw = value.strip()
            match = re.match(r"^(.*)([+-]\d{2})(\d{2})$", raw)
            if match:
                raw = f"{match.group(1)}{match.group(2)}:{match.group(3)}"
            raw = raw.replace("Z", "+00:00")
            try:
                parsed = datetime.fromisoformat(raw)
            except ValueError:
                return None
            if timezone.is_naive(parsed):
                return timezone.make_aware(parsed)
            return parsed

        from urllib.request import urlopen

        match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", sheet_url)
        gid_match = re.search(r"[?&]gid=(\d+)", sheet_url)
        if not match or not gid_match:
            messages.error(request, "URL de Google Sheets inválida (falta ID o gid).")
            return redirect("inventory_sales_list")

        sheet_id = match.group(1)
        gid = gid_match.group(1)
        csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"

        try:
            with urlopen(csv_url, timeout=30) as resp:
                content = resp.read().decode("utf-8", errors="ignore")
        except Exception as exc:
            messages.error(request, f"No se pudo leer la hoja: {exc}")
            return redirect("inventory_sales_list")

        reader = csv.reader(io.StringIO(content))
        rows = list(reader)
        if not rows:
            messages.error(request, "La hoja no tiene datos.")
            return redirect("inventory_sales_list")

        headers = [normalize_header(h) for h in rows[0]]

        def idx(*names: str) -> int | None:
            for name in names:
                key = normalize_header(name)
                if key in headers:
                    return headers.index(key)
            return None

        fecha_idx = idx("Fecha")
        idventa_idx = idx("IDVenta", "ID Vent", "IDVenta")
        idorder_idx = idx("IDOrder", "ID Order", "IDOrden", "ID Or", "IDor")
        producto_idx = idx("Producto")
        cantidad_idx = idx("Cantidad")
        precio_idx = idx("Precio Bruto Venta", "Precio Bruto", "Precio")
        comision_idx = idx("Comision", "Comisión")
        iibb_idx = idx("IIBB", "Impuestos", "Impuesto")

        required = [fecha_idx, idventa_idx, producto_idx, cantidad_idx, precio_idx, comision_idx, iibb_idx]
        if any(i is None for i in required):
            messages.error(
                request,
                "Faltan columnas requeridas en la hoja (Fecha, IDVenta, Producto, Cantidad, Precio Bruto Venta, Comision, Impuestos).",
            )
            return redirect("inventory_sales_list")

        ml_items = list(MercadoLibreItem.objects.select_related("product"))
        ml_title_index = {}
        for ml_item in ml_items:
            key = ml._normalize(ml_item.title or "")
            if key:
                ml_title_index.setdefault(key, []).append(ml_item)
        orders: dict[str, dict[str, object]] = {}
        unmatched = 0

        for row in rows[1:]:
            if not row:
                continue
            if len(row) < len(headers):
                row = row + [""] * (len(headers) - len(row))
            order_id = (row[idorder_idx] if idorder_idx is not None else "") or row[idventa_idx]
            order_id = str(order_id).strip()
            if not order_id:
                continue
            title = str(row[producto_idx]).strip()
            qty = parse_decimal(row[cantidad_idx])
            if qty <= 0:
                continue
            price_total = parse_decimal(row[precio_idx])
            commission = parse_decimal(row[comision_idx])
            iibb = parse_decimal(row[iibb_idx])
            created_at = parse_datetime_gs(row[fecha_idx])

            product = None
            title_norm = ml._normalize(title)
            if title_norm in ml_title_index:
                product = ml_title_index[title_norm][0].product
            if not product and title_norm:
                for ml_item in ml_items:
                    item_norm = ml._normalize(ml_item.title or "")
                    if not item_norm:
                        continue
                    if title_norm in item_norm or item_norm in title_norm:
                        product = ml_item.product
                        if product:
                            break
            if not product:
                unmatched += 1
                continue

            entry = orders.setdefault(
                order_id,
                {
                    "created_at": created_at,
                    "total": Decimal("0.00"),
                    "commission": Decimal("0.00"),
                    "iibb": Decimal("0.00"),
                    "items": [],
                },
            )
            entry["total"] += price_total
            entry["commission"] += commission
            entry["iibb"] += iibb
            unit_price = (price_total / qty).quantize(Decimal("0.01"))
            entry["items"].append(
                {
                    "product": product,
                    "quantity": qty,
                    "unit_price": unit_price,
                }
            )
            if created_at and (entry["created_at"] is None or created_at < entry["created_at"]):
                entry["created_at"] = created_at

        created_sales = 0
        skipped = 0
        ml_wh = Warehouse.objects.filter(type=Warehouse.WarehouseType.MERCADOLIBRE).first()
        for order_id, data in orders.items():
            reference = f"GS ORDER {order_id}"
            if Sale.objects.filter(reference=reference).exists():
                skipped += 1
                continue
            if not data["items"]:
                continue
            if not ml_wh:
                messages.error(request, "Falta el depósito MercadoLibre.")
                return redirect("inventory_sales_list")
            with transaction.atomic():
                sale = Sale.objects.create(
                    warehouse=ml_wh,
                    audience=Customer.Audience.CONSUMER,
                    total=data["total"].quantize(Decimal("0.01")),
                    reference=reference,
                    ml_order_id=str(order_id),
                    ml_commission_total=data["commission"].quantize(Decimal("0.01")),
                    ml_tax_total=data["iibb"].quantize(Decimal("0.01")),
                    user=request.user,
                )
                if data["created_at"]:
                    Sale.objects.filter(pk=sale.pk).update(created_at=data["created_at"])
                for item in data["items"]:
                    qty = item["quantity"]
                    unit_price = item["unit_price"]
                    line_total = (unit_price * qty).quantize(Decimal("0.01"))
                    SaleItem.objects.create(
                        sale=sale,
                        product=item["product"],
                        quantity=qty,
                        unit_price=unit_price,
                        cost_unit=item["product"].last_purchase_cost(),
                        discount_percent=Decimal("0.00"),
                        final_unit_price=unit_price,
                        line_total=line_total,
                        vat_percent=item["product"].vat_percent or Decimal("0.00"),
                    )
                created_sales += 1

        messages.success(
            request,
            f"Ventas importadas: {created_sales}, omitidas: {skipped}, sin match: {unmatched}.",
        )
        return redirect("inventory_sales_list")
    if action == "bulk_delete_sales":
        start_raw = (request.POST.get("bulk_start") or "").strip()
        end_raw = (request.POST.get("bulk_end") or "").strip()
        only_ml = request.POST.get("bulk_only_ml") == "1"
        if not start_raw and not end_raw and not only_ml:
            messages.error(request, "Indicá un rango de fechas o seleccioná solo MercadoLibre.")
            return redirect("inventory_sales_list")

        sales_qs = Sale.objects.all()
        if only_ml:
            sales_qs = sales_qs.filter(
                Q(reference__startswith="ML ORDER ")
                | Q(reference__startswith="GS ORDER ")
                | Q(ml_order_id__gt="")
            )
        if start_raw:
            try:
                start_dt = datetime.fromisoformat(start_raw)
                if timezone.is_naive(start_dt):
                    start_dt = timezone.make_aware(start_dt)
                sales_qs = sales_qs.filter(created_at__gte=start_dt)
            except ValueError:
                messages.error(request, "Fecha desde inválida.")
                return redirect("inventory_sales_list")
        if end_raw:
            try:
                end_dt = datetime.fromisoformat(end_raw)
                if timezone.is_naive(end_dt):
                    end_dt = timezone.make_aware(end_dt)
                sales_qs = sales_qs.filter(created_at__lte=end_dt)
            except ValueError:
                messages.error(request, "Fecha hasta inválida.")
                return redirect("inventory_sales_list")

        deleted_ids = list(sales_qs.values_list("id", flat=True))
        StockMovement.objects.filter(sale_id__in=deleted_ids).delete()
        deleted = len(deleted_ids)
        Sale.objects.filter(id__in=deleted_ids).delete()
        messages.success(request, f"Ventas eliminadas: {deleted}.")
        return redirect("inventory_sales_list")
    if action == "bulk_delete_selected":
        ids = request.POST.getlist("sale_ids")
        if not ids:
            messages.error(request, "No seleccionaste ventas para eliminar.")
            return redirect("inventory_sales_list")
        sales = list(
            Sale.objects.filter(id__in=ids).prefetch_related("items", "movements")
        )
        deleted = 0
        with transaction.atomic():
            for sale in sales:
                is_ml_sale = sale.ml_order_id or sale.reference.startswith("ML ORDER") or sale.reference.startswith("GS ORDER")
                comun_wh = Warehouse.objects.filter(type=Warehouse.WarehouseType.COMUN).first()
                if sale.warehouse.type == Warehouse.WarehouseType.COMUN:
                    for item in sale.items.select_related("variant", "product"):
                        if item.variant_id:
                            variant = (
                                ProductVariant.objects.select_for_update()
                                .filter(id=item.variant_id, product=item.product)
                                .first()
                            )
                            if variant:
                                variant.quantity = (variant.quantity + item.quantity).quantize(Decimal("0.01"))
                                variant.save(update_fields=["quantity"])
                                if comun_wh:
                                    _koda_sync_common_with_variants(item.product, comun_wh)
                for movement in sale.movements.select_for_update():
                    if not is_ml_sale and movement.movement_type == StockMovement.MovementType.EXIT and movement.from_warehouse:
                        stock, _ = Stock.objects.select_for_update().get_or_create(
                            product=movement.product,
                            warehouse=movement.from_warehouse,
                            defaults={"quantity": Decimal("0.00")},
                        )
                        stock.quantity = (stock.quantity + movement.quantity).quantize(Decimal("0.01"))
                        stock.save(update_fields=["quantity"])
                    movement.delete()
                sale.delete()
                deleted += 1
        messages.success(request, f"Ventas eliminadas: {deleted}.")
        return redirect("inventory_sales_list")
    if request.method == "POST":
        post_data = request.POST.copy()
        for key in list(post_data.keys()):
            if not key.endswith("-product_text"):
                continue
            prefix = key[:-len("product_text")]
            product_key = f"{prefix}product"
            if post_data.get(product_key):
                continue
            product_text = (post_data.get(key) or "").strip()
            if not product_text:
                continue
            label = product_text.split(" (", 1)[0].strip()
            sku_candidate = ""
            name_candidate = label
            if " - " in label:
                sku_candidate, name_candidate = [part.strip() for part in label.split(" - ", 1)]
            product = None
            if sku_candidate and sku_candidate.lower() != "sin sku":
                product = Product.objects.filter(sku__iexact=sku_candidate).first()
            if not product and name_candidate:
                product = (
                    Product.objects.filter(name__iexact=name_candidate).first()
                    or Product.objects.filter(name__icontains=name_candidate).first()
                )
            if product:
                post_data[product_key] = str(product.id)
        header_form = SaleHeaderForm(post_data)
        formset = SaleItemFormSet(post_data)
        for form in formset.forms:
            prefix = form.prefix
            product_raw = (post_data.get(f"{prefix}-product") or "").strip()
            product_text = (post_data.get(f"{prefix}-product_text") or "").strip()
            qty_raw = (post_data.get(f"{prefix}-quantity") or "").strip()
            qty_value = None
            if qty_raw:
                try:
                    qty_value = Decimal(str(qty_raw).replace(",", "."))
                except Exception:
                    qty_value = None
            qty_zeroish = qty_raw in {"", "0", "0.0", "0.00", "0,00"} or (qty_value is not None and qty_value <= 0)
            if not product_raw and not product_text and qty_zeroish:
                form.empty_permitted = True
            elif not form.has_changed():
                form.empty_permitted = True
        if header_form.is_valid() and formset.is_valid():
            warehouse = header_form.cleaned_data["warehouse"]
            require_variants = warehouse.type == Warehouse.WarehouseType.COMUN
            audience = header_form.cleaned_data.get("audiencia") or Customer.Audience.CONSUMER
            customer = header_form.cleaned_data.get("cliente")
            total_venta = header_form.cleaned_data.get("total_venta")
            sale_date = header_form.cleaned_data.get("sale_date")
            extra_discount_percent = header_form.cleaned_data.get("descuento_total") or Decimal("0.00")
            comision_ml = header_form.cleaned_data.get("comision_ml") or Decimal("0.00")
            impuestos_ml = header_form.cleaned_data.get("impuestos_ml") or Decimal("0.00")
            delivery_status = header_form.cleaned_data.get("delivery_status") or Sale.DeliveryStatus.NOT_DELIVERED
            if warehouse.type == Warehouse.WarehouseType.MERCADOLIBRE:
                customer = None
                audience = Customer.Audience.CONSUMER
                delivery_status = Sale.DeliveryStatus.NOT_DELIVERED
            elif customer:
                audience = customer.audience
            items = []
            for form in formset.forms:
                if form.cleaned_data and form.cleaned_data.get("DELETE"):
                    continue
                product = form.cleaned_data.get("product") if form.cleaned_data else None
                if not product:
                    continue
                variant_id_raw = (post_data.get(f"{form.prefix}-variant_id") or "").strip()
                variant = None
                if variant_id_raw:
                    variant = ProductVariant.objects.filter(id=variant_id_raw, product=product).first()
                    if not variant:
                        form.add_error(None, "Variedad inválida.")
                if require_variants and ProductVariant.objects.filter(product=product).exists() and not variant:
                    form.add_error(None, "Seleccioná una variedad.")
                items.append({**form.cleaned_data, "variant": variant})
            if any(form.errors for form in formset):
                messages.error(request, "Revisá los campos de la venta.")
                return redirect("inventory_sales_list")
            if not items:
                messages.error(request, "Agregá al menos un producto.")
            else:
                try:
                    with transaction.atomic():
                        comun_wh = Warehouse.objects.filter(type=Warehouse.WarehouseType.COMUN).first()
                        sale = Sale.objects.create(
                            customer=customer,
                            warehouse=warehouse,
                            audience=audience,
                            delivery_status=delivery_status,
                            reference=f"Venta {audience}",
                            user=request.user,
                            ml_commission_total=comision_ml if warehouse.type == Warehouse.WarehouseType.MERCADOLIBRE else Decimal("0.00"),
                            ml_tax_total=impuestos_ml if warehouse.type == Warehouse.WarehouseType.MERCADOLIBRE else Decimal("0.00"),
                        )
                        if sale_date:
                            created_at = datetime.combine(sale_date, time(12, 0))
                            if timezone.is_naive(created_at):
                                created_at = timezone.make_aware(created_at)
                            Sale.objects.filter(pk=sale.pk).update(created_at=created_at)
                        if sale_date:
                            created_at = datetime.combine(sale_date, time(12, 0))
                            if timezone.is_naive(created_at):
                                created_at = timezone.make_aware(created_at)
                            Sale.objects.filter(pk=sale.pk).update(created_at=created_at)
                        total = Decimal("0.00")
                        discount_total = Decimal("0.00")
                        base_subtotal = Decimal("0.00")
                        for data in items:
                            base_price, discount, custom_cost = _resolve_sale_item_pricing(
                                product=data["product"],
                                audience=audience,
                                customer=customer,
                                requested_discount=None,
                            )
                            manual_unit_price = data.get("unit_price_override")
                            if manual_unit_price is not None:
                                base_price = manual_unit_price
                            if discount < 0 or discount > 100:
                                raise ValueError("Descuento por ítem inválido (0 a 100).")
                            vat_value = data.get("vat_percent") or Decimal("0.00")
                            if vat_value < 0 or vat_value > 100:
                                raise ValueError("IVA % inválido (0 a 100).")
                            final_price = base_price * (Decimal("1.00") - discount / Decimal("100.00"))
                            qty = Decimal(data["quantity"])
                            line_total = (qty * final_price).quantize(Decimal("0.01"))
                            discount_amount = (qty * (base_price - final_price)).quantize(Decimal("0.01"))
                            base_subtotal += (qty * base_price).quantize(Decimal("0.01"))
                            variant = data.get("variant")
                            if warehouse.type == Warehouse.WarehouseType.COMUN and variant and not data["product"].is_kit:
                                variant = (
                                    ProductVariant.objects.select_for_update()
                                    .filter(id=variant.id, product=data["product"])
                                    .first()
                                )
                                if variant:
                                    variant.quantity = (variant.quantity - qty).quantize(Decimal("0.01"))
                                    variant.save(update_fields=["quantity"])
                                    if comun_wh:
                                        _koda_sync_common_with_variants(data["product"], comun_wh)
                            default_cost = data["product"].cost_with_vat()
                            manual_cost = data.get("cost_unit_override")
                            cost_unit = (
                                manual_cost
                                if manual_cost is not None
                                else (custom_cost if custom_cost is not None else default_cost)
                            )
                            SaleItem.objects.create(
                                sale=sale,
                                product=data["product"],
                                variant=variant,
                                quantity=qty,
                                unit_price=base_price,
                                cost_unit=cost_unit,
                                discount_percent=discount,
                                final_unit_price=final_price,
                                line_total=line_total,
                                vat_percent=vat_value,
                            )
                            total += line_total
                            discount_total += discount_amount
                            if warehouse.type != Warehouse.WarehouseType.MERCADOLIBRE:
                                if data["product"].is_kit:
                                    for component in KitComponent.objects.select_related("component").filter(kit=data["product"]):
                                        services.register_exit(
                                            product=component.component,
                                            warehouse=warehouse,
                                            quantity=(qty * component.quantity),
                                            user=request.user,
                                            reference=f"Venta kit {audience} #{sale.id}",
                                            sale_price=final_price,
                                            vat_percent=vat_value,
                                            sale=sale,
                                            allow_negative=True,
                                        )
                                else:
                                    services.register_exit(
                                        product=data["product"],
                                        warehouse=warehouse,
                                        quantity=data["quantity"],
                                        user=request.user,
                                        reference=f"Venta {audience} #{sale.id}",
                                        sale_price=final_price,
                                        vat_percent=vat_value,
                                        sale=sale,
                                        allow_negative=True,
                                    )
                        gross_total = (
                            total_venta
                            if warehouse.type == Warehouse.WarehouseType.MERCADOLIBRE and total_venta is not None
                            else total
                        )
                        discount_base = total_venta if total_venta is not None else base_subtotal
                        extra_discount_amount = (discount_base * extra_discount_percent / Decimal("100.00")).quantize(Decimal("0.01"))
                        sale.total = (gross_total - extra_discount_amount).quantize(Decimal("0.01"))
                        sale.discount_total = (discount_total + extra_discount_amount).quantize(Decimal("0.01"))
                        sale.save(update_fields=["total", "discount_total"])
                    messages.success(request, "Venta registrada.")
                    if warehouse.type == Warehouse.WarehouseType.MERCADOLIBRE:
                        return redirect("inventory_sales_list")
                    return redirect("inventory_sale_receipt", sale_id=sale.id)
                except services.NegativeStockError:
                    messages.error(request, "No hay stock suficiente para completar la venta.")
                except services.InvalidMovementError as exc:
                    messages.error(request, str(exc))
        else:
            messages.error(request, "Revisá los campos de la venta.")
    else:
        header_initial = {"sale_date": timezone.localdate()}
        if default_wh:
            header_initial["warehouse"] = default_wh
        header_form = SaleHeaderForm(initial=header_initial)
        formset = SaleItemFormSet()
    if show_history:
        sales = (
            Sale.objects.select_related("customer", "warehouse", "user")
            .prefetch_related("items__product")
            .order_by("-created_at", "-id")
        )
        if start_date_raw:
            try:
                start_dt = datetime.fromisoformat(start_date_raw)
                if timezone.is_naive(start_dt):
                    start_dt = timezone.make_aware(datetime.combine(start_dt.date(), time.min))
                else:
                    start_dt = datetime.combine(start_dt.date(), time.min, tzinfo=start_dt.tzinfo)
                sales = sales.filter(created_at__gte=start_dt)
            except ValueError:
                messages.error(request, "Fecha desde inválida.")
                return redirect("inventory_sales_list")
        if end_date_raw:
            try:
                end_dt = datetime.fromisoformat(end_date_raw)
                if timezone.is_naive(end_dt):
                    end_dt = timezone.make_aware(datetime.combine(end_dt.date(), time.max))
                else:
                    end_dt = datetime.combine(end_dt.date(), time.max, tzinfo=end_dt.tzinfo)
                sales = sales.filter(created_at__lte=end_dt)
            except ValueError:
                messages.error(request, "Fecha hasta inválida.")
                return redirect("inventory_sales_list")
        if search_query:
            sales = sales.filter(
                Q(customer__name__icontains=search_query)
                | Q(items__product__name__icontains=search_query)
                | Q(items__product__sku__icontains=search_query)
            ).distinct()
        if include_comun and not include_ml:
            sales = sales.filter(warehouse__type=Warehouse.WarehouseType.COMUN)
        elif include_ml and not include_comun:
            sales = sales.filter(warehouse__type=Warehouse.WarehouseType.MERCADOLIBRE)
        page_number = request.GET.get("page")
        paginator = Paginator(sales, 25)
        page_obj = paginator.get_page(page_number)
        sales_list_qs = list(page_obj.object_list)
        for sale in sales_list_qs:
            cost_total = Decimal("0.00")
            for item in sale.items.all():
                cost_unit = item.cost_unit
                if cost_unit is None or cost_unit <= 0:
                    cost_unit = item.product.cost_with_vat()
                cost_total += item.quantity * cost_unit
            commission_total = sale.ml_commission_total or Decimal("0.00")
            tax_total = sale.ml_tax_total or Decimal("0.00")
            is_ml_sale = (
                sale.ml_order_id
                or sale.reference.startswith("ML ORDER")
                or sale.reference.startswith("GS ORDER")
                or sale.warehouse.type == Warehouse.WarehouseType.MERCADOLIBRE
            )
            if is_ml_sale:
                net_total = (sale.total or Decimal("0.00")) - commission_total - tax_total
                sale.margin_total = net_total - cost_total
            else:
                sale.margin_total = (sale.total or Decimal("0.00")) - cost_total
        sales_comun = [sale for sale in sales_list_qs if sale.warehouse.type == Warehouse.WarehouseType.COMUN]
        sales_ml = [sale for sale in sales_list_qs if sale.warehouse.type == Warehouse.WarehouseType.MERCADOLIBRE]
    else:
        sales_list_qs = []
        sales_comun = []
        sales_ml = []
        page_obj = None
    show_comun = show_history and (include_comun or (not include_ml and not include_comun))
    show_ml = show_history and (include_ml or (not include_ml and not include_comun))
    delivery_status_choices = [
        (Sale.DeliveryStatus.NOT_DELIVERED, Sale.DeliveryStatus.NOT_DELIVERED.label),
        (Sale.DeliveryStatus.IN_TRANSIT, Sale.DeliveryStatus.IN_TRANSIT.label),
        (Sale.DeliveryStatus.DELIVERED, Sale.DeliveryStatus.DELIVERED.label),
    ]
    if request.GET.get("ajax") == "1":
        from django.template.loader import render_to_string

        html = render_to_string(
            "inventory/_sales_history.html",
            {
                "sales_comun": sales_comun,
                "sales_ml": sales_ml,
                "page_obj": page_obj,
                "search_query": search_query,
                "start_date": start_date_raw,
                "end_date": end_date_raw,
                "include_comun": include_comun,
                "include_ml": include_ml,
                "show_history": show_history,
                "show_comun": show_comun,
                "show_ml": show_ml,
                "delivery_status_choices": delivery_status_choices,
            },
            request=request,
        )
        return JsonResponse({"html": html})
    variant_data = {}
    for row in ProductVariant.objects.values("id", "product_id", "name").order_by("name", "id"):
        variant_data.setdefault(str(row["product_id"]), []).append({"id": row["id"], "name": row["name"]})
    return render(
        request,
        "inventory/sales_list.html",
        {
            "sales": sales_list_qs,
            "sales_comun": sales_comun,
            "sales_ml": sales_ml,
            "page_obj": page_obj,
            "search_query": search_query,
            "start_date": start_date_raw,
            "end_date": end_date_raw,
            "include_comun": include_comun,
            "include_ml": include_ml,
            "show_history": show_history,
            "show_comun": show_comun,
            "show_ml": show_ml,
            "delivery_status_choices": delivery_status_choices,
            "sales_sheet_url": os.environ.get("GOOGLE_SHEETS_SALES_URL", ""),
            "customers": customers,
            "form": header_form,
            "formset": formset,
            "customer_audiences": customer_audiences,
            "variant_data": variant_data,
            "clear_sale_form": request.method != "POST",
            "default_warehouse_id": str(default_wh.id) if default_wh else "",
        },
    )


@login_required
@require_http_methods(["POST"])
def sale_delivery_status_update(request, sale_id: int):
    sale = get_object_or_404(Sale, pk=sale_id)
    delivery_status = (request.POST.get("delivery_status") or "").strip()
    allowed_statuses = {
        Sale.DeliveryStatus.NOT_DELIVERED,
        Sale.DeliveryStatus.IN_TRANSIT,
        Sale.DeliveryStatus.DELIVERED,
    }
    if delivery_status not in allowed_statuses:
        return JsonResponse({"ok": False, "error": "Estado inválido."}, status=400)
    if sale.delivery_status != delivery_status:
        sale.delivery_status = delivery_status
        sale.save(update_fields=["delivery_status"])
    return JsonResponse(
        {
            "ok": True,
            "delivery_status": sale.delivery_status,
            "label": sale.get_delivery_status_display(),
        }
    )


@login_required
@require_http_methods(["POST"])
def sale_delete(request, sale_id: int):
    sale = get_object_or_404(Sale.objects.prefetch_related("items", "movements"), pk=sale_id)
    try:
        with transaction.atomic():
            is_ml_sale = sale.ml_order_id or sale.reference.startswith("ML ORDER") or sale.reference.startswith("GS ORDER")
            comun_wh = Warehouse.objects.filter(type=Warehouse.WarehouseType.COMUN).first()
            if sale.warehouse.type == Warehouse.WarehouseType.COMUN:
                for item in sale.items.select_related("variant", "product"):
                    if item.variant_id:
                        variant = (
                            ProductVariant.objects.select_for_update()
                            .filter(id=item.variant_id, product=item.product)
                            .first()
                        )
                        if variant:
                            variant.quantity = (variant.quantity + item.quantity).quantize(Decimal("0.01"))
                            variant.save(update_fields=["quantity"])
                            if comun_wh:
                                _koda_sync_common_with_variants(item.product, comun_wh)
            for movement in sale.movements.select_for_update():
                if not is_ml_sale and movement.movement_type == StockMovement.MovementType.EXIT and movement.from_warehouse:
                    stock, _ = Stock.objects.select_for_update().get_or_create(
                        product=movement.product,
                        warehouse=movement.from_warehouse,
                        defaults={"quantity": Decimal("0.00")},
                    )
                    stock.quantity = (stock.quantity + movement.quantity).quantize(Decimal("0.01"))
                    stock.save(update_fields=["quantity"])
                movement.delete()
            sale.delete()
        if is_ml_sale:
            messages.success(request, "Venta eliminada.")
        else:
            messages.success(request, "Venta eliminada y stock ajustado.")
    except Exception as exc:
        messages.error(request, f"No se pudo eliminar la venta: {exc}")
    return redirect("inventory_sales_list")
