"""Purchase views."""
import json
from datetime import datetime, time
from decimal import Decimal, ROUND_HALF_UP

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.files.base import ContentFile
from django.core.paginator import Paginator
from django.db import transaction
from django.forms import formset_factory
from django.http import HttpResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone
from django.views.decorators.http import require_http_methods

from .. import services
from ..models import (
    Product,
    ProductVariant,
    Purchase,
    PurchaseItem,
    Stock,
    StockMovement,
    Supplier,
    Warehouse,
)
from .common import (
    _apply_product_queryset_to_formset,
    _extract_product_ids_from_payload,
    _products_with_last_cost_queryset,
    _shipping_cost_per_unit,
)
from .forms import PurchaseHeaderForm, PurchaseItemForm
from .koda import _koda_sync_common_with_variants
from .utils_purchase_pdf import (
    _create_product_from_purchase_pdf,
    _extract_purchase_items_from_pdf_bytes,
    _normalize_purchase_pdf_item_fields,
    _resolve_product_from_purchase_pdf,
)


@login_required
def register_purchase(request):
    return purchases_list(request)


@login_required
def purchase_receipt(request, purchase_id: int):
    purchase = get_object_or_404(
        Purchase.objects.select_related("supplier", "warehouse").prefetch_related("items__product"), pk=purchase_id
    )
    items = list(purchase.items.all())
    has_vat = False
    subtotal_no_vat = Decimal("0.00")
    vat_total = Decimal("0.00")
    for item in items:
        discount_percent = item.discount_percent or Decimal("0.00")
        effective_unit_cost = (item.unit_cost * (Decimal("1.00") - (discount_percent / Decimal("100.00")))).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )
        item.discount_percent = discount_percent
        item.unit_cost_effective = effective_unit_cost
        vat_percent = item.vat_percent or Decimal("0.00")
        item.unit_cost_no_vat = effective_unit_cost
        if vat_percent > 0:
            has_vat = True
            item.unit_vat = (effective_unit_cost * (vat_percent / Decimal("100.00"))).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )
        else:
            item.unit_vat = Decimal("0.00")
        item.vat_percent = vat_percent
        item.unit_cost_with_vat = (item.unit_cost_no_vat + item.unit_vat).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        item.line_total = (item.quantity * item.unit_cost_with_vat).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        item.line_vat = (item.unit_vat * item.quantity).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        subtotal_no_vat += (item.unit_cost_no_vat * item.quantity).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        vat_total += item.line_vat
    subtotal = sum((item.line_total for item in items), Decimal("0.00"))
    discount_percent = purchase.discount_percent or Decimal("0.00")
    shipping_cost = purchase.shipping_cost or Decimal("0.00")
    subtotal_with_shipping = (subtotal + shipping_cost).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    discount_total = (subtotal_with_shipping * discount_percent / Decimal("100.00")).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )
    total = (subtotal_with_shipping - discount_total).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    if purchase.total != total:
        purchase.total = total
        purchase.save(update_fields=["total"])
    context = {
        "purchase": purchase,
        "items": items,
        "subtotal": subtotal,
        "subtotal_no_vat": subtotal_no_vat,
        "vat_total": vat_total,
        "shipping_cost": shipping_cost,
        "subtotal_with_shipping": subtotal_with_shipping,
        "discount_total": discount_total,
        "discount_percent": discount_percent,
        "total": total,
        "invoice_number": purchase.invoice_number,
        "has_vat": has_vat,
    }
    return render(request, "inventory/purchase_receipt.html", context)


@login_required
def purchase_receipt_pdf(request, purchase_id: int):
    purchase = get_object_or_404(
        Purchase.objects.select_related("supplier", "warehouse").prefetch_related("items__product", "items__variant"),
        pk=purchase_id,
    )
    items = list(purchase.items.all())
    has_vat = False
    subtotal_no_vat = Decimal("0.00")
    vat_total = Decimal("0.00")
    for item in items:
        discount_percent = item.discount_percent or Decimal("0.00")
        effective_unit_cost = (item.unit_cost * (Decimal("1.00") - (discount_percent / Decimal("100.00")))).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )
        item.discount_percent = discount_percent
        item.unit_cost_effective = effective_unit_cost
        vat_percent = item.vat_percent or Decimal("0.00")
        item.unit_cost_no_vat = effective_unit_cost
        if vat_percent > 0:
            has_vat = True
            item.unit_vat = (effective_unit_cost * (vat_percent / Decimal("100.00"))).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )
        else:
            item.unit_vat = Decimal("0.00")
        item.vat_percent = vat_percent
        item.unit_cost_with_vat = (item.unit_cost_no_vat + item.unit_vat).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        item.line_total = (item.quantity * item.unit_cost_with_vat).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        item.line_vat = (item.unit_vat * item.quantity).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        subtotal_no_vat += (item.unit_cost_no_vat * item.quantity).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        vat_total += item.line_vat
    subtotal = sum((item.line_total for item in items), Decimal("0.00"))
    discount_percent = purchase.discount_percent or Decimal("0.00")
    shipping_cost = purchase.shipping_cost or Decimal("0.00")
    subtotal_with_shipping = (subtotal + shipping_cost).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    discount_total = (subtotal_with_shipping * discount_percent / Decimal("100.00")).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )
    total = (subtotal_with_shipping - discount_total).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

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
        "inventory/purchase_receipt_pdf.html",
        {
            "purchase": purchase,
            "items": items,
            "pages": pages,
            "subtotal": subtotal,
            "subtotal_no_vat": subtotal_no_vat,
            "vat_total": vat_total,
            "shipping_cost": shipping_cost,
            "subtotal_with_shipping": subtotal_with_shipping,
            "discount_total": discount_total,
            "discount_percent": discount_percent,
            "total": total,
            "invoice_number": purchase.invoice_number,
            "has_vat": has_vat,
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
    invoice_raw = purchase.invoice_number
    if isinstance(invoice_raw, str) and "-" in invoice_raw:
        invoice_raw = invoice_raw.split("-", 1)[1]
    invoice_number = str(invoice_raw).lstrip("0") or str(purchase.id)
    response = HttpResponse(pdf_bytes, content_type="application/pdf")
    response["Content-Disposition"] = f'attachment; filename="COMP-{invoice_number}.pdf"'
    return response


@login_required
def purchases_list(request):
    PurchaseItemFormSet = formset_factory(PurchaseItemForm, extra=1, can_delete=True)
    show_history = request.GET.get("show_history") == "1"
    if request.method == "POST":
        if request.POST.get("action") == "import_purchase_pdf":
            pdf_upload = request.FILES.get("purchase_pdf")
            warehouse_id = (request.POST.get("pdf_warehouse_id") or "").strip()
            supplier_id = (request.POST.get("pdf_supplier_id") or "").strip()
            raw_date = (request.POST.get("pdf_purchase_date") or "").strip()
            warehouse = Warehouse.objects.filter(id=warehouse_id).first() if warehouse_id else None
            supplier = Supplier.objects.filter(id=supplier_id).first() if supplier_id else None
            if not pdf_upload:
                messages.error(request, "Subí un PDF para importar la compra.")
                return redirect("inventory_purchases_list")
            if not warehouse:
                messages.error(request, "Seleccioná un depósito para la compra importada.")
                return redirect("inventory_purchases_list")
            if not supplier:
                messages.error(request, "Seleccioná un proveedor para la compra importada.")
                return redirect("inventory_purchases_list")

            pdf_bytes = pdf_upload.read()
            parsed_items, metadata, parse_error = _extract_purchase_items_from_pdf_bytes(pdf_bytes)
            if parse_error:
                messages.error(request, parse_error)
                return redirect("inventory_purchases_list")

            unresolved: list[str] = []
            resolved_items: list[dict] = []
            auto_created_products = 0
            for entry in parsed_items:
                entry = _normalize_purchase_pdf_item_fields(entry)
                product = _resolve_product_from_purchase_pdf(entry.get("description") or "")
                if not product:
                    product = _create_product_from_purchase_pdf(
                        description=entry.get("description") or "",
                        supplier=supplier,
                        unit_cost_with_vat=entry.get("unit_cost"),
                        vat_percent=Decimal("0.00"),
                    )
                    if product:
                        auto_created_products += 1
                    else:
                        unresolved.append(entry.get("description") or "(sin descripción)")
                        continue
                variants = list(ProductVariant.objects.filter(product=product).order_by("id"))
                variant = None
                if len(variants) == 1:
                    variant = variants[0]
                elif len(variants) > 1:
                    unresolved.append(f"{entry.get('description') or product.name} (requiere variedad)")
                    continue
                resolved_items.append(
                    {
                        "product": product,
                        "variant": variant,
                        "quantity": entry["quantity"],
                        "unit_cost": entry["unit_cost"],
                        "discount_percent": entry.get("discount_percent") or Decimal("0.00"),
                        "vat_percent": Decimal("0.00"),
                        "supplier": supplier,
                    }
                )

            if not resolved_items:
                messages.error(request, "No se encontraron ítems válidos para registrar.")
                return redirect("inventory_purchases_list")

            purchase_date = None
            date_candidate = raw_date or (metadata.get("date") or "")
            if date_candidate:
                try:
                    if "/" in date_candidate:
                        try:
                            purchase_date = datetime.strptime(date_candidate, "%d/%m/%Y").date()
                        except Exception:
                            purchase_date = datetime.strptime(date_candidate, "%d/%m/%y").date()
                    else:
                        purchase_date = datetime.strptime(date_candidate, "%Y-%m-%d").date()
                except Exception:
                    purchase_date = None
            reference = "Compra PDF"
            if metadata.get("invoice_number"):
                reference = f"Factura {metadata['invoice_number']}"

            try:
                with transaction.atomic():
                    purchase = Purchase.objects.create(
                        supplier=supplier,
                        warehouse=warehouse,
                        reference=reference,
                        discount_percent=Decimal("0.00"),
                        user=request.user,
                    )
                    if pdf_bytes:
                        purchase.invoice_image.save(
                            pdf_upload.name or f"compra_{purchase.id}.pdf",
                            ContentFile(pdf_bytes),
                            save=False,
                        )
                    if purchase_date:
                        created_at = datetime.combine(purchase_date, time(12, 0))
                        if timezone.is_naive(created_at):
                            created_at = timezone.make_aware(created_at)
                        purchase.created_at = created_at

                    subtotal = Decimal("0.00")
                    for data in resolved_items:
                        qty = Decimal(data["quantity"])
                        unit_cost = data["unit_cost"]
                        discount_percent = data.get("discount_percent") or Decimal("0.00")
                        vat_percent = data.get("vat_percent") or Decimal("0.00")
                        effective_unit_cost = (
                            unit_cost * (Decimal("1.00") - (discount_percent / Decimal("100.00")))
                        ).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                        effective_unit_cost_with_vat = (
                            effective_unit_cost * (Decimal("1.00") + (vat_percent / Decimal("100.00")))
                        ).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                        subtotal += qty * effective_unit_cost_with_vat
                        PurchaseItem.objects.create(
                            purchase=purchase,
                            product=data["product"],
                            variant=data.get("variant"),
                            quantity=qty,
                            unit_cost=unit_cost,
                            discount_percent=discount_percent,
                            vat_percent=vat_percent,
                        )
                        if warehouse.type == Warehouse.WarehouseType.COMUN and data.get("variant") is not None:
                            variant = (
                                ProductVariant.objects.select_for_update()
                                .filter(id=data["variant"].id, product=data["product"])
                                .first()
                            )
                            if variant:
                                variant.quantity = (variant.quantity + qty).quantize(Decimal("0.01"))
                                variant.save(update_fields=["quantity"])
                                _koda_sync_common_with_variants(data["product"], warehouse)
                        services.register_entry(
                            product=data["product"],
                            warehouse=warehouse,
                            quantity=qty,
                            unit_cost=effective_unit_cost,
                            supplier=supplier,
                            vat_percent=vat_percent,
                            user=request.user,
                            reference=f"Compra #{purchase.id}",
                            purchase=purchase,
                        )

                    purchase.total = subtotal.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                    purchase.save()
                messages.success(request, f"Compra importada desde PDF ({len(resolved_items)} ítems).")
                if auto_created_products:
                    messages.success(
                        request,
                        f"Se crearon automáticamente {auto_created_products} productos nuevos durante la importación.",
                    )
                if unresolved:
                    preview = ", ".join(unresolved[:4])
                    more = f" y {len(unresolved) - 4} más" if len(unresolved) > 4 else ""
                    messages.warning(
                        request,
                        f"Se omitieron {len(unresolved)} ítems no mapeados: {preview}{more}. "
                        "Podés crearlos/ajustarlos y cargar esos faltantes manualmente.",
                    )
                return redirect("inventory_purchases_list")
            except Exception as exc:
                messages.error(request, f"No se pudo importar la compra desde PDF: {exc}")
                return redirect("inventory_purchases_list")

        header_form = PurchaseHeaderForm(request.POST)
        formset = PurchaseItemFormSet(request.POST)
        payload_ids = _extract_product_ids_from_payload(request.POST.get("items_payload"))
        if payload_ids:
            products_qs = _products_with_last_cost_queryset().filter(id__in=payload_ids)
        else:
            products_qs = Product.objects.none()
        _apply_product_queryset_to_formset(formset, products_qs)
        for form in formset.forms:
            prefix = form.prefix
            product_raw = (request.POST.get(f"{prefix}-product") or "").strip()
            qty_raw = (request.POST.get(f"{prefix}-quantity") or "").strip()
            cost_raw = (request.POST.get(f"{prefix}-unit_cost") or "").strip()
            qty_zeroish = qty_raw in {"", "0", "0.0", "0.00", "0,00"}
            cost_zeroish = cost_raw in {"", "0", "0.0", "0.00", "0,00"}
            if not product_raw and qty_zeroish and cost_zeroish:
                form.empty_permitted = True
            elif not form.has_changed():
                form.empty_permitted = True
        if header_form.is_valid():
            def parse_decimal(raw: str | None) -> Decimal | None:
                if raw is None:
                    return None
                value = str(raw).strip().replace(",", ".")
                if not value:
                    return None
                try:
                    return Decimal(value)
                except Exception:
                    return None

            def resolve_product(product_id: str, product_text: str):
                product = None
                if product_id:
                    product = Product.objects.filter(id=product_id).first()
                if not product and product_text:
                    label = product_text.split(" (", 1)[0].strip()
                    sku_candidate = ""
                    name_candidate = label
                    if " - " in label:
                        sku_candidate, name_candidate = [part.strip() for part in label.split(" - ", 1)]
                    if sku_candidate and sku_candidate.lower() != "sin sku":
                        product = Product.objects.filter(sku__iexact=sku_candidate).first()
                    if not product and name_candidate:
                        product = (
                            Product.objects.filter(name__iexact=name_candidate).first()
                            or Product.objects.filter(name__icontains=name_candidate).first()
                        )
                return product

            def parse_items_from_payload() -> tuple[list[dict], list[str]]:
                raw = request.POST.get("items_payload")
                if not raw:
                    return [], []
                try:
                    items_raw = json.loads(raw)
                except Exception:
                    return [], ["No se pudo leer el detalle de la compra."]
                if not isinstance(items_raw, list):
                    return [], ["Detalle de compra inválido."]
                items_data: list[dict] = []
                errors: list[str] = []
                for idx, entry in enumerate(items_raw, start=1):
                    if not isinstance(entry, dict):
                        continue
                    product_id = str(entry.get("product_id") or "").strip()
                    product_text = str(entry.get("product_text") or "").strip()
                    qty_raw = entry.get("quantity")
                    cost_raw = entry.get("unit_cost")
                    discount_raw = entry.get("discount_percent")
                    supplier_id = str(entry.get("supplier_id") or "").strip()
                    variant_id = str(entry.get("variant_id") or "").strip()
                    vat_raw = entry.get("vat_percent")
                    if not product_id and not str(qty_raw or "").strip() and not str(cost_raw or "").strip():
                        continue
                    if not product_id and not product_text:
                        errors.append(f"Fila {idx}: producto inválido.")
                        continue
                    if not str(qty_raw or "").strip() or not str(cost_raw or "").strip():
                        errors.append(f"Fila {idx}: completá cantidad y costo.")
                        continue
                    product = resolve_product(product_id, product_text)
                    if not product:
                        errors.append(f"Fila {idx}: producto inválido.")
                        continue
                    variant = None
                    if variant_id:
                        variant = ProductVariant.objects.filter(id=variant_id, product=product).first()
                        if not variant:
                            errors.append(f"Fila {idx}: variedad inválida.")
                            continue
                    if ProductVariant.objects.filter(product=product).exists() and not variant:
                        errors.append(f"Fila {idx}: elegí variedad.")
                        continue
                    qty = parse_decimal(str(qty_raw))
                    unit_cost = parse_decimal(str(cost_raw))
                    discount_percent = parse_decimal(str(discount_raw)) or Decimal("0.00")
                    vat_percent = parse_decimal(str(vat_raw)) or Decimal("0.00")
                    if qty is None or qty <= 0:
                        errors.append(f"Fila {idx}: cantidad inválida.")
                        continue
                    if unit_cost is None or unit_cost < 0:
                        errors.append(f"Fila {idx}: costo inválido.")
                        continue
                    if discount_percent < 0 or discount_percent > 100:
                        errors.append(f"Fila {idx}: descuento inválido.")
                        continue
                    supplier = Supplier.objects.filter(id=supplier_id).first() if supplier_id else None
                    items_data.append(
                        {
                            "product": product,
                            "quantity": qty,
                            "unit_cost": unit_cost,
                            "discount_percent": discount_percent,
                            "vat_percent": vat_percent,
                            "supplier": supplier,
                            "variant": variant,
                        }
                    )
                return items_data, errors

            def parse_items_from_post() -> tuple[list[dict], list[str]]:
                indices: set[int] = set()
                for key in request.POST.keys():
                    if key.startswith("form-") and (key.endswith("-product") or key.endswith("-product_text")):
                        parts = key.split("-")
                        if len(parts) >= 3 and parts[1].isdigit():
                            indices.add(int(parts[1]))
                items_data: list[dict] = []
                errors: list[str] = []
                for idx in sorted(indices):
                    prefix = f"form-{idx}-"
                    product_id = (request.POST.get(f"{prefix}product") or "").strip()
                    product_text = (request.POST.get(f"{prefix}product_text") or "").strip()
                    qty_raw = request.POST.get(f"{prefix}quantity")
                    cost_raw = request.POST.get(f"{prefix}unit_cost")
                    discount_raw = request.POST.get(f"{prefix}discount_percent")
                    supplier_id = (request.POST.get(f"{prefix}supplier") or "").strip()
                    variant_id = (request.POST.get(f"{prefix}variant_id") or "").strip()
                    vat_raw = request.POST.get(f"{prefix}vat_percent")
                    if not product_id and not (qty_raw or "").strip() and not (cost_raw or "").strip():
                        continue
                    if not product_id and product_text:
                        label = product_text.split(" (", 1)[0].strip()
                        sku_candidate = ""
                        name_candidate = label
                        if " - " in label:
                            sku_candidate, name_candidate = [part.strip() for part in label.split(" - ", 1)]
                        if sku_candidate and sku_candidate.lower() != "sin sku":
                            product = Product.objects.filter(sku__iexact=sku_candidate).first()
                        else:
                            product = None
                        if not product and name_candidate:
                            product = (
                                Product.objects.filter(name__iexact=name_candidate).first()
                                or Product.objects.filter(name__icontains=name_candidate).first()
                            )
                        if product:
                            product_id = str(product.id)
                    if not product_id or not (qty_raw or "").strip() or not (cost_raw or "").strip():
                        errors.append(f"Fila {idx + 1}: completá producto, cantidad y costo.")
                        continue
                    product = resolve_product(product_id, product_text)
                    if not product:
                        errors.append(f"Fila {idx + 1}: producto inválido.")
                        continue
                    variant = None
                    if variant_id:
                        variant = ProductVariant.objects.filter(id=variant_id, product=product).first()
                        if not variant:
                            errors.append(f"Fila {idx + 1}: variedad inválida.")
                            continue
                    if ProductVariant.objects.filter(product=product).exists() and not variant:
                        errors.append(f"Fila {idx + 1}: elegí variedad.")
                        continue
                    qty = parse_decimal(qty_raw)
                    unit_cost = parse_decimal(cost_raw)
                    discount_percent = parse_decimal(discount_raw) or Decimal("0.00")
                    vat_percent = parse_decimal(vat_raw) or Decimal("0.00")
                    if qty is None or qty <= 0:
                        errors.append(f"Fila {idx + 1}: cantidad inválida.")
                        continue
                    if unit_cost is None or unit_cost < 0:
                        errors.append(f"Fila {idx + 1}: costo inválido.")
                        continue
                    if discount_percent < 0 or discount_percent > 100:
                        errors.append(f"Fila {idx + 1}: descuento inválido.")
                        continue
                    supplier = Supplier.objects.filter(id=supplier_id).first() if supplier_id else None
                    items_data.append(
                        {
                            "product": product,
                            "quantity": qty,
                            "unit_cost": unit_cost,
                            "discount_percent": discount_percent,
                            "vat_percent": vat_percent,
                            "supplier": supplier,
                            "variant": variant,
                        }
                    )
                return items_data, errors

            items, parse_errors = parse_items_from_payload()
            if not items and not parse_errors:
                items, parse_errors = parse_items_from_post()
            if parse_errors:
                for err in parse_errors[:3]:
                    messages.error(request, err)
                if len(parse_errors) > 3:
                    messages.error(request, f"Hay {len(parse_errors)} filas con errores.")
                items = []
            warehouse = header_form.cleaned_data["warehouse"]
            purchase_date = header_form.cleaned_data.get("purchase_date")
            header_discount_percent = header_form.cleaned_data.get("descuento_total") or Decimal("0.00")
            shipping_cost = header_form.cleaned_data.get("costo_envio") or Decimal("0.00")
            if not items:
                messages.error(request, "Agregá al menos un producto.")
                return redirect("inventory_purchases_list")
            try:
                with transaction.atomic():
                    purchase_supplier = items[0].get("supplier") if items else None
                    purchase = Purchase.objects.create(
                        supplier=purchase_supplier,
                        warehouse=warehouse,
                        reference="Compra",
                        discount_percent=header_discount_percent,
                        shipping_cost=shipping_cost,
                        user=request.user,
                    )
                    if purchase_date:
                        created_at = datetime.combine(purchase_date, time(12, 0))
                        if timezone.is_naive(created_at):
                            created_at = timezone.make_aware(created_at)
                        Purchase.objects.filter(pk=purchase.pk).update(created_at=created_at)
                    total_units = sum((Decimal(item["quantity"]) for item in items), Decimal("0.00"))
                    shipping_per_unit = _shipping_cost_per_unit(shipping_cost, total_units)
                    subtotal = Decimal("0.00")
                    for data in items:
                        qty = Decimal(data["quantity"])
                        unit_cost = data["unit_cost"]
                        item_discount_percent = data.get("discount_percent")
                        if item_discount_percent is None:
                            item_discount_percent = Decimal("0.00")
                        vat_percent = data.get("vat_percent")
                        if vat_percent is None:
                            vat_percent = Decimal("0.00")
                        effective_unit_cost = (
                            unit_cost * (Decimal("1.00") - (item_discount_percent / Decimal("100.00")))
                        ).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                        effective_unit_cost_for_stock = (effective_unit_cost + shipping_per_unit).quantize(
                            Decimal("0.01"), rounding=ROUND_HALF_UP
                        )
                        effective_unit_cost_with_vat = (
                            effective_unit_cost * (Decimal("1.00") + (vat_percent / Decimal("100.00")))
                        ).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                        subtotal += qty * effective_unit_cost_with_vat
                        PurchaseItem.objects.create(
                            purchase=purchase,
                            product=data["product"],
                            variant=data.get("variant"),
                            quantity=qty,
                            unit_cost=unit_cost,
                            discount_percent=item_discount_percent,
                            vat_percent=vat_percent,
                        )
                        if warehouse.type == Warehouse.WarehouseType.COMUN and data.get("variant") is not None:
                            variant = (
                                ProductVariant.objects.select_for_update()
                                .filter(id=data["variant"].id, product=data["product"])
                                .first()
                            )
                            if variant:
                                variant.quantity = (variant.quantity + qty).quantize(Decimal("0.01"))
                                variant.save(update_fields=["quantity"])
                                _koda_sync_common_with_variants(data["product"], warehouse)
                        services.register_entry(
                            product=data["product"],
                            warehouse=warehouse,
                            quantity=qty,
                            unit_cost=effective_unit_cost_for_stock,
                            supplier=data["supplier"],
                            vat_percent=vat_percent,
                            user=request.user,
                            reference=f"Compra #{purchase.id}",
                            purchase=purchase,
                        )
                    subtotal_with_shipping = (subtotal + shipping_cost).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                    discount_total = (subtotal_with_shipping * header_discount_percent / Decimal("100.00")).quantize(
                        Decimal("0.01"), rounding=ROUND_HALF_UP
                    )
                    total = (subtotal_with_shipping - discount_total).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                    purchase.total = total
                    purchase.save(update_fields=["total", "discount_percent", "shipping_cost"])
                messages.success(request, "Compra registrada.")
                return redirect("inventory_purchases_list")
            except Exception as exc:
                messages.error(request, f"No se pudo registrar la compra: {exc}")
                return redirect("inventory_purchases_list")
        messages.error(request, "Revisá los campos de la compra.")
    else:
        header_form = PurchaseHeaderForm()
        formset = PurchaseItemFormSet()
        _apply_product_queryset_to_formset(formset, Product.objects.none())

    if show_history:
        purchases = (
            Purchase.objects.select_related("supplier", "warehouse", "user")
            .order_by("-created_at", "-id")
        )
        paginator = Paginator(purchases, 25)
        page_number = request.GET.get("page")
        page_obj = paginator.get_page(page_number)
        purchase_list = page_obj.object_list
    else:
        page_obj = None
        purchase_list = []
    variant_data = {}
    for row in ProductVariant.objects.values("id", "product_id", "name").order_by("name", "id"):
        variant_data.setdefault(str(row["product_id"]), []).append({"id": row["id"], "name": row["name"]})
    warehouses = Warehouse.objects.order_by("name")
    suppliers = Supplier.objects.order_by("name")
    return render(
        request,
        "inventory/purchases_list.html",
        {
            "purchases": purchase_list,
            "page_obj": page_obj,
            "form": header_form,
            "formset": formset,
            "show_history": show_history,
            "variant_data": variant_data,
            "warehouses": warehouses,
            "suppliers": suppliers,
            "default_purchase_date": timezone.localdate().isoformat(),
        },
    )


@login_required
@require_http_methods(["GET", "POST"])
def purchase_edit(request, purchase_id: int):
    purchase = get_object_or_404(
        Purchase.objects.select_related("supplier", "warehouse").prefetch_related("items__product", "movements"),
        pk=purchase_id,
    )
    PurchaseItemFormSet = formset_factory(PurchaseItemForm, extra=0, can_delete=True)
    if request.method == "POST":
        header_form = PurchaseHeaderForm(request.POST)
        formset = PurchaseItemFormSet(request.POST)
        payload_ids = _extract_product_ids_from_payload(request.POST.get("items_payload"))
        if payload_ids:
            products_qs = _products_with_last_cost_queryset().filter(id__in=payload_ids)
        else:
            products_qs = Product.objects.none()
        _apply_product_queryset_to_formset(formset, products_qs)
        items_payload_raw = request.POST.get("items_payload")
        for form in formset.forms:
            prefix = form.prefix
            product_raw = (request.POST.get(f"{prefix}-product") or "").strip()
            qty_raw = (request.POST.get(f"{prefix}-quantity") or "").strip()
            cost_raw = (request.POST.get(f"{prefix}-unit_cost") or "").strip()
            qty_zeroish = qty_raw in {"", "0", "0.0", "0.00", "0,00"}
            cost_zeroish = cost_raw in {"", "0", "0.0", "0.00", "0,00"}
            if not product_raw and qty_zeroish and cost_zeroish:
                form.empty_permitted = True
            elif not form.has_changed():
                form.empty_permitted = True
        if header_form.is_valid():
            items = []
            errors: list[str] = []
            if items_payload_raw:
                def parse_decimal(raw: str | None) -> Decimal | None:
                    if raw is None:
                        return None
                    value = str(raw).strip().replace(",", ".")
                    if not value:
                        return None
                    try:
                        return Decimal(value)
                    except Exception:
                        return None

                def resolve_product(product_id: str, product_text: str):
                    product = None
                    if product_id:
                        product = Product.objects.filter(id=product_id).first()
                    if not product and product_text:
                        label = product_text.split(" (", 1)[0].strip()
                        sku_candidate = ""
                        name_candidate = label
                        if " - " in label:
                            sku_candidate, name_candidate = [part.strip() for part in label.split(" - ", 1)]
                        if sku_candidate and sku_candidate.lower() != "sin sku":
                            product = Product.objects.filter(sku__iexact=sku_candidate).first()
                        if not product and name_candidate:
                            product = (
                                Product.objects.filter(name__iexact=name_candidate).first()
                                or Product.objects.filter(name__icontains=name_candidate).first()
                            )
                    return product

                try:
                    items_raw = json.loads(items_payload_raw)
                except Exception:
                    items_raw = []
                if isinstance(items_raw, list):
                    for idx, entry in enumerate(items_raw, start=1):
                        if not isinstance(entry, dict):
                            continue
                        product_id = str(entry.get("product_id") or "").strip()
                        product_text = str(entry.get("product_text") or "").strip()
                        qty_raw = entry.get("quantity")
                        cost_raw = entry.get("unit_cost")
                        discount_raw = entry.get("discount_percent")
                        supplier_id = str(entry.get("supplier_id") or "").strip()
                        variant_id = str(entry.get("variant_id") or "").strip()
                        vat_raw = entry.get("vat_percent")
                        if not product_id and not product_text:
                            continue
                        product = resolve_product(product_id, product_text)
                        qty = parse_decimal(str(qty_raw))
                        unit_cost = parse_decimal(str(cost_raw))
                        discount_percent = parse_decimal(str(discount_raw)) or Decimal("0.00")
                        if not product or qty is None or qty <= 0 or unit_cost is None:
                            errors.append(f"Fila {idx}: datos inválidos.")
                            continue
                        variant = None
                        if variant_id:
                            variant = ProductVariant.objects.filter(id=variant_id, product=product).first()
                            if not variant:
                                errors.append(f"Fila {idx}: variedad inválida.")
                                continue
                        if ProductVariant.objects.filter(product=product).exists() and not variant:
                            errors.append(f"Fila {idx}: elegí variedad.")
                            continue
                        if discount_percent < 0 or discount_percent > 100:
                            errors.append(f"Fila {idx}: descuento inválido.")
                            continue
                        supplier = Supplier.objects.filter(id=supplier_id).first() if supplier_id else None
                        items.append(
                            {
                                "product": product,
                                "quantity": qty,
                                "unit_cost": unit_cost,
                                "discount_percent": discount_percent,
                                "vat_percent": parse_decimal(str(vat_raw)) or Decimal("0.00"),
                                "supplier": supplier,
                                "variant": variant,
                            }
                        )
            elif formset.is_valid():
                for form in formset:
                    if form.cleaned_data.get("DELETE"):
                        continue
                    if not form.cleaned_data.get("product"):
                        continue
                    prefix = form.prefix
                    product = form.cleaned_data.get("product")
                    variant_id = (request.POST.get(f"{prefix}-variant_id") or "").strip()
                    variant = None
                    if variant_id:
                        variant = ProductVariant.objects.filter(id=variant_id, product=product).first()
                        if not variant:
                            errors.append("Variedad inválida.")
                            continue
                    if product and ProductVariant.objects.filter(product=product).exists() and not variant:
                        errors.append("Elegí variedad.")
                        continue
                    items.append({**form.cleaned_data, "variant": variant})
            if errors:
                for err in errors[:3]:
                    messages.error(request, err)
            if not items:
                messages.error(request, "Agregá al menos un producto.")
                return redirect("inventory_purchase_edit", purchase_id=purchase.id)
            try:
                with transaction.atomic():
                    new_warehouse = header_form.cleaned_data["warehouse"]
                    current_items = list(purchase.items.all())
                    current_signature = sorted(
                        [
                            (item.product_id, item.variant_id or 0, Decimal(item.quantity))
                            for item in current_items
                        ],
                        key=lambda row: (row[0], row[1], row[2]),
                    )
                    new_signature = sorted(
                        [
                            (item["product"].id, item.get("variant").id if item.get("variant") else 0, Decimal(item["quantity"]))
                            for item in items
                        ],
                        key=lambda row: (row[0], row[1], row[2]),
                    )
                    stock_changed = (
                        purchase.warehouse_id != new_warehouse.id
                        or current_signature != new_signature
                    )
                    current_qty_map: dict[tuple[int, int], Decimal] = {}
                    for current in current_items:
                        key = (current.product_id, current.variant_id or 0)
                        current_qty_map[key] = (current_qty_map.get(key) or Decimal("0.00")) + Decimal(current.quantity)
                    new_qty_map: dict[tuple[int, int], Decimal] = {}
                    for item in items:
                        key = (item["product"].id, item.get("variant").id if item.get("variant") else 0)
                        new_qty_map[key] = (new_qty_map.get(key) or Decimal("0.00")) + Decimal(item["quantity"])
                    additive_update = purchase.warehouse_id == new_warehouse.id
                    if additive_update:
                        for key, old_qty in current_qty_map.items():
                            if new_qty_map.get(key, Decimal("0.00")) < old_qty:
                                additive_update = False
                                break
                    if additive_update:
                        stock_changed = False
                    if stock_changed:
                        for movement in purchase.movements.select_for_update():
                            if movement.movement_type == StockMovement.MovementType.ENTRY and movement.to_warehouse:
                                stock, _ = Stock.objects.select_for_update().get_or_create(
                                    product=movement.product,
                                    warehouse=movement.to_warehouse,
                                    defaults={"quantity": Decimal("0.00")},
                                )
                                stock.quantity = (stock.quantity - movement.quantity).quantize(Decimal("0.01"))
                                if stock.quantity < 0:
                                    raise services.NegativeStockError("Stock cannot go negative")
                                stock.save(update_fields=["quantity"])
                            movement.delete()
                        if purchase.warehouse.type == Warehouse.WarehouseType.COMUN:
                            for item in current_items:
                                if not item.variant_id:
                                    continue
                                variant = (
                                    ProductVariant.objects.select_for_update()
                                    .filter(id=item.variant_id, product=item.product)
                                    .first()
                                )
                                if not variant:
                                    continue
                                if variant.quantity - item.quantity < 0:
                                    raise services.NegativeStockError("Stock cannot go negative")
                                variant.quantity = (variant.quantity - item.quantity).quantize(Decimal("0.01"))
                                variant.save(update_fields=["quantity"])
                                _koda_sync_common_with_variants(item.product, purchase.warehouse)
                    purchase.items.all().delete()

                    purchase.warehouse = new_warehouse
                    purchase.supplier = items[0].get("supplier")
                    purchase_date = header_form.cleaned_data.get("purchase_date")
                    header_discount_percent = header_form.cleaned_data.get("descuento_total") or Decimal("0.00")
                    shipping_cost = header_form.cleaned_data.get("costo_envio") or Decimal("0.00")
                    purchase.discount_percent = header_discount_percent
                    purchase.shipping_cost = shipping_cost
                    update_fields = ["warehouse", "supplier", "discount_percent", "shipping_cost"]
                    if purchase_date:
                        created_at = datetime.combine(purchase_date, time(12, 0))
                        if timezone.is_naive(created_at):
                            created_at = timezone.make_aware(created_at)
                        purchase.created_at = created_at
                        update_fields.append("created_at")
                    purchase.save(update_fields=update_fields)

                    total_units = sum((Decimal(item["quantity"]) for item in items), Decimal("0.00"))
                    shipping_per_unit = _shipping_cost_per_unit(shipping_cost, total_units)
                    subtotal = Decimal("0.00")
                    accumulated_new_qty: dict[tuple[int, int], Decimal] = {}
                    for item in items:
                        product = item["product"]
                        qty = item["quantity"]
                        key = (product.id, item.get("variant").id if item.get("variant") else 0)
                        previous_accumulated = accumulated_new_qty.get(key, Decimal("0.00"))
                        accumulated_new_qty[key] = previous_accumulated + qty
                        delta_qty = qty
                        if additive_update:
                            old_total = current_qty_map.get(key, Decimal("0.00"))
                            new_total_after_row = accumulated_new_qty[key]
                            new_total_before_row = previous_accumulated
                            delta_after = max(new_total_after_row - old_total, Decimal("0.00"))
                            delta_before = max(new_total_before_row - old_total, Decimal("0.00"))
                            delta_qty = (delta_after - delta_before).quantize(Decimal("0.01"))
                        unit_cost = item["unit_cost"]
                        item_discount_percent = item.get("discount_percent") or Decimal("0.00")
                        effective_unit_cost = (
                            unit_cost * (Decimal("1.00") - (item_discount_percent / Decimal("100.00")))
                        ).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                        effective_unit_cost_for_stock = (effective_unit_cost + shipping_per_unit).quantize(
                            Decimal("0.01"), rounding=ROUND_HALF_UP
                        )
                        vat = item.get("vat_percent") or Decimal("0.00")
                        effective_unit_cost_with_vat = (
                            effective_unit_cost * (Decimal("1.00") + (vat / Decimal("100.00")))
                        ).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                        subtotal += effective_unit_cost_with_vat * qty
                        PurchaseItem.objects.create(
                            purchase=purchase,
                            product=product,
                            variant=item.get("variant"),
                            quantity=qty,
                            unit_cost=unit_cost,
                            discount_percent=item_discount_percent,
                            vat_percent=vat,
                        )
                        if (stock_changed or additive_update) and purchase.warehouse.type == Warehouse.WarehouseType.COMUN and item.get("variant") is not None:
                            variant = (
                                ProductVariant.objects.select_for_update()
                                .filter(id=item["variant"].id, product=product)
                                .first()
                            )
                            if variant:
                                apply_qty = qty if stock_changed else delta_qty
                                if apply_qty > 0:
                                    variant.quantity = (variant.quantity + apply_qty).quantize(Decimal("0.01"))
                                    variant.save(update_fields=["quantity"])
                                    _koda_sync_common_with_variants(product, purchase.warehouse)
                        if stock_changed or (additive_update and delta_qty > 0):
                            services.register_entry(
                                product=product,
                                warehouse=purchase.warehouse,
                                quantity=qty if stock_changed else delta_qty,
                                unit_cost=effective_unit_cost_for_stock,
                                vat_percent=vat,
                                user=request.user,
                                reference=f"Compra #{purchase.id}",
                                supplier=purchase.supplier,
                                purchase=purchase,
                            )
                    subtotal_with_shipping = (subtotal + shipping_cost).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                    discount_total = (subtotal_with_shipping * header_discount_percent / Decimal("100.00")).quantize(
                        Decimal("0.01"), rounding=ROUND_HALF_UP
                    )
                    total = (subtotal_with_shipping - discount_total).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                    purchase.total = total
                    purchase.save(update_fields=["total"])
                messages.success(request, "Compra actualizada.")
                return redirect("inventory_purchase_receipt", purchase_id=purchase.id)
            except services.NegativeStockError:
                messages.error(request, "No se puede actualizar: el stock quedaría negativo.")
                return redirect("inventory_purchase_edit", purchase_id=purchase.id)
            except Exception as exc:
                messages.error(request, f"No se pudo actualizar la compra: {exc}")
                return redirect("inventory_purchase_edit", purchase_id=purchase.id)
        messages.error(request, "Revisá los campos de la compra.")
        return redirect("inventory_purchase_edit", purchase_id=purchase.id)
    else:
        header_form = PurchaseHeaderForm(
            initial={
                "warehouse": purchase.warehouse,
                "purchase_date": timezone.localtime(purchase.created_at).date(),
                "descuento_total": purchase.discount_percent,
                "costo_envio": purchase.shipping_cost,
            }
        )
        purchase_items = list(purchase.items.select_related("variant", "product"))
        initial = [
            {
                "product": item.product,
                "quantity": int(item.quantity),
                "unit_cost": item.unit_cost,
                "discount_percent": item.discount_percent,
                "supplier": purchase.supplier,
                "vat_percent": item.vat_percent,
            }
            for item in purchase_items
        ]
        formset = PurchaseItemFormSet(initial=initial)
        product_ids = {item["product"].id for item in initial}
        products_qs = _products_with_last_cost_queryset().filter(id__in=product_ids) if product_ids else Product.objects.none()
        _apply_product_queryset_to_formset(formset, products_qs)
        item_rows = [
            {"form": form, "variant_id": item.variant_id}
            for form, item in zip(formset.forms, purchase_items)
        ]
    variant_data = {}
    for row in ProductVariant.objects.values("id", "product_id", "name").order_by("name", "id"):
        variant_data.setdefault(str(row["product_id"]), []).append({"id": row["id"], "name": row["name"]})
    return render(
        request,
        "inventory/purchase_edit.html",
        {
            "purchase": purchase,
            "form": header_form,
            "formset": formset,
            "item_rows": item_rows if request.method != "POST" else None,
            "variant_data": variant_data,
        },
    )


@login_required
@require_http_methods(["POST"])
def purchase_delete(request, purchase_id: int):
    purchase = get_object_or_404(Purchase.objects.prefetch_related("items", "movements"), pk=purchase_id)
    try:
        with transaction.atomic():
            for movement in purchase.movements.select_for_update():
                if movement.movement_type == StockMovement.MovementType.ENTRY and movement.to_warehouse:
                    stock, _ = Stock.objects.select_for_update().get_or_create(
                        product=movement.product,
                        warehouse=movement.to_warehouse,
                        defaults={"quantity": Decimal("0.00")},
                    )
                    stock.quantity = (stock.quantity - movement.quantity).quantize(Decimal("0.01"))
                    if stock.quantity < 0:
                        raise services.NegativeStockError("Stock cannot go negative")
                    stock.save(update_fields=["quantity"])
                movement.delete()
            purchase.delete()
        messages.success(request, "Compra eliminada y stock ajustado.")
    except services.NegativeStockError:
        messages.error(request, "No se puede eliminar: el stock quedaría negativo.")
    except Exception as exc:
        messages.error(request, f"No se pudo eliminar la compra: {exc}")
    return redirect("inventory_purchases_list")
