"""Import stock deductions from a FULL/MercadoLibre shipment preparation PDF."""
import re
from decimal import Decimal
from difflib import SequenceMatcher

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.db import transaction
from django.db.models import Sum
from django.shortcuts import redirect, render

from .. import services
from ..models import MercadoLibreItem, Product, ProductVariant, Stock, Warehouse


def _parse_full_shipment_pdf(file):
    """
    Parse a FULL shipment preparation PDF.
    Returns list of dicts: {ml_code, barcode, name, quantity}
    """
    try:
        import pdfplumber
    except ImportError:
        raise RuntimeError(
            "pdfplumber no está instalado. Agregalo a requirements.txt y reiniciá el servidor."
        )

    items = []
    with pdfplumber.open(file) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            for table in tables:
                for row in table:
                    if not row or len(row) < 2:
                        continue
                    product_cell = (row[0] or "").strip()
                    units_cell = (row[1] or "").strip() if len(row) > 1 else ""

                    if not product_cell or "PRODUCTO" in product_cell.upper():
                        continue

                    try:
                        quantity = int(units_cell)
                    except (ValueError, TypeError):
                        continue
                    if quantity <= 0:
                        continue

                    ml_match = re.search(
                        r'C[oó]digo\s+ML[:\s]+([A-Z0-9]+)',
                        product_cell,
                        re.IGNORECASE,
                    )
                    ml_code = ml_match.group(1).strip() if ml_match else ""

                    barcode_match = re.search(
                        r'C[oó]digo\s+universal[:\s]*\n?\s*(\d{7,14})',
                        product_cell,
                        re.IGNORECASE | re.DOTALL,
                    )
                    barcode = barcode_match.group(1).strip() if barcode_match else ""

                    # Name is the text after "SKU: <value>" and before category label
                    sku_split = re.split(r'SKU:\s*[-\w]*', product_cell, maxsplit=1)
                    if len(sku_split) > 1:
                        name_raw = sku_split[1]
                        name_raw = re.sub(
                            r'[\r\n]*(SUPERMERCADO|COSM[ÉE]TICOS?|HIGIENE.*?|BELLEZA.*?)\s*$',
                            '',
                            name_raw,
                            flags=re.IGNORECASE,
                        ).strip()
                        name = " ".join(name_raw.split())
                    else:
                        name = ""

                    if ml_code or barcode or name:
                        items.append(
                            {
                                "ml_code": ml_code,
                                "barcode": barcode,
                                "name": name,
                                "quantity": quantity,
                            }
                        )

    return items


def _match_product(ml_code: str, name: str, all_products):
    """
    Try to find a matching Product.
    Returns (product, confidence_0_to_1) or (None, 0.0).
    Priority: ML item title match > product name fuzzy match.
    """
    if not name:
        return None, 0.0

    name_lower = name.lower()

    # 1. Match against MercadoLibre item titles (already linked to products)
    linked_items = list(
        MercadoLibreItem.objects.filter(product__isnull=False).select_related("product")
    )
    best_item = None
    best_item_ratio = 0.0
    for ml_item in linked_items:
        for candidate in [ml_item.title, ml_item.matched_name]:
            if not candidate:
                continue
            ratio = SequenceMatcher(None, name_lower, candidate.lower()).ratio()
            if ratio > best_item_ratio:
                best_item_ratio = ratio
                best_item = ml_item

    if best_item_ratio >= 0.60 and best_item:
        return best_item.product, best_item_ratio

    # 2. Fallback: fuzzy match against product names
    best_product = None
    best_ratio = 0.0
    for product in all_products:
        ratio = SequenceMatcher(None, name_lower, product.name.lower()).ratio()
        if ratio > best_ratio:
            best_ratio = ratio
            best_product = product

    if best_ratio >= 0.50:
        return best_product, best_ratio
    return None, best_ratio


def _get_comun_stock(product, comun_wh) -> Decimal:
    """Stock COMUN: suma de variantes si tiene, sino Stock directo."""
    has_variants = ProductVariant.objects.filter(product=product).exists()
    if has_variants:
        total = (
            ProductVariant.objects.filter(product=product)
            .aggregate(t=Sum("quantity"))
            .get("t")
        )
        return Decimal(str(total or "0"))
    stock_obj = Stock.objects.filter(product=product, warehouse=comun_wh).first()
    return stock_obj.quantity if stock_obj else Decimal("0.00")


@login_required
def import_transfer_pdf(request):
    comun_wh = Warehouse.objects.filter(type=Warehouse.WarehouseType.COMUN).first()

    if not comun_wh:
        messages.error(request, "Falta el depósito Común configurado.")
        return redirect("inventory_stock_list")

    if request.method == "POST" and "pdf_file" in request.FILES:
        # Step 1: parse PDF and show confirmation
        pdf_file = request.FILES["pdf_file"]
        try:
            parsed_items = _parse_full_shipment_pdf(pdf_file)
        except RuntimeError as exc:
            messages.error(request, str(exc))
            return redirect("inventory_stock_list")
        except Exception as exc:
            messages.error(request, f"Error al leer el PDF: {exc}")
            return redirect("inventory_stock_list")

        if not parsed_items:
            messages.error(request, "No se encontraron productos en el PDF.")
            return redirect("inventory_stock_list")

        all_products = list(Product.objects.order_by("name"))
        matched = []
        for item in parsed_items:
            product, confidence = _match_product(item["ml_code"], item["name"], all_products)
            comun_stock = _get_comun_stock(product, comun_wh) if product else Decimal("0.00")
            has_variants = ProductVariant.objects.filter(product=product).exists() if product else False
            matched.append(
                {
                    "pdf_name": item["name"],
                    "ml_code": item["ml_code"],
                    "barcode": item["barcode"],
                    "quantity": item["quantity"],
                    "product": product,
                    "product_id": product.id if product else "",
                    "confidence": round(confidence * 100),
                    "comun_stock": comun_stock,
                    "has_variants": has_variants,
                }
            )

        all_products_json = [
            {"id": p.id, "name": p.name, "sku": p.sku or ""}
            for p in all_products
        ]

        return render(
            request,
            "inventory/stock_import_pdf.html",
            {
                "matched": matched,
                "all_products_json": all_products_json,
                "comun_wh": comun_wh,
            },
        )

    elif request.method == "POST":
        # Step 2: descontar del stock COMUN
        product_ids = request.POST.getlist("product_id")
        quantities = request.POST.getlist("quantity")

        if not product_ids:
            messages.error(request, "No hay productos para descontar.")
            return redirect("inventory_stock_list")

        bulk_items = []
        errors = []
        for i, (pid, qty_raw) in enumerate(zip(product_ids, quantities), start=1):
            if not pid:
                continue
            try:
                quantity = Decimal(str(qty_raw).replace(",", "."))
            except Exception:
                errors.append(f"Línea {i}: cantidad inválida.")
                continue
            if quantity <= 0:
                continue
            product = Product.objects.filter(id=pid).first()
            if not product:
                errors.append(f"Línea {i}: producto no encontrado.")
                continue
            bulk_items.append({"product": product, "quantity": quantity})

        if errors:
            messages.error(request, " ".join(errors))
            return redirect("inventory_stock_list")

        if not bulk_items:
            messages.warning(request, "No se seleccionó ningún producto.")
            return redirect("inventory_stock_list")

        try:
            with transaction.atomic():
                for item in bulk_items:
                    product = item["product"]
                    qty = item["quantity"]
                    has_variants = ProductVariant.objects.filter(product=product).exists()
                    if has_variants:
                        # Descontar de variantes proporcionalmente (mayor stock primero)
                        variants = list(
                            ProductVariant.objects.filter(product=product).order_by("-quantity")
                        )
                        remaining = qty
                        for v in variants:
                            if remaining <= 0:
                                break
                            deduct = min(v.quantity, remaining)
                            if deduct > 0:
                                v.quantity = (v.quantity - deduct).quantize(Decimal("0.01"))
                                v.save(update_fields=["quantity"])
                                remaining -= deduct
                        services.sync_comun_from_variants(product)
                    else:
                        services.register_adjustment(
                            product=product,
                            warehouse=comun_wh,
                            quantity=-qty,
                            user=request.user,
                            reference="Descuento envío FULL",
                            allow_negative=True,
                        )
            messages.success(request, f"Stock descontado: {len(bulk_items)} productos.")
        except Exception as exc:
            messages.error(request, f"Error al descontar stock: {exc}")

        return redirect("inventory_stock_list")

    return redirect("inventory_stock_list")
