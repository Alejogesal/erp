"""Import stock transfers from a FULL/MercadoLibre shipment preparation PDF."""
import re
from decimal import Decimal
from difflib import SequenceMatcher

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.db import transaction
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
    """
    # 1. MercadoLibreItem by item_id (exact or partial)
    if ml_code:
        for candidate_id in [ml_code, f"MLA{ml_code}"]:
            ml_item = (
                MercadoLibreItem.objects.filter(item_id=candidate_id)
                .select_related("product")
                .first()
            )
            if ml_item and ml_item.product:
                return ml_item.product, 1.0
        # Partial match as fallback
        ml_item = (
            MercadoLibreItem.objects.filter(item_id__icontains=ml_code)
            .select_related("product")
            .first()
        )
        if ml_item and ml_item.product:
            return ml_item.product, 0.9

    # 2. Fuzzy name match
    if not name:
        return None, 0.0

    name_lower = name.lower()
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


@login_required
def import_transfer_pdf(request):
    comun_wh = Warehouse.objects.filter(type=Warehouse.WarehouseType.COMUN).first()
    ml_wh = Warehouse.objects.filter(type=Warehouse.WarehouseType.MERCADOLIBRE).first()

    if not comun_wh or not ml_wh:
        messages.error(request, "Faltan depósitos configurados.")
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
            comun_stock = Decimal("0.00")
            if product:
                stock_obj = Stock.objects.filter(product=product, warehouse=comun_wh).first()
                comun_stock = stock_obj.quantity if stock_obj else Decimal("0.00")
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
                "ml_wh": ml_wh,
            },
        )

    elif request.method == "POST":
        # Step 2: process confirmed transfers
        product_ids = request.POST.getlist("product_id")
        quantities = request.POST.getlist("quantity")

        if not product_ids:
            messages.error(request, "No hay productos para transferir.")
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
            if ProductVariant.objects.filter(product=product).exists():
                errors.append(
                    f"'{product.name}' tiene variedades — transferí por el formulario principal."
                )
                continue
            bulk_items.append({"product": product, "quantity": quantity})

        if errors:
            messages.error(request, " ".join(errors))
            return redirect("inventory_stock_list")

        if not bulk_items:
            messages.warning(request, "No se seleccionó ningún producto para transferir.")
            return redirect("inventory_stock_list")

        try:
            with transaction.atomic():
                for item in bulk_items:
                    services.register_transfer(
                        product=item["product"],
                        from_warehouse=comun_wh,
                        to_warehouse=ml_wh,
                        quantity=item["quantity"],
                        user=request.user,
                        reference="Transferencia FULL envío",
                    )
            messages.success(request, f"Transferencias registradas: {len(bulk_items)}.")
        except services.NegativeStockError:
            messages.error(request, "No hay stock suficiente en depósito común.")
        except services.InvalidMovementError as exc:
            messages.error(request, str(exc))

        return redirect("inventory_stock_list")

    return redirect("inventory_stock_list")
