"""Supplier views."""
from datetime import datetime, time
from decimal import Decimal

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.db import transaction
from django.db.models import ProtectedError, Sum
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
from .common import _normalize_lookup_text
from .forms import (
    SupplierForm,
    SupplierGroupForm,
    SupplierPaymentForm,
    SupplierProductForm,
    SupplierUnlinkGroupForm,
)

from decimal import ROUND_HALF_UP, InvalidOperation


def _parse_price_decimal(value) -> Decimal | None:
    if value is None or value == "":
        return None
    raw = str(value).strip().replace("$", "").replace(" ", "").replace("%", "")
    if not raw:
        return None
    if "," in raw and "." in raw:
        raw = raw.replace(".", "").replace(",", ".")
    elif "," in raw:
        raw = raw.replace(",", ".")
    try:
        return Decimal(raw)
    except InvalidOperation:
        return None


def _name_numbers(norm_name: str) -> tuple:
    """Números presentes en el nombre (tamaños/gramajes/cantidades). Sirven de
    guarda para el match difuso: dos nombres con números distintos NO son el
    mismo producto (p. ej. X 250 ML vs X 500 ML)."""
    import re as _re
    return tuple(sorted(_re.findall(r"\d+", norm_name or "")))


def _parse_price_list_xlsx(file_obj):
    """Parsea un xlsx de lista de precios.

    Columnas: (grupo opcional) + nombre/descripción + precio (neto) + IVA % opcional.
    Devuelve (rows, error) donde rows = [(grupo, nombre, precio_neto, iva_or_None)].
    Detecta encabezados por palabras clave; si no, asume col0=nombre, col1=precio.
    """
    try:
        import openpyxl
        wb = openpyxl.load_workbook(file_obj, data_only=True)
        ws = wb.active
    except Exception as exc:
        return [], f"No se pudo leer el archivo: {exc}"

    all_rows = list(ws.iter_rows(values_only=True))
    if not all_rows:
        return [], "El archivo está vacío."

    name_idx, price_idx, vat_idx, group_idx = 0, 1, None, None
    data_start = 0
    NAME_KEYS = ("nombre", "producto", "descrip", "articulo", "artículo", "detalle")
    PRICE_KEYS = ("precio", "costo", "neto", "importe", "valor", "unitario")
    VAT_KEYS = ("iva", "alicuota", "alícuota")
    GROUP_KEYS = ("grupo", "marca", "rubro", "categoria", "categoría", "linea", "línea")
    for i, row in enumerate(all_rows[:5]):
        cells = [str(c).strip().lower() if c is not None else "" for c in row]
        n_idx = next((j for j, c in enumerate(cells) if any(k in c for k in NAME_KEYS)), None)
        p_idx = next((j for j, c in enumerate(cells) if any(k in c for k in PRICE_KEYS)), None)
        if n_idx is not None and p_idx is not None:
            name_idx, price_idx, data_start = n_idx, p_idx, i + 1
            vat_idx = next((j for j, c in enumerate(cells) if any(k in c for k in VAT_KEYS)), None)
            group_idx = next((j for j, c in enumerate(cells) if any(k in c for k in GROUP_KEYS)), None)
            break

    rows = []
    for row in all_rows[data_start:]:
        if not row:
            continue
        name = row[name_idx] if name_idx < len(row) else None
        price = row[price_idx] if price_idx < len(row) else None
        name = str(name).strip() if name is not None else ""
        net = _parse_price_decimal(price)
        if not name or net is None or net < 0:
            continue
        vat = None
        if vat_idx is not None and vat_idx < len(row):
            vat = _parse_price_decimal(row[vat_idx])
            if vat is not None and (vat < 0 or vat > 100):
                vat = None
        group = ""
        if group_idx is not None and group_idx < len(row) and row[group_idx] is not None:
            group = str(row[group_idx]).strip()
        rows.append((group, name, net, vat))
    if not rows:
        return [], "No se encontraron filas con nombre y precio válidos."
    return rows, None


def _pdf_lines(page):
    """Devuelve las líneas de una página como listas de palabras ordenadas por x."""
    words = page.extract_words(use_text_flow=False, keep_blank_chars=False)
    if not words:
        return []
    words.sort(key=lambda w: (round(float(w["top"])), float(w["x0"])))
    lines = []
    current = []
    current_top = None
    for w in words:
        top = float(w["top"])
        if current_top is None or abs(top - current_top) <= 3:
            current.append(w)
            if current_top is None:
                current_top = top
        else:
            lines.append(sorted(current, key=lambda x: float(x["x0"])))
            current = [w]
            current_top = top
    if current:
        lines.append(sorted(current, key=lambda x: float(x["x0"])))
    return lines


def _group_words_into_cells(words, gap=10.0):
    """Agrupa palabras de una línea en celdas: corta donde el espacio horizontal
    entre palabras supera `gap`. Devuelve [(x0, x1, texto)]."""
    cells = []
    cur = [words[0]]
    for prev, w in zip(words, words[1:]):
        if float(w["x0"]) - float(prev["x1"]) > gap:
            cells.append(cur)
            cur = [w]
        else:
            cur.append(w)
    cells.append(cur)
    return [
        (float(c[0]["x0"]), float(c[-1]["x1"]), " ".join(x["text"] for x in c).strip())
        for c in cells
    ]


def _classify_pdf_column(text: str):
    t = text.strip().lower()
    if any(k in t for k in ("grupo", "marca", "rubro")):
        return "group"
    if any(k in t for k in ("descrip", "producto", "nombre", "detalle", "articulo", "artículo")):
        return "name"
    if "sin iva" in t or "neto" in t:
        return "net"
    if "con iva" in t:
        return "gross"
    if "iva" in t or "alicuota" in t or "alícuota" in t:
        return "iva"
    if any(k in t for k in ("precio", "costo", "importe", "valor")):
        return "price"
    return None


def _parse_price_list_pdf(pdf_bytes):
    """Parsea un PDF de lista de precios (Grupo · Descripción · … · Precio).

    Robusto a descripciones largas: los precios se detectan por patrón (número con
    coma decimal) y el IVA por el '%', NO por posición de columna. El primer precio
    de la fila es el neto (sin IVA); si hay más de uno, el resto (p. ej. "con IVA")
    se ignora y se recalcula. El corte Grupo/Descripción se detecta por el mayor
    espacio horizontal entre las palabras de texto.
    Devuelve (rows, error) con rows = [(grupo, nombre, precio_neto, iva_or_None)].
    """
    import io
    import re as _re
    try:
        import pdfplumber
    except Exception as exc:
        return [], f"No se pudo abrir el PDF: {exc}"

    price_re = _re.compile(r"^\$?\d[\d.]*,\d{1,2}$")
    iva_re = _re.compile(r"^\d{1,3}([.,]\d+)?%$")
    rows = []
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                for ws in _pdf_lines(page):
                    if len(ws) < 2:
                        continue
                    joined = " ".join(w["text"] for w in ws).lower()
                    if "descrip" in joined and "precio" in joined:
                        continue  # fila de encabezados

                    prices = []
                    iva = None
                    text_words = []
                    for w in ws:
                        t = w["text"].strip()
                        if price_re.match(t):
                            val = _parse_price_decimal(t)
                            if val is not None:
                                prices.append(val)
                        elif iva_re.match(t):
                            v = _parse_price_decimal(t)
                            if v is not None and 0 <= v <= 100:
                                iva = v
                        else:
                            text_words.append(w)
                    if not prices or not text_words:
                        continue
                    net = prices[0]
                    if net is None or net < 0:
                        continue

                    if len(text_words) == 1:
                        group, name = "", text_words[0]["text"]
                    else:
                        gaps = [
                            (float(text_words[i + 1]["x0"]) - float(text_words[i]["x1"]), i)
                            for i in range(len(text_words) - 1)
                        ]
                        _, idx = max(gaps, key=lambda g: g[0])
                        group = " ".join(w["text"] for w in text_words[: idx + 1])
                        name = " ".join(w["text"] for w in text_words[idx + 1:])
                    name = name.strip()
                    if not name or name.lower() in ("descripción", "descripcion"):
                        continue
                    rows.append((group.strip(), name, net, iva))
    except Exception as exc:
        return [], f"No se pudo procesar el PDF: {exc}"
    if not rows:
        return [], "No se encontraron filas con descripción y precio en el PDF."
    return rows, None


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
                vat_percent = link_form.cleaned_data.get("vat_percent")
                if vat_percent is None:
                    vat_percent = product.vat_percent or Decimal("0.00")
                SupplierProduct.objects.update_or_create(
                    supplier=supplier,
                    product=product,
                    defaults={
                        "last_cost": last_cost,
                        "vat_percent": vat_percent,
                        "last_purchase_at": timezone.now(),
                    },
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
                        defaults={
                            "last_cost": last_cost,
                            "vat_percent": product.vat_percent or Decimal("0.00"),
                            "last_purchase_at": timezone.now(),
                        },
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
        elif action == "import_price_list":
            supplier = Supplier.objects.filter(id=request.POST.get("price_supplier_id")).first()
            upload = request.FILES.get("price_file")
            iva_raw = (request.POST.get("price_vat") or "").strip().replace(",", ".")
            try:
                default_iva = Decimal(iva_raw) if iva_raw else Decimal("0.00")
            except Exception:
                default_iva = Decimal("0.00")
            if not supplier:
                messages.error(request, "Elegí un proveedor para la lista de precios.")
                return redirect("inventory_suppliers")
            if not upload:
                messages.error(request, "Subí un archivo .xlsx o .pdf con la lista de precios.")
                return redirect("inventory_suppliers")
            if (upload.name or "").lower().endswith(".pdf"):
                rows, parse_error = _parse_price_list_pdf(upload.read())
            else:
                rows, parse_error = _parse_price_list_xlsx(upload)
            if parse_error:
                messages.error(request, parse_error)
                return redirect("inventory_suppliers")
            from difflib import SequenceMatcher
            existing_by_key = {}
            existing_by_nums = {}
            for p in Product.objects.all():
                nk = _normalize_lookup_text(p.name)
                existing_by_key.setdefault(nk, p)
                existing_by_nums.setdefault(_name_numbers(nk), []).append((nk, p))
            created_names = []
            new_links = updated_links = matched_fuzzy = 0
            for group, name, net, row_vat in rows:
                iva = row_vat if row_vat is not None else default_iva
                cost_with_vat = (net * (Decimal("1.00") + iva / Decimal("100.00"))).quantize(
                    Decimal("0.01"), rounding=ROUND_HALF_UP
                )
                key = _normalize_lookup_text(name)
                product = existing_by_key.get(key)
                if product is None:
                    # Match por coincidencia, SOLO entre productos con los mismos
                    # números (tamaños/gramajes), para no fusionar presentaciones
                    # distintas (250 ml vs 500 ml).
                    key_tokens = set(key.split())
                    candidates = existing_by_nums.get(_name_numbers(key), [])
                    best, best_r = None, None
                    for cand_nk, cand_p in candidates:
                        cand_tokens = set(cand_nk.split())
                        # (a) Un nombre es el otro + palabras extra (p. ej. la marca
                        # "THE HUNTER"): subconjunto de tokens → mismo producto.
                        if key_tokens <= cand_tokens or cand_tokens <= key_tokens:
                            extra = len(key_tokens ^ cand_tokens)
                            if best_r is None or extra < best_r:
                                best, best_r = cand_p, extra
                    if best is None:
                        # (b) Si no hubo subconjunto, similitud de texto alta (typos,
                        # puntuación) con los mismos números.
                        best_r = 0.86
                        for cand_nk, cand_p in candidates:
                            r = SequenceMatcher(None, key, cand_nk).ratio()
                            if r >= best_r:
                                best, best_r = cand_p, r
                    if best is not None:
                        product = best
                        matched_fuzzy += 1
                if product is None:
                    product = Product.objects.create(
                        name=name,
                        group=group or "",
                        vat_percent=iva,
                        avg_cost=net,
                        default_supplier=supplier,
                    )
                    existing_by_key[key] = product
                    existing_by_nums.setdefault(_name_numbers(key), []).append((key, product))
                    created_names.append(name)
                _, was_created = SupplierProduct.objects.update_or_create(
                    supplier=supplier,
                    product=product,
                    defaults={
                        "last_cost": cost_with_vat,
                        "vat_percent": iva,
                        "last_purchase_at": timezone.now(),
                    },
                )
                if was_created:
                    new_links += 1
                else:
                    updated_links += 1
            messages.success(
                request,
                (
                    f"Lista de precios de {supplier.name} importada: "
                    f"{len(created_names)} producto(s) nuevo(s), "
                    f"{matched_fuzzy} por coincidencia, "
                    f"{new_links} vínculo(s) nuevo(s), {updated_links} actualizado(s)."
                ),
            )
            if created_names:
                # Se muestran tras el redirect para revisar posibles duplicados.
                request.session["price_import_created"] = created_names[:500]
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
        elif action == "set_link_vat":
            link = SupplierProduct.objects.filter(pk=request.POST.get("link_id")).first()
            new_vat = _parse_price_decimal(request.POST.get("vat_percent"))
            if not link or new_vat is None or new_vat < 0 or new_vat > 100:
                messages.error(request, "IVA inválido.")
                return redirect("inventory_suppliers")
            # El neto se mantiene; el costo con IVA se recalcula con la nueva alícuota.
            net = (link.last_cost / (Decimal("1.00") + (link.vat_percent or Decimal("0.00")) / Decimal("100.00")))
            link.last_cost = (net * (Decimal("1.00") + new_vat / Decimal("100.00"))).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )
            link.vat_percent = new_vat
            link.save(update_fields=["last_cost", "vat_percent"])
            messages.success(request, "IVA actualizado.")
            return redirect("inventory_suppliers")
        elif action == "set_supplier_vat_all":
            supplier = Supplier.objects.filter(id=request.POST.get("supplier_id")).first()
            new_vat = _parse_price_decimal(request.POST.get("vat_percent"))
            if not supplier or new_vat is None or new_vat < 0 or new_vat > 100:
                messages.error(request, "IVA inválido.")
                return redirect("inventory_suppliers")
            factor_new = Decimal("1.00") + new_vat / Decimal("100.00")
            count = 0
            for link in SupplierProduct.objects.filter(supplier=supplier):
                net = link.last_cost / (Decimal("1.00") + (link.vat_percent or Decimal("0.00")) / Decimal("100.00"))
                link.last_cost = (net * factor_new).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                link.vat_percent = new_vat
                link.save(update_fields=["last_cost", "vat_percent"])
                count += 1
            messages.success(request, f"IVA {new_vat}% aplicado a {count} producto(s) de {supplier.name}.")
            return redirect("inventory_suppliers")
        elif action == "clear_supplier_pricelist":
            supplier = Supplier.objects.filter(id=request.POST.get("supplier_id")).first()
            if not supplier:
                messages.error(request, "Proveedor no encontrado.")
                return redirect("inventory_suppliers")
            deleted, _ = SupplierProduct.objects.filter(supplier=supplier).delete()
            messages.success(request, f"Se vació la lista de precios de {supplier.name} ({deleted} vínculo/s). Los productos no se borraron.")
            return redirect("inventory_suppliers")
        elif action == "delete_supplier_products":
            supplier = Supplier.objects.filter(id=request.POST.get("supplier_id")).first()
            if not supplier:
                messages.error(request, "Proveedor no encontrado.")
                return redirect("inventory_suppliers")
            # Borra los productos cuyo proveedor principal es este. Los que tienen
            # ventas/compras/movimientos quedan protegidos: se saltean y se informan.
            deleted = skipped = 0
            for product in Product.objects.filter(default_supplier=supplier):
                try:
                    with transaction.atomic():
                        product.delete()
                    deleted += 1
                except ProtectedError:
                    skipped += 1
            msg = f"Se eliminaron {deleted} producto(s) de {supplier.name}."
            if skipped:
                msg += f" {skipped} se conservaron por tener ventas/compras asociadas."
            messages.success(request, msg)
            return redirect("inventory_suppliers")
        elif action == "set_supplier_vat_group":
            supplier = Supplier.objects.filter(id=request.POST.get("supplier_id")).first()
            group = (request.POST.get("group") or "").strip()
            new_vat = _parse_price_decimal(request.POST.get("vat_percent"))
            if not supplier or not group or new_vat is None or new_vat < 0 or new_vat > 100:
                messages.error(request, "Elegí marca e IVA válidos.")
                return redirect("inventory_suppliers")
            factor_new = Decimal("1.00") + new_vat / Decimal("100.00")
            count = 0
            for link in SupplierProduct.objects.filter(
                supplier=supplier, product__group__iexact=group
            ).select_related("product"):
                net = link.last_cost / (Decimal("1.00") + (link.vat_percent or Decimal("0.00")) / Decimal("100.00"))
                link.last_cost = (net * factor_new).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                link.vat_percent = new_vat
                link.save(update_fields=["last_cost", "vat_percent"])
                count += 1
            messages.success(request, f"IVA {new_vat}% aplicado a {count} producto(s) de la marca '{group}' ({supplier.name}).")
            return redirect("inventory_suppliers")

    # Lista de proveedores con sus marcas (para aplicar IVA por marca).
    suppliers_list = list(suppliers_qs.order_by("name"))
    for s in suppliers_list:
        s.group_list = sorted({
            (sp.product.group or "").strip()
            for sp in s.supplier_products.all()
            if (sp.product.group or "").strip()
        })
    context = {
        "supplier_form": supplier_form,
        "link_form": link_form,
        "link_group_form": link_group_form,
        "unlink_group_form": unlink_group_form,
        "suppliers": suppliers_list,
        "price_import_created": request.session.pop("price_import_created", None),
    }
    purchases_totals = {
        row["supplier_id"]: row["total"] or Decimal("0.00")
        for row in Purchase.objects.filter(supplier__isnull=False)
        .values("supplier_id")
        .annotate(total=Sum("total"))
    }
    # Filtro de fechas para "comprado en el período" (no afecta el saldo CC,
    # que es siempre acumulado).
    period_start = (request.GET.get("start_date") or "").strip()
    period_end = (request.GET.get("end_date") or "").strip()
    period_qs = Purchase.objects.filter(supplier__isnull=False)
    start_d = end_d = None
    if period_start:
        try:
            start_d = datetime.strptime(period_start, "%Y-%m-%d").date()
            period_qs = period_qs.filter(
                created_at__gte=timezone.make_aware(datetime.combine(start_d, time.min))
            )
        except ValueError:
            pass
    if period_end:
        try:
            end_d = datetime.strptime(period_end, "%Y-%m-%d").date()
            period_qs = period_qs.filter(
                created_at__lte=timezone.make_aware(datetime.combine(end_d, time.max))
            )
        except ValueError:
            pass
    purchases_period_totals = {
        row["supplier_id"]: row["total"] or Decimal("0.00")
        for row in period_qs.values("supplier_id").annotate(total=Sum("total"))
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
    period_total = Decimal("0.00")
    for supplier in suppliers_qs.order_by("name"):
        purchases_total = purchases_totals.get(supplier.id, Decimal("0.00"))
        payments_total = payments_totals.get(supplier.id, Decimal("0.00"))
        adjustments_total = adjustments_totals.get(supplier.id, Decimal("0.00"))
        balance = purchases_total - payments_total + adjustments_total
        purchases_period = purchases_period_totals.get(supplier.id, Decimal("0.00"))
        period_total += purchases_period
        supplier_rows.append({
            "supplier": supplier,
            "balance": balance,
            "purchases_period": purchases_period,
        })
        if balance > 0:
            debtors.append({"supplier": supplier, "balance": balance})
            total_debt += balance
    debtors.sort(key=lambda item: item["balance"], reverse=True)
    debtors = debtors[:8]
    context["supplier_rows"] = supplier_rows
    context["period_start"] = period_start
    context["period_end"] = period_end
    context["period_total"] = period_total
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

    # Filtro de fechas (opcional): acota compras y movimientos al plazo elegido.
    period_start = (request.GET.get("start_date") or "").strip()
    period_end = (request.GET.get("end_date") or "").strip()
    start_d = end_d = None
    if period_start:
        try:
            start_d = datetime.strptime(period_start, "%Y-%m-%d").date()
        except ValueError:
            start_d = None
    if period_end:
        try:
            end_d = datetime.strptime(period_end, "%Y-%m-%d").date()
        except ValueError:
            end_d = None

    all_purchases = list(
        Purchase.objects.filter(supplier=supplier)
        .select_related("warehouse")
        .order_by("-created_at", "-id")
    )
    all_payments = list(
        SupplierPayment.objects.filter(supplier=supplier)
        .select_related("purchase")
        .order_by("-paid_at", "-id")
    )

    def _purchase_date(purchase):
        return timezone.localdate(purchase.created_at)

    def _in_range(d):
        if start_d and d < start_d:
            return False
        if end_d and d > end_d:
            return False
        return True

    # Saldo arrastrado: movimientos anteriores al inicio del plazo.
    opening_balance = Decimal("0.00")
    if start_d:
        for purchase in all_purchases:
            if _purchase_date(purchase) < start_d:
                opening_balance += purchase.total
        for payment in all_payments:
            if payment.paid_at < start_d:
                if payment.kind == SupplierPayment.Kind.PAYMENT:
                    opening_balance -= payment.amount
                else:
                    opening_balance += payment.amount

    purchases = [p for p in all_purchases if _in_range(_purchase_date(p))]
    payments = [p for p in all_payments if _in_range(p.paid_at)]

    # El pagado por compra usa TODOS los pagos (un pago puede caer fuera del
    # plazo elegido), para que el saldo por compra siga siendo real.
    payment_by_purchase = {}
    for payment in all_payments:
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
    balance = opening_balance
    for entry in ledger_entries:
        balance += entry["debit"]
        balance -= entry["credit"]
        entry["balance"] = balance

    # Totales del plazo (lo filtrado). Sin filtro, equivalen a todo el historial.
    total_purchases = sum((purchase.total for purchase in purchases), Decimal("0.00"))
    total_payments = sum(
        (payment.amount for payment in payments if payment.kind == SupplierPayment.Kind.PAYMENT),
        Decimal("0.00"),
    )
    total_adjustments = sum(
        (payment.amount for payment in payments if payment.kind == SupplierPayment.Kind.ADJUSTMENT),
        Decimal("0.00"),
    )
    # Saldo actual: SIEMPRE acumulado (todo el historial), no depende del filtro.
    current_balance = (
        sum((p.total for p in all_purchases), Decimal("0.00"))
        - sum((p.amount for p in all_payments if p.kind == SupplierPayment.Kind.PAYMENT), Decimal("0.00"))
        + sum((p.amount for p in all_payments if p.kind == SupplierPayment.Kind.ADJUSTMENT), Decimal("0.00"))
    )
    has_filter = bool(start_d or end_d)

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
            "opening_balance": opening_balance,
            "period_start": period_start,
            "period_end": period_end,
            "has_filter": has_filter,
        },
    )
