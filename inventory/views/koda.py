"""Koda AI assistant views and helpers."""
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import ast
import base64
import calendar
import json
import operator
import os
import re
import secrets
from datetime import datetime, time, timedelta
from urllib.request import Request, urlopen

from django.contrib.auth.decorators import login_required
from django.contrib.auth import get_user_model
from django.core.files.base import ContentFile
from django.db import transaction
from django.db.models import Sum
from django.http import JsonResponse
from django.utils import timezone
from django.views.decorators.http import require_http_methods

from .. import services
from ..services import update_product_avg_costs
from ..models import (
    Customer,
    KitComponent,
    Product,
    ProductVariant,
    Purchase,
    PurchaseItem,
    Sale,
    SaleItem,
    Stock,
    StockMovement,
    Supplier,
    TaxExpense,
    Warehouse,
)
from .utils_xlsx import _normalize_header, _sku_prefix


def _koda_allowed(user) -> bool:
    if not user or not user.is_authenticated:
        return False
    full_name = (user.get_full_name() or "").strip().lower()
    username = (getattr(user, "username", "") or "").strip().lower()
    email = (getattr(user, "email", "") or "").strip().lower()
    if full_name == "alejo salmeron":
        return True
    if username in {"alejo", "alejosalmeron", "alejo.salmeron"}:
        return True
    if email in {"alejo@stylmoda.site", "alejosalmeron@gmail.com"}:
        return True
    return bool(getattr(user, "is_superuser", False))


def _koda_system_prompt() -> str:
    return (
        "Sos Koda, el asistente del ERP. Respondé SOLO en JSON válido con las claves: "
        "reply (string), actions (array), needs_confirmation (boolean). "
        "Cada acción debe ser un objeto con {type, data}. "
        "REGLA CRÍTICA: needs_confirmation=true SOLO cuando hay actions que modifican datos (crear, registrar, transferir, eliminar). "
        "Para preguntas informativas (margen, ventas, stock, costos, reportes) NUNCA uses needs_confirmation=true ni pidas confirmación. "
        "Respondé directamente con la información solicitada. "
        "Si el usuario dice 'en general', 'todo el año', 'siempre' o similar, asumí el año actual sin pedir fechas. "
        "Solo pedí fechas si el período es genuinamente ambiguo (ej: 'el mes pasado de qué año'). "
        "Nunca respondas 'puedo hacerlo, confirmame' para consultas informativas. Simplemente respondé. "
        "Nunca inventes cifras o resultados. "
        "Para consultas informativas, dejá actions vacío y needs_confirmation=false. "
        "Siempre respondé algo útil en reply, aunque no haya acciones. "
        "Si hay una imagen adjunta, extraé los datos relevantes de la imagen. "
        "Nunca digas que ejecutaste una acción si no envías actions. "
        "Si el usuario pide ejecutar/registrar/crear/transferir, devolvé actions y needs_confirmation=true. "
        "Respondé en español rioplatense, directo y claro. "
        "Acciones permitidas:\n"
        "- create_product: {name, sku?, group?, avg_cost?, vat_percent?, price_consumer?, price_barber?, price_distributor?}\n"
        "- add_stock_comun: {items:[{product, quantity, unit_cost?, variant?}]}\n"
        "- transfer_to_ml: {items:[{product, quantity, variant?}]}\n"
        "- register_sale: {warehouse, audience?, customer?, items:[{product, quantity, unit_price?, vat_percent?}]}\n"
        "- update_sale: {sale_id? or invoice_number?, sale_date?, total?, ml_commission_total?, ml_tax_total?}\n"
        "- register_purchase: {warehouse?, supplier, reference?, items:[{product, quantity, unit_cost, vat_percent, discount_percent?}]}\n"
        "Para compras con imagen/PDF de factura: extraé TODOS los ítems con {product: descripcion_exacta, quantity, unit_cost: precio_unitario_sin_iva, vat_percent: porcentaje_iva_numerico, discount_percent: descuento_porcentaje_o_0}.\n"
        "unit_cost es el precio neto unitario SIN IVA tal como figura en la factura. vat_percent es el número (ej: 21, 10.5, 0). Si hay bonificación/descuento, ponelo en discount_percent.\n"
        "Si un ítem tiene 100% de descuento, unit_cost=0 y discount_percent=100.\n"
        "supplier debe ser el nombre del proveedor tal como figura en la factura. reference puede ser el número de factura.\n"
        "Si no podés leer el precio de un ítem, dejá unit_cost=null (el sistema usará el costo histórico).\n"
        "Usá identificadores de producto por SKU si es posible, sino por nombre exacto. "
        "Si el producto tiene variedades, pedí o incluí la variedad."
    )


def _koda_format_amount(value: Decimal, currency: bool = True) -> str:
    try:
        number = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        number = Decimal("0.00")
    number = number.quantize(Decimal("1.00"), rounding=ROUND_HALF_UP)
    formatted = f"{number:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"${formatted}" if currency else formatted


def _koda_extract_percent(message: str) -> Decimal | None:
    match = re.search(r"(\d+(?:[.,]\d+)?)\s*(?:%|por\s*ciento)", message, re.IGNORECASE)
    if not match:
        return None
    raw = match.group(1).replace(",", ".")
    try:
        return Decimal(raw)
    except InvalidOperation:
        return None


def _koda_extract_warehouse(message: str) -> str | None:
    lowered = message.lower()
    if "mercadolibre" in lowered or "mercado libre" in lowered or re.search(r"\bml\b", lowered):
        return Warehouse.WarehouseType.MERCADOLIBRE
    if "comun" in lowered or "común" in lowered:
        return Warehouse.WarehouseType.COMUN
    return None


def _koda_parse_date_range(message: str) -> tuple[datetime, datetime, datetime.date, datetime.date] | None:
    lowered = message.lower()
    today = timezone.localdate()
    if "hoy" in lowered:
        start_date = end_date = today
    elif "ayer" in lowered:
        day = today - timedelta(days=1)
        start_date = end_date = day
    elif "este mes" in lowered or "mes actual" in lowered:
        start_date = today.replace(day=1)
        end_date = today
    elif "mes pasado" in lowered or "mes anterior" in lowered:
        first_this_month = today.replace(day=1)
        last_prev_month = first_this_month - timedelta(days=1)
        start_date = last_prev_month.replace(day=1)
        end_date = last_prev_month
    elif re.search(r"\baño\s+(actual|en\s+curso|corriente|vigente)\b|\beste\s+año\b|\baño\s+actual\b", lowered):
        start_date = today.replace(month=1, day=1)
        end_date = today
    elif re.search(r"\baño\s+pasado\b|\baño\s+anterior\b|\baño\s+\d{4}\b", lowered):
        year_match = re.search(r"\b(\d{4})\b", lowered)
        if year_match:
            y = int(year_match.group(1))
            start_date = today.replace(year=y, month=1, day=1)
            end_date = today.replace(year=y, month=12, day=31)
        else:
            y = today.year - 1
            start_date = today.replace(year=y, month=1, day=1)
            end_date = today.replace(year=y, month=12, day=31)
    elif re.search(r"\ben\s+general\b|\bsiemp?re\b|\btodo\s+el\s+tiempo\b|\bhistorial\b|\btodos?\b", lowered):
        start_date = today.replace(year=today.year, month=1, day=1)
        end_date = today
    else:
        month_map = {
            "enero": 1,
            "febrero": 2,
            "marzo": 3,
            "abril": 4,
            "mayo": 5,
            "junio": 6,
            "julio": 7,
            "agosto": 8,
            "septiembre": 9,
            "setiembre": 9,
            "octubre": 10,
            "noviembre": 11,
            "diciembre": 12,
        }
        month_match = re.search(
            r"\b(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|setiembre|octubre|noviembre|diciembre)\b(?:\s+de)?\s*(\d{4})?",
            lowered,
        )
        if month_match:
            month = month_map[month_match.group(1)]
            year_raw = month_match.group(2)
            if year_raw:
                year = int(year_raw)
            else:
                year = today.year
                if month > today.month:
                    year -= 1
            last_day = calendar.monthrange(year, month)[1]
            start_date = datetime(year, month, 1).date()
            end_date = datetime(year, month, last_day).date()
        else:
            matches = list(re.finditer(r"(\d{1,2})[/-](\d{1,2})(?:[/-](\d{2,4}))?", lowered))
            if not matches:
                return None
            def parse_match(match, fallback_year: int) -> datetime.date | None:
                day = int(match.group(1))
                month = int(match.group(2))
                year_raw = match.group(3)
                if year_raw:
                    year = int(year_raw)
                    if year < 100:
                        year += 2000
                else:
                    year = fallback_year
                try:
                    return datetime(year, month, day).date()
                except ValueError:
                    return None

            first = matches[0]
            fallback_year = today.year
            first_date = parse_match(first, fallback_year)
            if not first_date:
                return None
            if len(matches) >= 2:
                second = matches[1]
                second_date = parse_match(second, first_date.year)
                if not second_date:
                    return None
                start_date, end_date = first_date, second_date
            else:
                start_date = end_date = first_date
    if end_date < start_date:
        start_date, end_date = end_date, start_date
    start_dt = timezone.make_aware(datetime.combine(start_date, time.min))
    end_dt = timezone.make_aware(datetime.combine(end_date, time.max))
    return start_dt, end_dt, start_date, end_date


def _koda_safe_eval(expression: str) -> Decimal:
    bin_ops = {
        ast.Add: operator.add,
        ast.Sub: operator.sub,
        ast.Mult: operator.mul,
        ast.Div: operator.truediv,
    }
    unary_ops = {ast.UAdd: operator.pos, ast.USub: operator.neg}

    def eval_node(node):
        if isinstance(node, ast.Expression):
            return eval_node(node.body)
        if isinstance(node, ast.BinOp) and type(node.op) in bin_ops:
            return bin_ops[type(node.op)](eval_node(node.left), eval_node(node.right))
        if isinstance(node, ast.UnaryOp) and type(node.op) in unary_ops:
            return unary_ops[type(node.op)](eval_node(node.operand))
        if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
            return Decimal(str(node.value))
        raise ValueError("Expresión inválida")

    tree = ast.parse(expression, mode="eval")
    return eval_node(tree)


def _koda_try_math(message: str) -> str | None:
    lowered = message.lower()
    if any(word in lowered for word in ("venta", "ventas", "compra", "compras", "ganancia", "margen", "stock", "producto", "cliente", "proveedor", "iva", "impuesto")):
        return None
    if not re.search(r"\d", message):
        return None
    expr = re.sub(r"[^0-9\.\,\+\-\*\/\(\)\s]", "", message)
    if not re.search(r"[\+\-\*\/]", expr):
        return None
    expr = expr.replace(",", ".")
    try:
        result = _koda_safe_eval(expr)
        return f"Resultado: {_koda_format_amount(result, currency=False)}"
    except Exception:
        return None


def _koda_sales_profit(sales_qs) -> Decimal:
    profit_total = Decimal("0.00")
    for sale in sales_qs:
        cost_total = Decimal("0.00")
        for item in sale.items.all():
            cost_unit = item.cost_unit if item.cost_unit and item.cost_unit > 0 else item.product.cost_with_vat()
            cost_total += item.quantity * cost_unit
        if sale.warehouse.type == Warehouse.WarehouseType.MERCADOLIBRE:
            net_total = (sale.total or Decimal("0.00")) - (sale.ml_commission_total or Decimal("0.00")) - (
                sale.ml_tax_total or Decimal("0.00")
            )
            profit_total += net_total - cost_total
        else:
            profit_total += (sale.total or Decimal("0.00")) - cost_total
    return profit_total


def _koda_try_local_response(message: str) -> str | None:
    if not message:
        return None
    lowered = message.lower()
    percent = _koda_extract_percent(lowered)
    wants_profit = any(word in lowered for word in ("ganancia", "margen", "utilidad", "gané", "gane", "ganado", "ganar")) or bool(re.search(r"\bgan", lowered))
    wants_sales = (
        "venta" in lowered
        or "ventas" in lowered
        or "monto de venta" in lowered
        or "monto venta" in lowered
        or "ingreso" in lowered
        or "ingresos" in lowered
        or bool(re.search(r"\bfactur", lowered))
    )
    wants_purchases = "compra" in lowered or "compras" in lowered
    wants_tax = "impuesto" in lowered or "iva" in lowered

    if percent and not (wants_sales or wants_purchases):
        return "¿Querés ese % sobre compras, ventas o ambos?"

    if wants_profit or wants_sales or wants_purchases or percent or wants_tax:
        date_range = _koda_parse_date_range(lowered)
        if not date_range:
            return "¿De qué fechas? Indicame desde y hasta (ej: 01/01/2026 al 11/01/2026)."
        start_dt, end_dt, start_date, end_date = date_range
        warehouse = _koda_extract_warehouse(lowered)
        range_label = f"{start_date.strftime('%d/%m/%Y')} al {end_date.strftime('%d/%m/%Y')}"
        reply_lines = [f"Rango: {range_label}."]
        if warehouse == Warehouse.WarehouseType.MERCADOLIBRE:
            reply_lines.append("Depósito: MercadoLibre.")
        elif warehouse == Warehouse.WarehouseType.COMUN:
            reply_lines.append("Depósito: Común.")

        sales_qs = Sale.objects.filter(created_at__gte=start_dt, created_at__lte=end_dt).select_related("warehouse").prefetch_related("items__product")
        sale_item_qs = SaleItem.objects.filter(sale__created_at__gte=start_dt, sale__created_at__lte=end_dt)
        purchase_qs = Purchase.objects.filter(created_at__gte=start_dt, created_at__lte=end_dt)
        tax_qs = TaxExpense.objects.filter(paid_at__gte=start_date, paid_at__lte=end_date)
        if warehouse:
            sales_qs = sales_qs.filter(warehouse__type=warehouse)
            sale_item_qs = sale_item_qs.filter(sale__warehouse__type=warehouse)
            purchase_qs = purchase_qs.filter(warehouse__type=warehouse)

        sales_total = sale_item_qs.aggregate(total=Sum("line_total")).get("total") or Decimal("0.00")
        purchases_total = purchase_qs.aggregate(total=Sum("total")).get("total") or Decimal("0.00")
        tax_total = tax_qs.aggregate(total=Sum("amount")).get("total") or Decimal("0.00")

        if wants_sales:
            reply_lines.append(f"Total ventas: {_koda_format_amount(sales_total)}.")
        if wants_purchases:
            reply_lines.append(f"Total compras: {_koda_format_amount(purchases_total)}.")
        if wants_profit:
            profit_sales = _koda_sales_profit(sales_qs)
            reply_lines.append(f"Ganancia bruta: {_koda_format_amount(profit_sales)}.")
            if sales_total and sales_total > 0:
                margin_pct = (profit_sales / sales_total * Decimal("100.00")).quantize(Decimal("0.01"))
                reply_lines.append(f"Margen promedio sobre ventas: {_koda_format_amount(margin_pct, currency=False)}%.")
            if tax_total:
                net_profit = profit_sales - tax_total
                reply_lines.append(f"Impuestos registrados: {_koda_format_amount(tax_total)}.")
                reply_lines.append(f"Ganancia neta después de impuestos: {_koda_format_amount(net_profit)}.")
                if sales_total and sales_total > 0:
                    net_margin_pct = (net_profit / sales_total * Decimal("100.00")).quantize(Decimal("0.01"))
                    reply_lines.append(f"Margen neto: {_koda_format_amount(net_margin_pct, currency=False)}%.")
        if wants_tax and not wants_profit:
            reply_lines.append(f"Impuestos registrados: {_koda_format_amount(tax_total)}.")
        if percent is not None and (wants_sales or wants_purchases):
            if wants_sales:
                sales_pct = (sales_total * percent / Decimal("100.00")).quantize(Decimal("1.00"))
                reply_lines.append(f"{percent}% de ventas: {_koda_format_amount(sales_pct)}.")
            if wants_purchases:
                purchases_pct = (purchases_total * percent / Decimal("100.00")).quantize(Decimal("1.00"))
                reply_lines.append(f"{percent}% de compras: {_koda_format_amount(purchases_pct)}.")
        return " ".join(reply_lines)

    return _koda_try_math(message)


def _koda_call_openai(messages, image_data_urls: list[str] | None = None) -> dict:
    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not api_key:
        return {"reply": "Falta configurar OPENAI_API_KEY.", "actions": [], "needs_confirmation": False}

    user_content = [{"type": "text", "text": messages[-1]["content"]}]
    for data_url in (image_data_urls or []):
        user_content.append({"type": "image_url", "image_url": {"url": data_url}})

    payload_messages = [{"role": "system", "content": _koda_system_prompt()}]
    payload_messages.extend(messages[:-1])
    payload_messages.append({"role": "user", "content": user_content})

    payload = {
        "model": "gpt-4.1",
        "temperature": 0.2,
        "messages": payload_messages,
    }
    request = Request(
        "https://api.openai.com/v1/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=30) as response:
            data = json.loads(response.read().decode("utf-8"))
        content = data["choices"][0]["message"]["content"]
    except Exception as exc:
        return {"reply": f"No pude contactar a OpenAI: {exc}", "actions": [], "needs_confirmation": False}

    try:
        parsed = json.loads(content)
        return {
            "reply": parsed.get("reply", ""),
            "actions": parsed.get("actions", []) or [],
            "needs_confirmation": bool(parsed.get("needs_confirmation")),
        }
    except Exception:
        return {"reply": content, "actions": [], "needs_confirmation": False}


def _koda_pdf_to_images(pdf_bytes: bytes, max_pages: int = 4) -> list[str]:
    """Render PDF pages to base64 JPEG data URLs using pymupdf. Returns [] if unavailable."""
    try:
        import fitz  # pymupdf
    except ImportError:
        return []
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        data_urls = []
        mat = fitz.Matrix(2.0, 2.0)  # 2x zoom for readability
        for page_num in range(min(doc.page_count, max_pages)):
            pix = doc[page_num].get_pixmap(matrix=mat)
            encoded = base64.b64encode(pix.tobytes("jpeg")).decode("utf-8")
            data_urls.append(f"data:image/jpeg;base64,{encoded}")
        doc.close()
        return data_urls
    except Exception:
        return []


def _koda_extract_pdf_text(path: str) -> tuple[str | None, str | None]:
    try:
        from pdfminer.high_level import extract_text
    except Exception:
        return None, "Falta pdfminer.six para leer PDFs."
    try:
        text = extract_text(path) or ""
        text = text.strip()
        return text, None
    except Exception as exc:
        return None, f"No pude leer el PDF: {exc}"


def _koda_actions_summary(actions: list[dict]) -> str:
    summaries = []
    for action in actions:
        action_type = action.get("type")
        data = action.get("data", {})
        if action_type == "create_product":
            summaries.append(f"Crear producto: {data.get('sku') or 'Sin SKU'} - {data.get('name')}")
        elif action_type == "add_stock_comun":
            items = data.get("items") or []
            summaries.append(f"Agregar stock Común: {len(items)} ítems")
        elif action_type == "transfer_to_ml":
            items = data.get("items") or []
            summaries.append(f"Transferir a MercadoLibre: {len(items)} ítems")
        elif action_type == "register_sale":
            items = data.get("items") or []
            summaries.append(f"Registrar venta: {len(items)} ítems")
        elif action_type == "update_sale":
            summaries.append("Actualizar venta")
        elif action_type == "register_purchase":
            items = data.get("items") or []
            summaries.append(f"Registrar compra: {len(items)} ítems")
        else:
            summaries.append(f"Acción: {action_type}")
    return " | ".join(summaries)


def _koda_resolve_product(identifier: str) -> Product | None:
    if not identifier:
        return None
    raw = identifier.strip()
    product = Product.objects.filter(sku__iexact=raw).first()
    if product:
        return product
    product = Product.objects.filter(name__iexact=raw).first()
    if product:
        return product
    normalized = _normalize_header(raw)
    tokens = [token for token in normalized.split() if len(token) > 2]
    if not tokens:
        return None
    candidates = Product.objects.filter(name__icontains=tokens[0])[:50]
    best = None
    best_score = 0
    for candidate in candidates:
        name_norm = _normalize_header(candidate.name)
        score = sum(1 for token in tokens if token in name_norm)
        if score > best_score:
            best_score = score
            best = candidate
    return best


def _koda_resolve_variant(product: Product, variant_name: str | None) -> ProductVariant | None:
    if not variant_name:
        return None
    return ProductVariant.objects.filter(product=product, name__iexact=variant_name.strip()).first()


def _koda_resolve_supplier(name: str | None) -> Supplier | None:
    if not name:
        return None
    raw = name.strip()
    supplier = Supplier.objects.filter(name__iexact=raw).first()
    if supplier:
        return supplier
    return Supplier.objects.filter(name__icontains=raw).first()


def _koda_resolve_sale(data: dict) -> Sale | None:
    sale_id = data.get("sale_id")
    if sale_id:
        return Sale.objects.filter(id=sale_id).first()
    invoice_number = (data.get("invoice_number") or "").strip()
    if invoice_number:
        parts = invoice_number.split("-")
        tail = parts[-1] if parts else invoice_number
        if tail.isdigit():
            return Sale.objects.filter(id=int(tail)).first()
    return None


def _koda_sync_common_with_variants(product: Product, comun_wh: Warehouse):
    from django.db.models import Sum as _Sum
    total = (
        ProductVariant.objects.filter(product=product)
        .aggregate(total=_Sum("quantity"))
        .get("total")
    )
    total = total if total is not None else Decimal("0.00")
    stock = Stock.objects.select_for_update().get_or_create(
        product=product,
        warehouse=comun_wh,
        defaults={"quantity": total},
    )[0]
    stock.quantity = total
    stock.save(update_fields=["quantity"])


def _koda_execute_actions(actions: list[dict], user, invoice_path: str | None) -> list[str]:
    results = []
    comun_wh = Warehouse.objects.filter(type=Warehouse.WarehouseType.COMUN).first()
    ml_wh = Warehouse.objects.filter(type=Warehouse.WarehouseType.MERCADOLIBRE).first()
    for action in actions:
        action_type = action.get("type")
        data = action.get("data", {})
        if action_type == "create_product":
            name = (data.get("name") or "").strip()
            if not name:
                raise ValueError("Falta el nombre del producto.")
            sku = (data.get("sku") or "").strip()
            group = (data.get("group") or "").strip()
            if not sku:
                prefix = _sku_prefix(group or "", name)
                existing = Product.objects.filter(sku__startswith=prefix).values_list("sku", flat=True)
                max_suffix = 0
                for existing_sku in existing:
                    suffix = existing_sku[len(prefix):]
                    if suffix.isdigit():
                        max_suffix = max(max_suffix, int(suffix))
                sku = f"{prefix}{max_suffix + 1:04d}"
            product = Product.objects.create(
                sku=sku,
                name=name,
                group=group,
                avg_cost=Decimal(str(data.get("avg_cost") or "0.00")),
                vat_percent=Decimal(str(data.get("vat_percent") or "0.00")),
                price_consumer=Decimal(str(data.get("price_consumer") or "0.00")),
                price_barber=Decimal(str(data.get("price_barber") or "0.00")),
                price_distributor=Decimal(str(data.get("price_distributor") or "0.00")),
            )
            results.append(f"Producto creado: {product.sku} - {product.name}")
        elif action_type == "add_stock_comun":
            if not comun_wh:
                raise ValueError("No está configurado el depósito Común.")
            items = data.get("items") or []
            if not items:
                raise ValueError("No hay ítems para agregar stock.")
            with transaction.atomic():
                for item in items:
                    product = _koda_resolve_product(item.get("product", ""))
                    if not product:
                        raise ValueError("Producto no encontrado.")
                    quantity = Decimal(str(item.get("quantity") or "0"))
                    if quantity <= 0:
                        raise ValueError("Cantidad inválida.")
                    variant = _koda_resolve_variant(product, item.get("variant"))
                    if not variant and ProductVariant.objects.filter(product=product).exists():
                        raise ValueError(f"El producto {product.sku} requiere variedad.")
                    unit_cost = item.get("unit_cost")
                    services.register_adjustment(
                        product=product,
                        warehouse=comun_wh,
                        quantity=quantity,
                        user=user,
                        unit_cost=Decimal(str(unit_cost)) if unit_cost is not None else None,
                        reference="Koda: ajuste de stock Común",
                    )
                    if variant:
                        variant.quantity = (variant.quantity + quantity).quantize(Decimal("0.01"))
                        variant.save(update_fields=["quantity"])
                        _koda_sync_common_with_variants(product, comun_wh)
            results.append(f"Stock Común actualizado: {len(items)} ítems.")
        elif action_type == "transfer_to_ml":
            if not comun_wh or not ml_wh:
                raise ValueError("Faltan depósitos configurados para transferencias.")
            items = data.get("items") or []
            if not items:
                raise ValueError("No hay ítems para transferir.")
            with transaction.atomic():
                for item in items:
                    product = _koda_resolve_product(item.get("product", ""))
                    if not product:
                        raise ValueError("Producto no encontrado.")
                    quantity = Decimal(str(item.get("quantity") or "0"))
                    if quantity <= 0:
                        raise ValueError("Cantidad inválida.")
                    variant = _koda_resolve_variant(product, item.get("variant"))
                    if not variant and ProductVariant.objects.filter(product=product).exists():
                        raise ValueError(f"El producto {product.sku} requiere variedad.")
                    if variant:
                        _koda_sync_common_with_variants(product, comun_wh)
                        if variant.quantity - quantity < 0:
                            raise services.NegativeStockError("Stock insuficiente en variedad.")
                        variant.quantity = (variant.quantity - quantity).quantize(Decimal("0.01"))
                        variant.save(update_fields=["quantity"])
                        _koda_sync_common_with_variants(product, comun_wh)
                    services.register_transfer(
                        product=product,
                        from_warehouse=comun_wh,
                        to_warehouse=ml_wh,
                        quantity=quantity,
                        user=user,
                        reference="Koda: transferencia Común -> MercadoLibre",
                    )
            results.append(f"Transferencias a ML: {len(items)} ítems.")
        elif action_type == "register_sale":
            warehouse_label = (data.get("warehouse") or "COMUN").strip().lower()
            warehouse_code = {
                "comun": Warehouse.WarehouseType.COMUN,
                "común": Warehouse.WarehouseType.COMUN,
                "mercadolibre": Warehouse.WarehouseType.MERCADOLIBRE,
                "mercado libre": Warehouse.WarehouseType.MERCADOLIBRE,
                "ml": Warehouse.WarehouseType.MERCADOLIBRE,
            }.get(warehouse_label, warehouse_label.upper())
            warehouse = Warehouse.objects.filter(type=warehouse_code).first()
            if not warehouse:
                raise ValueError("Depósito no encontrado.")
            customer = None
            if data.get("customer"):
                customer = Customer.objects.filter(name__iexact=data.get("customer")).first()
            audience = data.get("audience") or Customer.Audience.CONSUMER
            items = data.get("items") or []
            if not items:
                raise ValueError("No hay ítems para la venta.")
            with transaction.atomic():
                sale = Sale.objects.create(
                    customer=customer,
                    warehouse=warehouse,
                    audience=audience,
                    total=Decimal("0.00"),
                    discount_total=Decimal("0.00"),
                    reference="Koda: venta",
                    user=user,
                )
                total = Decimal("0.00")
                for item in items:
                    product = _koda_resolve_product(item.get("product", ""))
                    if not product:
                        raise ValueError("Producto no encontrado.")
                    quantity = Decimal(str(item.get("quantity") or "0"))
                    if quantity <= 0:
                        raise ValueError("Cantidad inválida.")
                    unit_price = item.get("unit_price")
                    if unit_price is None:
                        if audience == Customer.Audience.BARBER:
                            unit_price = product.barber_price
                        elif audience == Customer.Audience.DISTRIBUTOR:
                            unit_price = product.distributor_price
                        else:
                            unit_price = product.consumer_price
                    unit_price = Decimal(str(unit_price))
                    line_total = (unit_price * quantity).quantize(Decimal("0.01"))
                    SaleItem.objects.create(
                        sale=sale,
                        product=product,
                        quantity=quantity,
                        unit_price=unit_price,
                        cost_unit=product.last_purchase_cost(),
                        discount_percent=Decimal("0.00"),
                        final_unit_price=unit_price,
                        line_total=line_total,
                        vat_percent=Decimal(str(item.get("vat_percent") or "0.00")),
                    )
                    total += line_total
                    if warehouse.type != Warehouse.WarehouseType.MERCADOLIBRE:
                        services.register_exit(
                            product=product,
                            warehouse=warehouse,
                            quantity=quantity,
                            user=user,
                            reference=f"Koda: venta #{sale.id}",
                            sale_price=unit_price,
                            vat_percent=Decimal(str(item.get("vat_percent") or "0.00")),
                            sale=sale,
                        )
                sale.total = total
                sale.save(update_fields=["total"])
            results.append(f"Venta registrada #{sale.id}.")
        elif action_type == "update_sale":
            sale = _koda_resolve_sale(data)
            if not sale:
                raise ValueError("Venta no encontrada.")
            update_fields = []
            sale_date = data.get("sale_date")
            if sale_date:
                try:
                    created_at = datetime.fromisoformat(str(sale_date))
                except Exception:
                    created_at = datetime.combine(str(sale_date), time(12, 0))
                if timezone.is_naive(created_at):
                    created_at = timezone.make_aware(created_at)
                sale.created_at = created_at
                update_fields.append("created_at")
            if data.get("total") is not None:
                sale.total = Decimal(str(data.get("total")))
                update_fields.append("total")
            if data.get("ml_commission_total") is not None:
                sale.ml_commission_total = Decimal(str(data.get("ml_commission_total")))
                update_fields.append("ml_commission_total")
            if data.get("ml_tax_total") is not None:
                sale.ml_tax_total = Decimal(str(data.get("ml_tax_total")))
                update_fields.append("ml_tax_total")
            if update_fields:
                sale.save(update_fields=update_fields)
            results.append(f"Venta actualizada #{sale.id}.")
        elif action_type == "register_purchase":
            warehouse_label = (data.get("warehouse") or "COMUN").strip().lower()
            warehouse_code = {
                "comun": Warehouse.WarehouseType.COMUN,
                "común": Warehouse.WarehouseType.COMUN,
                "mercadolibre": Warehouse.WarehouseType.MERCADOLIBRE,
                "mercado libre": Warehouse.WarehouseType.MERCADOLIBRE,
                "ml": Warehouse.WarehouseType.MERCADOLIBRE,
            }.get(warehouse_label, warehouse_label.upper())
            warehouse = Warehouse.objects.filter(type=warehouse_code).first()
            if not warehouse:
                raise ValueError("Depósito no encontrado.")
            supplier_name = (data.get("supplier") or "").strip()
            if not supplier_name:
                raise ValueError("Falta proveedor.")
            supplier = _koda_resolve_supplier(supplier_name)
            if not supplier:
                supplier = Supplier.objects.create(name=supplier_name)
            items = data.get("items") or []
            if not items:
                raise ValueError("No hay ítems para la compra.")
            with transaction.atomic():
                purchase = Purchase.objects.create(
                    supplier=supplier,
                    warehouse=warehouse,
                    reference=data.get("reference") or "Koda: compra",
                    user=user,
                )
                total = Decimal("0.00")
                avg_cost_tracker: list[dict] = []
                for item in items:
                    product = _koda_resolve_product(item.get("product", ""))
                    if not product:
                        raise ValueError(f"Producto no encontrado: {item.get('product', '')!r}")
                    quantity = Decimal(str(item.get("quantity") or "0"))
                    if quantity <= 0:
                        raise ValueError("Cantidad inválida.")
                    unit_cost_raw = item.get("unit_cost")
                    vat_raw = item.get("vat_percent")
                    discount_raw = item.get("discount_percent") or "0"
                    discount = Decimal(str(discount_raw)).quantize(Decimal("0.01"))

                    if unit_cost_raw is not None:
                        unit_cost_base = Decimal(str(unit_cost_raw)).quantize(Decimal("0.01"))
                        vat = Decimal(str(vat_raw or "0.00")).quantize(Decimal("0.01"))
                    else:
                        # Fallback: use existing avg_cost (already includes VAT), no re-apply
                        unit_cost_base = product.avg_cost or Decimal("0.00")
                        vat = Decimal("0.00")

                    if discount >= Decimal("100.00"):
                        effective_base = Decimal("0.00")
                        vat = Decimal("0.00")
                    elif discount > 0:
                        effective_base = (unit_cost_base * (1 - discount / 100)).quantize(
                            Decimal("0.01"), rounding=ROUND_HALF_UP
                        )
                    else:
                        effective_base = unit_cost_base

                    cost_with_vat = (
                        (effective_base * (1 + vat / 100)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                        if vat > 0
                        else effective_base
                    )

                    PurchaseItem.objects.create(
                        purchase=purchase,
                        product=product,
                        quantity=quantity,
                        unit_cost=effective_base,
                        discount_percent=discount,
                        vat_percent=vat,
                    )
                    services.register_entry(
                        product=product,
                        warehouse=warehouse,
                        quantity=quantity,
                        unit_cost=effective_base,
                        vat_percent=vat if vat_raw is not None else None,
                        user=user,
                        reference=f"Koda: compra #{purchase.id}",
                        supplier=supplier,
                        purchase=purchase,
                    )
                    avg_cost_tracker.append({"product": product, "qty": quantity, "cost_with_vat": cost_with_vat})
                    total += (quantity * cost_with_vat).quantize(Decimal("0.01"))

                update_product_avg_costs(avg_cost_tracker)
                purchase.total = total
                if invoice_path:
                    with open(invoice_path, "rb") as handle:
                        purchase.invoice_image.save(
                            os.path.basename(invoice_path),
                            ContentFile(handle.read()),
                            save=False,
                        )
                purchase.save()
            results.append(f"Compra registrada #{purchase.id}: {len(items)} ítems, total {_koda_format_amount(total)}.")
        else:
            raise ValueError(f"Acción desconocida: {action_type}")
    return results


@login_required
@require_http_methods(["POST"])
def koda_chat(request):
    if not _koda_allowed(request.user):
        return JsonResponse({"reply": "No tenés permisos para usar Koda.", "actions": []})

    message = ""
    image_file = None
    if request.content_type and request.content_type.startswith("multipart/form-data"):
        message = (request.POST.get("message") or "").strip()
        image_file = request.FILES.get("image")
    else:
        try:
            payload = json.loads(request.body.decode("utf-8"))
            message = (payload.get("message") or "").strip()
        except Exception:
            message = ""

    if not message and not image_file:
        return JsonResponse({"reply": "Decime qué necesitás.", "actions": []})

    if not image_file:
        local_reply = _koda_try_local_response(message)
        if local_reply:
            history = request.session.get("koda_history", [])
            history = history[-8:]
            history.append({"role": "user", "content": message})
            history.append({"role": "assistant", "content": local_reply})
            request.session["koda_history"] = history[-10:]
            return JsonResponse({"reply": local_reply, "needs_confirmation": False, "summary": ""})

    image_data_urls: list[str] = []
    pending_file_path = None
    if image_file:
        from django.conf import settings as django_settings
        raw = image_file.read()
        pending_dir = os.path.join(str(django_settings.MEDIA_ROOT), "koda_pending")
        os.makedirs(pending_dir, exist_ok=True)
        filename = f"{secrets.token_hex(8)}_{image_file.name}"
        pending_file_path = os.path.join(pending_dir, filename)
        with open(pending_file_path, "wb") as handle:
            handle.write(raw)

        if image_file.content_type and image_file.content_type.startswith("image/"):
            encoded = base64.b64encode(raw).decode("utf-8")
            image_data_urls = [f"data:{image_file.content_type};base64,{encoded}"]
            if not message:
                message = "Registrá esta factura de compra: extraé proveedor, número, todos los ítems con cantidad, precio unitario sin IVA y porcentaje de IVA por línea."
            elif "imagen" not in message.lower() and "factura" not in message.lower():
                message = f"{message}\n\nUsá la imagen adjunta para extraer los datos."
        elif image_file.content_type == "application/pdf":
            pdf_text, pdf_error = _koda_extract_pdf_text(pending_file_path)
            if pdf_error:
                return JsonResponse({"reply": pdf_error, "actions": []})
            if pdf_text:
                # Text-based PDF: send content as text
                message = f"{message}\n\nContenido del PDF:\n{pdf_text}".strip()
            else:
                # Scanned PDF: render pages as images for vision
                image_data_urls = _koda_pdf_to_images(raw)
                if not image_data_urls:
                    return JsonResponse(
                        {"reply": "El PDF no tiene texto legible y no pude renderizarlo como imagen. Intentá subir una foto de la factura.", "actions": []}
                    )
                if not message:
                    message = "Registrá esta factura de compra: extraé proveedor, número, todos los ítems con cantidad, precio unitario sin IVA y porcentaje de IVA por línea."

    history = request.session.get("koda_history", [])
    history = history[-8:]
    messages = history + [{"role": "user", "content": message}]
    result = _koda_call_openai(messages, image_data_urls=image_data_urls or None)

    actions = result.get("actions") or []
    needs_confirmation = bool(result.get("needs_confirmation")) and bool(actions)
    reply = result.get("reply") or ""
    lowered = reply.lower()
    if "no tengo acceso" in lowered or "no tengo acceso directo" in lowered or "no puedo acceder" in lowered:
        reply = "Puedo gestionarlo. Confirmame los datos exactos para proceder."
    if actions and not needs_confirmation:
        needs_confirmation = True
    if not actions and any(word in lowered for word in ("registr", "cre", "transfer", "actualic", "listo", "confirm")):
        reply = "Puedo hacerlo, pero necesito confirmación y los datos exactos para ejecutar."

    if needs_confirmation:
        request.session["koda_pending"] = {
            "actions": actions,
            "image_path": pending_file_path,
        }
    else:
        if pending_file_path:
            os.remove(pending_file_path)

    history.append({"role": "user", "content": message})
    history.append({"role": "assistant", "content": reply})
    request.session["koda_history"] = history[-10:]

    return JsonResponse(
        {
            "reply": reply,
            "needs_confirmation": needs_confirmation,
            "summary": _koda_actions_summary(actions) if needs_confirmation else "",
        }
    )


@login_required
@require_http_methods(["POST"])
def koda_confirm(request):
    if not _koda_allowed(request.user):
        return JsonResponse({"reply": "No tenés permisos para usar Koda."}, status=403)

    try:
        payload = json.loads(request.body.decode("utf-8"))
    except Exception:
        payload = {}
    decision = payload.get("decision")
    pending = request.session.get("koda_pending")
    if not pending:
        return JsonResponse({"reply": "No hay acciones pendientes."})

    image_path = pending.get("image_path")
    if decision == "cancel":
        if image_path and os.path.exists(image_path):
            os.remove(image_path)
        request.session.pop("koda_pending", None)
        return JsonResponse({"reply": "Acción cancelada."})

    try:
        results = _koda_execute_actions(pending.get("actions") or [], request.user, image_path)
        reply = "Listo. " + " ".join(results)
    except Exception as exc:
        reply = f"No pude ejecutar la acción: {exc}"
    finally:
        if image_path and os.path.exists(image_path):
            os.remove(image_path)
        request.session.pop("koda_pending", None)

    return JsonResponse({"reply": reply})
