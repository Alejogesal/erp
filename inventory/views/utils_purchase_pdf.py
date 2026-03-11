"""PDF parsing helpers for purchase imports."""
from decimal import Decimal, ROUND_HALF_UP
import io
import re

from django.db.models import Q

from ..models import Product, Supplier
from .common import _normalize_lookup_text
from .utils_xlsx import _sku_prefix


def _purchase_pdf_match_tokens(value: str) -> list[str]:
    normalized = _normalize_lookup_text(value)
    if not normalized:
        return []
    alias = {
        "enj": "enjuague",
        "unid": "",
        "unidad": "",
        "unidades": "",
        "ml": "",
        "lts": "",
        "lt": "",
        "gr": "",
        "g": "",
    }
    stopwords = {"de", "del", "con", "sin", "para", "por", "the", "la", "el"}
    tokens: list[str] = []
    for raw_tok in normalized.split():
        tok = alias.get(raw_tok, raw_tok)
        if not tok:
            continue
        if re.fullmatch(r"x\d+[a-z]*", tok):
            continue
        if re.fullmatch(r"\d+[a-z]*", tok):
            continue
        if tok in stopwords:
            continue
        if len(tok) >= 5 and tok.endswith("s"):
            tok = tok[:-1]
        if len(tok) < 3:
            continue
        tokens.append(tok)
    return tokens


def _parse_latam_decimal(raw: str | None) -> Decimal | None:
    if raw is None:
        return None
    value = str(raw).strip()
    if not value:
        return None
    value = value.replace("$", "").replace(" ", "")
    value = value.replace("%", "").replace("+", "")
    value = value.replace("\u00a0", "")
    value = value.replace(".", "").replace(",", ".")
    try:
        return Decimal(value)
    except Exception:
        return None


def _extract_purchase_items_from_pdf_bytes(pdf_bytes: bytes) -> tuple[list[dict], dict, str | None]:
    try:
        from pdfminer.high_level import extract_text
    except Exception:
        return [], {}, "Falta pdfminer.six para leer PDFs."

    try:
        text = (extract_text(io.BytesIO(pdf_bytes)) or "").strip()
    except Exception as exc:
        return [], {}, f"No se pudo leer el PDF: {exc}"
    if not text:
        return [], {}, "El PDF no contiene texto legible."

    compact_text = re.sub(r"\s+", " ", text)
    metadata: dict[str, str] = {}
    invoice_match = re.search(r"N[°ºo]?\s*([0-9]{4,5}-[0-9]{4,8})", compact_text, flags=re.IGNORECASE)
    if invoice_match:
        metadata["invoice_number"] = invoice_match.group(1)
    date_match = re.search(r"Fecha[:\s]+(\d{1,2}/\d{1,2}/\d{2,4})", compact_text, flags=re.IGNORECASE)
    if date_match:
        metadata["date"] = date_match.group(1)

    lines = [((line or "").replace("\u00a0", " ").rstrip()) for line in text.splitlines()]
    lines = [line for line in lines if line.strip()]
    in_table = False
    pending_descriptions: list[str] = []
    parsed_items: list[dict] = []
    row_re = re.compile(
        r"^(?P<desc>.+?)\s+"
        r"(?P<qty>\d[\d\.,]*)\s+"
        r"\$?\s*(?P<price>\d[\d\.,]*)\s+"
        r"(?:(?P<discount>-?\s*\d[\d\.,]*)\s*%?\s+)?"
        r"\$?\s*(?P<amount>\d[\d\.,]*)\s*$",
        flags=re.IGNORECASE,
    )
    numeric_row_re = re.compile(
        r"^(?P<qty>\d[\d\.,]*)\s+"
        r"\$?\s*(?P<price>\d[\d\.,]*)\s+"
        r"(?:(?P<discount>-?\s*\d[\d\.,]*)\s*%?\s+)?"
        r"\$?\s*(?P<amount>\d[\d\.,]*)\s*$",
        flags=re.IGNORECASE,
    )

    for raw_line in lines:
        line = raw_line.strip()
        normalized = _normalize_lookup_text(line)
        if not in_table:
            header_detected = "descripcion" in normalized and "cantidad" in normalized and "precio" in normalized
            row_candidate = bool(re.search(r"\d[\d\.,]*\s+\$?\s*\d[\d\.,]*\s+\$?\s*\d[\d\.,]*$", line))
            if header_detected or row_candidate:
                in_table = True
            if not in_table:
                continue
        if "descripcion" in normalized and "cantidad" in normalized and "precio" in normalized:
            continue
        if normalized.startswith("subtotal") or normalized.startswith("total ") or normalized == "total" or normalized.startswith("descuento"):
            break

        row_parsed = None
        parts = [part.strip() for part in re.split(r"\s{2,}", line) if part.strip()]
        if len(parts) >= 4:
            qty_candidate = _parse_latam_decimal(parts[1])
            price_candidate = _parse_latam_decimal(parts[2])
            amount_candidate = _parse_latam_decimal(parts[-1])
            if qty_candidate is not None and price_candidate is not None and amount_candidate is not None:
                discount_candidate = Decimal("0.00")
                if len(parts) >= 5:
                    discount_candidate = _parse_latam_decimal(parts[3]) or Decimal("0.00")
                    if discount_candidate < 0:
                        discount_candidate = abs(discount_candidate)
                row_parsed = {
                    "description": parts[0],
                    "quantity": qty_candidate,
                    "unit_cost": price_candidate,
                    "discount_percent": discount_candidate,
                    "line_total": amount_candidate,
                }

        if row_parsed is None:
            collapsed = re.sub(r"\s+", " ", line)
            match = row_re.match(collapsed)
        else:
            match = None

        if row_parsed is None and not match:
            numeric_only = numeric_row_re.match(re.sub(r"\s+", " ", line))
            if numeric_only and pending_descriptions:
                qty = _parse_latam_decimal(numeric_only.group("qty"))
                unit_cost = _parse_latam_decimal(numeric_only.group("price"))
                discount_raw = (numeric_only.group("discount") or "").replace(" ", "")
                discount = _parse_latam_decimal(discount_raw) or Decimal("0.00")
                if discount < 0:
                    discount = abs(discount)
                row_parsed = {
                    "description": pending_descriptions.pop(0),
                    "quantity": qty,
                    "unit_cost": unit_cost,
                    "discount_percent": discount,
                    "line_total": _parse_latam_decimal(numeric_only.group("amount")),
                }
            else:
                if re.search(r"[A-Za-z]", line):
                    pending_descriptions.append(line[:140])
                continue

        if row_parsed is None and match:
            desc = match.group("desc").strip()
            qty = _parse_latam_decimal(match.group("qty"))
            unit_cost = _parse_latam_decimal(match.group("price"))
            discount_raw = (match.group("discount") or "").replace(" ", "")
            discount = _parse_latam_decimal(discount_raw) or Decimal("0.00")
            if discount < 0:
                discount = abs(discount)
            row_parsed = {
                "description": desc,
                "quantity": qty,
                "unit_cost": unit_cost,
                "discount_percent": discount,
                "line_total": _parse_latam_decimal(match.group("amount")),
            }

        desc = row_parsed["description"].strip()
        qty = row_parsed["quantity"]
        unit_cost = row_parsed["unit_cost"]
        discount = row_parsed["discount_percent"]
        if qty is None or qty <= 0 or unit_cost is None or unit_cost < 0:
            continue

        parsed_items.append(
            {
                "description": desc,
                "quantity": qty,
                "unit_cost": unit_cost,
                "discount_percent": discount,
                "line_total": row_parsed.get("line_total"),
            }
        )

    layout_items = _extract_purchase_items_from_pdf_layout(pdf_bytes)
    parsed_items = _pick_best_purchase_pdf_parse(parsed_items, layout_items)
    if not parsed_items:
        return [], metadata, (
            "No se encontraron ítems en el PDF. "
            "Verificá que tenga una tabla con columnas descripción, cantidad y precio."
        )
    return parsed_items, metadata, None


def _extract_purchase_items_from_pdf_layout(pdf_bytes: bytes) -> list[dict]:
    try:
        from pdfminer.high_level import extract_pages
        from pdfminer.layout import LTTextContainer
    except Exception:
        return []

    def _group_rows(cells: list[tuple[float, float, str]]) -> list[list[tuple[float, float, str]]]:
        rows: list[list[tuple[float, float, str]]] = []
        tolerance = 3.0
        for x0, y0, text in sorted(cells, key=lambda c: -c[1]):
            placed = False
            for row in rows:
                if abs(row[0][1] - y0) <= tolerance:
                    row.append((x0, y0, text))
                    placed = True
                    break
            if not placed:
                rows.append([(x0, y0, text)])
        for row in rows:
            row.sort(key=lambda c: c[0])
        return rows

    parsed_items: list[dict] = []
    for page_layout in extract_pages(io.BytesIO(pdf_bytes)):
        cells: list[tuple[float, float, str]] = []
        for element in page_layout:
            if not isinstance(element, LTTextContainer):
                continue
            text = re.sub(r"\s+", " ", element.get_text().replace("\u00a0", " ").strip())
            if not text:
                continue
            cells.append((float(element.x0), float(element.y0), text))
        if not cells:
            continue

        rows = _group_rows(cells)
        header_row = None
        x_qty = x_price = x_discount = x_amount = None
        for row in rows:
            joined = _normalize_lookup_text(" ".join(part[2] for part in row))
            if "descripcion" in joined and "cantidad" in joined and "precio" in joined:
                header_row = row
                for x0, _, txt in row:
                    n = _normalize_lookup_text(txt)
                    if "cantidad" in n:
                        x_qty = x0
                    elif n == "precio" or "precio" in n:
                        x_price = x0
                    elif "dto" in n or "descuento" in n:
                        x_discount = x0
                    elif "importe" in n or "total" == n:
                        x_amount = x0
                break
        if not header_row or x_qty is None or x_price is None:
            continue

        header_y = header_row[0][1]
        pending_descriptions: list[str] = []
        for row in rows:
            y0 = row[0][1]
            if y0 >= header_y:
                continue
            line_text = _normalize_lookup_text(" ".join(part[2] for part in row))
            if line_text.startswith("subtotal") or line_text.startswith("descuento") or line_text.startswith("total"):
                break

            desc_parts: list[str] = []
            qty_raw = price_raw = discount_raw = amount_raw = None
            for x0, _, txt in row:
                if x0 < x_qty - 5:
                    desc_parts.append(txt)
                    continue
                if qty_raw is None and x0 < x_price - 10:
                    qty_raw = txt
                    continue
                if price_raw is None and (x_discount is None or x0 < x_discount - 10):
                    price_raw = txt
                    continue
                if x_discount is not None and discount_raw is None and (x_amount is None or x0 < x_amount - 10):
                    discount_raw = txt
                    continue
                if amount_raw is None:
                    amount_raw = txt

            desc = re.sub(r"\s+", " ", " ".join(desc_parts)).strip()
            qty = _parse_latam_decimal(qty_raw)
            price = _parse_latam_decimal(price_raw)
            amount = _parse_latam_decimal(amount_raw)
            discount = _parse_latam_decimal(discount_raw) or Decimal("0.00")
            if discount < 0:
                discount = abs(discount)

            if qty is None or price is None or amount is None:
                if desc and re.search(r"[A-Za-z]", desc):
                    pending_descriptions.append(desc[:140])
                continue

            if pending_descriptions and (not desc or desc == "(sin descripción)"):
                desc = pending_descriptions.pop(0)
            if not desc:
                desc = "(sin descripción)"
            if qty <= 0 or price < 0:
                continue
            parsed_items.append(
                {
                    "description": desc,
                    "quantity": qty,
                    "unit_cost": price,
                    "discount_percent": discount,
                    "line_total": amount,
                }
            )
    return parsed_items


def _purchase_pdf_parse_score(items: list[dict]) -> float:
    if not items:
        return -1.0
    score = float(len(items) * 10)
    for entry in items:
        desc = str(entry.get("description") or "").strip()
        qty = entry.get("quantity")
        unit_cost = entry.get("unit_cost")
        if not desc:
            score -= 6
            continue
        words = desc.split()
        if len(words) > 12:
            score -= (len(words) - 12) * 0.8
        if len(desc) > 110:
            score -= 8
        if qty is None:
            score -= 4
        if unit_cost is None:
            score -= 4
    return score


def _pick_best_purchase_pdf_parse(text_items: list[dict], layout_items: list[dict]) -> list[dict]:
    text_count = len(text_items)
    layout_count = len(layout_items)
    if layout_count >= text_count + 2:
        return layout_items
    if text_count >= layout_count + 2:
        return text_items

    text_score = _purchase_pdf_parse_score(text_items)
    layout_score = _purchase_pdf_parse_score(layout_items)
    if layout_score > text_score + 5:
        return layout_items
    if text_score >= 0:
        return text_items
    return layout_items


def _normalize_purchase_pdf_item_fields(item: dict) -> dict:
    qty = item.get("quantity")
    unit_cost = item.get("unit_cost")
    if qty is None or unit_cost is None:
        return item
    try:
        qty = Decimal(qty)
        unit_cost = Decimal(unit_cost)
    except Exception:
        return item
    # Some supplier PDFs can flip qty/price columns in layout extraction.
    # If qty looks unrealistically large and unit cost looks like a small count, swap them.
    if qty > Decimal("100.00") and unit_cost <= Decimal("50.00"):
        item["quantity"] = unit_cost
        item["unit_cost"] = qty
        qty = item["quantity"]
        unit_cost = item["unit_cost"]

    line_total = item.get("line_total")
    discount = item.get("discount_percent")
    try:
        line_total = Decimal(line_total) if line_total is not None else None
    except Exception:
        line_total = None
    try:
        discount = Decimal(discount) if discount is not None else Decimal("0.00")
    except Exception:
        discount = Decimal("0.00")
    if line_total is not None and qty > 0:
        net_unit = (line_total / qty).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        if discount > 0 and discount < 100:
            factor = Decimal("1.00") - (discount / Decimal("100.00"))
            if factor > 0:
                gross_unit = (net_unit / factor).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                if abs(unit_cost - gross_unit) >= Decimal("0.05"):
                    item["unit_cost"] = gross_unit
        else:
            if abs(unit_cost - net_unit) >= Decimal("0.05"):
                item["unit_cost"] = net_unit
    return item


def _resolve_product_from_purchase_pdf(description: str) -> Product | None:
    from difflib import SequenceMatcher
    raw = (description or "").strip()
    if not raw:
        return None
    product = Product.objects.filter(sku__iexact=raw).first()
    if product:
        return product
    product = Product.objects.filter(name__iexact=raw).first()
    if product:
        return product

    normalized_target = _normalize_lookup_text(raw)
    target_tokens_list = _purchase_pdf_match_tokens(raw)
    if not normalized_target or not target_tokens_list:
        return None

    # Strong fallback: exact match on normalized label (handles punctuation/accents/case differences).
    normalized_compact = normalized_target.replace(" ", "")
    for candidate in Product.objects.only("id", "name", "sku"):
        name_norm = _normalize_lookup_text(candidate.name or "")
        sku_norm = _normalize_lookup_text(candidate.sku or "")
        full_norm = _normalize_lookup_text(f"{candidate.sku or ''} {candidate.name or ''}")
        if name_norm == normalized_target or full_norm == normalized_target:
            return candidate
        if name_norm.replace(" ", "") == normalized_compact:
            return candidate
        if sku_norm and sku_norm == normalized_target:
            return candidate

    token_set = set(target_tokens_list)
    clauses = Q()
    for token in target_tokens_list[:6]:
        clauses |= Q(name__icontains=token) | Q(sku__icontains=token)
    candidates = list(Product.objects.filter(clauses).only("id", "name", "sku")[:200])
    if not candidates:
        return None

    def _score(candidate: Product) -> tuple[float, float, float]:
        candidate_text = _normalize_lookup_text(f"{candidate.sku or ''} {candidate.name or ''}")
        candidate_tokens = set(_purchase_pdf_match_tokens(f"{candidate.sku or ''} {candidate.name or ''}"))
        overlap_count = len(token_set & candidate_tokens)
        overlap_ratio = overlap_count / max(len(token_set), 1)
        text_ratio = SequenceMatcher(None, " ".join(target_tokens_list), " ".join(sorted(candidate_tokens))).ratio()
        # Overlap drives precision; text ratio helps when order/spacing differs.
        score_value = (overlap_ratio * 0.7) + (text_ratio * 0.3)
        return score_value, overlap_ratio, text_ratio

    ranked: list[tuple[float, float, float, Product]] = []
    for candidate in candidates:
        score_value, overlap_ratio, text_ratio = _score(candidate)
        ranked.append((score_value, overlap_ratio, text_ratio, candidate))
    ranked.sort(key=lambda row: row[0], reverse=True)

    best_score, best_overlap, best_ratio, best = ranked[0]
    second_score = ranked[1][0] if len(ranked) > 1 else 0.0

    # Require solid confidence and a margin from the second best to avoid wrong auto-matches.
    if best_overlap < 0.50 and best_ratio < 0.72:
        return None
    if best_score < 0.55:
        return None
    if (best_score - second_score) < 0.05:
        return None

    # Prefer strict SKU token containment when available.
    sku_tokens = {tok for tok in _normalize_lookup_text(best.sku or "").split() if len(tok) >= 3}
    if sku_tokens and not (sku_tokens & token_set):
        # If matched only by a weak name similarity and SKU doesn't align at all, reject.
        if best_ratio < 0.86:
            return None
    return best


def _create_product_from_purchase_pdf(
    description: str,
    supplier: Supplier | None,
    unit_cost_with_vat: Decimal | None = None,
    vat_percent: Decimal | None = None,
) -> Product | None:
    name = (description or "").strip()
    if not name:
        return None
    if len(name) > 255:
        name = name[:255].strip()
    normalized = _normalize_lookup_text(name)
    if normalized:
        for candidate in Product.objects.only("id", "name"):
            if _normalize_lookup_text(candidate.name or "") == normalized:
                return candidate
    prefix = _sku_prefix("", name)
    existing = Product.objects.filter(sku__startswith=prefix).values_list("sku", flat=True)
    max_suffix = 0
    for sku in existing:
        suffix = (sku or "")[len(prefix):]
        if suffix.isdigit():
            max_suffix = max(max_suffix, int(suffix))
    sku = f"{prefix}{max_suffix + 1:04d}"
    vat = vat_percent if vat_percent is not None else Decimal("0.00")
    # avg_cost stores the all-in cost (with VAT already included).
    avg_cost = (unit_cost_with_vat or Decimal("0.00")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return Product.objects.create(
        sku=sku,
        name=name,
        avg_cost=avg_cost,
        vat_percent=vat,
        default_supplier=supplier,
    )
