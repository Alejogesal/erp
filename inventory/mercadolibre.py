import json
import unicodedata
from dataclasses import dataclass
from datetime import timedelta, datetime
from decimal import Decimal
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError
import os

from django.conf import settings
from django.utils import timezone

from . import services
from .models import (
    Customer,
    MercadoLibreConnection,
    MercadoLibreItem,
    Product,
    ProductVariant,
    Sale,
    SaleItem,
    Stock,
    Warehouse,
)

ML_BASE_URL = "https://api.mercadolibre.com"
ML_AUTH_URL = "https://auth.mercadolibre.com.ar/authorization"


@dataclass
class SyncResult:
    total_items: int
    matched: int
    unmatched: int
    updated_stock: int
    metrics: dict


def get_authorize_url(state: str) -> str:
    params = {
        "response_type": "code",
        "client_id": settings.ML_CLIENT_ID,
        "redirect_uri": settings.ML_REDIRECT_URI,
        "state": state,
    }
    return f"{ML_AUTH_URL}?{urlencode(params)}"


def _request(method: str, path: str, access_token: str | None = None, params=None, data=None):
    url = f"{ML_BASE_URL}{path}"
    if params:
        url = f"{url}?{urlencode(params)}"
    body = None
    headers = {"Accept": "application/json"}
    if access_token:
        headers["Authorization"] = f"Bearer {access_token}"
    if data is not None:
        body = json.dumps(data).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = Request(url, data=body, headers=headers, method=method)
    with urlopen(req, timeout=30) as resp:
        raw = resp.read()
    return json.loads(raw.decode("utf-8") or "{}")


def _token_request(payload: dict) -> dict:
    body = urlencode(payload).encode("utf-8")
    req = Request(
        f"{ML_BASE_URL}/oauth/token",
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    try:
        with urlopen(req, timeout=30) as resp:
            raw = resp.read()
        return json.loads(raw.decode("utf-8") or "{}")
    except HTTPError as exc:
        raw = exc.read().decode("utf-8") if exc.fp else ""
        try:
            payload = json.loads(raw or "{}")
        except json.JSONDecodeError:
            payload = {"error_description": raw or str(exc)}
        payload.setdefault("error", "http_error")
        payload.setdefault("status", exc.code)
        return payload


def exchange_code_for_token(code: str) -> dict:
    return _token_request(
        {
            "grant_type": "authorization_code",
            "client_id": settings.ML_CLIENT_ID,
            "client_secret": settings.ML_CLIENT_SECRET,
            "code": code,
            "redirect_uri": settings.ML_REDIRECT_URI,
        }
    )


def refresh_access_token(refresh_token: str) -> dict:
    return _token_request(
        {
            "grant_type": "refresh_token",
            "client_id": settings.ML_CLIENT_ID,
            "client_secret": settings.ML_CLIENT_SECRET,
            "refresh_token": refresh_token,
        }
    )


def _refresh_connection_token(connection: MercadoLibreConnection) -> str:
    refreshed = refresh_access_token(connection.refresh_token)
    access_token = refreshed.get("access_token", "") or ""
    if not access_token:
        return ""
    connection.access_token = access_token
    connection.refresh_token = refreshed.get("refresh_token", connection.refresh_token)
    expires_in = int(refreshed.get("expires_in", 0) or 0)
    if expires_in:
        connection.expires_at = timezone.now() + timedelta(seconds=expires_in)
    connection.save(update_fields=["access_token", "refresh_token", "expires_at"])
    return access_token


def _call_with_refresh(connection: MercadoLibreConnection, func, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except HTTPError as exc:
        if exc.code != 401:
            raise
        new_token = _refresh_connection_token(connection)
        if not new_token:
            raise
        kwargs["access_token"] = new_token
        return func(*args, **kwargs)


def get_user_profile(access_token: str) -> dict:
    return _request("GET", "/users/me", access_token=access_token)


def push_item_stock_and_price(item_id: str, quantity: int, price: "Decimal | None", access_token: str) -> dict:
    """Update stock (and optionally price) on a ML publication."""
    data: dict = {"available_quantity": max(0, quantity)}
    if price is not None and price > 0:
        data["price"] = float(price)
    return _request("PUT", f"/items/{item_id}", access_token=access_token, data=data)


def get_order_messages(order_id: str, seller_id: str, access_token: str) -> dict:
    return _request(
        "GET",
        f"/messages/packs/{order_id}/sellers/{seller_id}",
        access_token=access_token,
        params={"tag": "post_sale"},
    )


# MLA agent user ID (messages must be addressed to the agent, not the buyer directly)
_ML_AGENT_MLA = "3037674934"


def send_order_message(order_id: str, seller_id: str, text: str, access_token: str) -> dict:
    return _request(
        "POST",
        f"/messages/packs/{order_id}/sellers/{seller_id}",
        access_token=access_token,
        params={"tag": "post_sale"},
        data={
            "from": {"user_id": seller_id},
            "to": {"user_id": _ML_AGENT_MLA},
            "text": text,
        },
    )


def get_open_claims(user_id: str, access_token: str, days: int = 30) -> list[dict]:
    """Fetch orders with active mediations (reclamos)."""
    date_from = (timezone.now() - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%S.000-00:00")
    try:
        data = _request(
            "GET", "/orders/search",
            access_token=access_token,
            params={
                "seller": user_id,
                "order.date_last_updated.from": date_from,
                "mediations.status": "opened,under_review",
                "sort": "date_desc",
                "limit": 50,
            },
        )
        return data.get("results") or []
    except Exception:
        return []


def get_seller_reputation(user_id: str, access_token: str) -> dict:
    data = _request("GET", f"/users/{user_id}", access_token=access_token)
    return data.get("seller_reputation") or {}


def get_valid_access_token(connection: MercadoLibreConnection) -> str:
    if not connection.access_token:
        return ""
    if connection.expires_at and timezone.now() >= connection.expires_at - timedelta(minutes=2):
        refreshed = refresh_access_token(connection.refresh_token)
        new_token = (refreshed.get("access_token") or "").strip()
        if not new_token:
            return ""
        connection.access_token = new_token
        connection.refresh_token = refreshed.get("refresh_token", connection.refresh_token)
        expires_in = int(refreshed.get("expires_in", 0) or 0)
        if expires_in:
            connection.expires_at = timezone.now() + timedelta(seconds=expires_in)
        connection.save(update_fields=["access_token", "refresh_token", "expires_at"])
    return connection.access_token


def get_user_product_stock(user_product_id: str, access_token: str) -> dict:
    """Stock per location for a user_product.

    Response shape (Full/Flex coexistence):
      {"locations": [{"type": "meli_facility", "quantity": 12},
                     {"type": "selling_address", "quantity": 9}], ...}
      - meli_facility  → stock managed by Fulfillment (Full)
      - selling_address → seller's own stock (Flex)
    HTTP errors propagate so the caller can refresh the token / fall back.
    """
    return _request("GET", f"/user-products/{user_product_id}/stock", access_token=access_token)


def _extract_user_product_ids(item: dict) -> list[str]:
    """Collect user_product_ids from an item (top-level + per-variation)."""
    ids: list[str] = []
    top = item.get("user_product_id")
    if top:
        ids.append(str(top))
    for variation in item.get("variations") or []:
        vid = variation.get("user_product_id")
        if vid:
            ids.append(str(vid))
    seen: set[str] = set()
    result: list[str] = []
    for i in ids:
        if i not in seen:
            seen.add(i)
            result.append(i)
    return result


def _full_stock_from_locations(data: dict) -> tuple[int, bool]:
    """Sum the Full (meli_facility) quantity from a user-products/stock payload.

    Returns (full_qty, found) where found indicates a meli_facility location
    was present (so the value is trustworthy vs. an empty/failed response).
    """
    total = 0
    found = False
    for loc in (data or {}).get("locations") or []:
        if loc.get("type") == "meli_facility":
            total += int(loc.get("quantity", 0) or 0)
            found = True
    return total, found


def resolve_authoritative_stock(
    connection: "MercadoLibreConnection",
    item: dict,
    access_token: str,
    cache: dict | None = None,
) -> tuple[int, str]:
    """Return (available, user_product_id) using the real source of truth.

    For Full (fulfillment) items the /items available_quantity is unreliable —
    under Full/Flex coexistence it does NOT reflect the actual Full stock. The
    authoritative value is the meli_facility quantity from
    GET /user-products/{user_product_id}/stock. Several publications (catalog +
    traditional) share the same user_product_id, so results are cached per
    user_product_id to avoid duplicate API calls within a sync run. Falls back
    to available_quantity when the stock payload can't be fetched.
    """
    shipping = item.get("shipping") or {}
    logistic_type = item.get("logistic_type", "") or shipping.get("logistic_type", "") or ""
    fallback = int(item.get("available_quantity", 0) or 0)
    up_ids = _extract_user_product_ids(item)
    if not up_ids:
        return fallback, ""
    primary_up = up_ids[0]
    # Only Full items need the override; for non-Full available_quantity is fine.
    if logistic_type != "fulfillment":
        return fallback, primary_up
    total_full = 0
    got_valid = False
    for up in up_ids:
        if cache is not None and up in cache:
            data = cache[up]
        else:
            try:
                data = _call_with_refresh(connection, get_user_product_stock, up, access_token=access_token)
            except Exception:
                data = {}
            if cache is not None:
                cache[up] = data
        qty, found = _full_stock_from_locations(data)
        if found:
            total_full += qty
            got_valid = True
    available = total_full if got_valid else fallback
    return available, primary_up


def get_item_ids(user_id: str, access_token: str, max_items: int | None = None) -> tuple[list[str], bool]:
    """Fetch all item IDs for a seller using scroll-based pagination."""
    item_ids: list[str] = []
    limit = 50
    truncated = False
    scroll_id = None
    while True:
        params: dict = {"search_type": "scan", "limit": limit}
        if scroll_id:
            params["scroll_id"] = scroll_id
        data = _request(
            "GET",
            f"/users/{user_id}/items/search",
            access_token=access_token,
            params=params,
        )
        results = data.get("results") or []
        item_ids.extend(results)
        if max_items is not None and len(item_ids) >= max_items:
            item_ids = item_ids[:max_items]
            truncated = True
            break
        scroll_id = data.get("scroll_id")
        if len(results) < limit or not scroll_id:
            break
    return item_ids, truncated


def get_item(item_id: str, access_token: str) -> dict:
    return _request("GET", f"/items/{item_id}", access_token=access_token)




def update_item_quantity(item_id: str, quantity: int, access_token: str) -> dict:
    return _request("PATCH", f"/items/{item_id}", access_token=access_token, data={"available_quantity": quantity})


def get_order(order_id: str, access_token: str) -> dict:
    return _request("GET", f"/orders/{order_id}", access_token=access_token)


def get_order_payments(order_id: str, access_token: str):
    return _request("GET", f"/orders/{order_id}/payments", access_token=access_token)


def _sum_payment_details(payments: list[dict]) -> tuple[Decimal, Decimal]:
    fee_total = Decimal("0.00")
    tax_total = Decimal("0.00")
    charges_fee = Decimal("0.00")
    charges_tax = Decimal("0.00")
    fee_keywords = {"fee", "commission", "marketplace_fee", "mp_fee"}
    tax_keywords = {"tax", "iva", "impuesto", "ingresos_brutos", "iibb"}
    for payment in payments:
        # marketplace_fee is the primary ML commission field
        mkt_fee = Decimal(str(payment.get("marketplace_fee", 0) or 0)).copy_abs()
        fee_amount = Decimal(str(payment.get("fee_amount", 0) or 0)).copy_abs()
        fee_total += max(mkt_fee, fee_amount)
        tax_total += Decimal(str(payment.get("taxes_amount", 0) or 0)).copy_abs()
        for charge in payment.get("charges_details") or []:
            ctype = str(charge.get("type", "") or "").lower()
            amount = Decimal(str(charge.get("amount", {}).get("value", 0) if isinstance(charge.get("amount"), dict) else charge.get("amount", 0) or 0)).copy_abs()
            if any(k in ctype for k in fee_keywords):
                charges_fee += amount
            elif any(k in ctype for k in tax_keywords):
                charges_tax += amount
    if fee_total == 0 and charges_fee > 0:
        fee_total = charges_fee
    if tax_total == 0 and charges_tax > 0:
        tax_total = charges_tax
    return fee_total, tax_total


def _parse_ml_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if timezone.is_naive(parsed):
        return timezone.make_aware(parsed)
    return parsed


def get_orders_summary(user_id: str, access_token: str, days: int = 30) -> dict:
    date_from = (timezone.now() - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%S.000-00:00")
    limit = 50
    offset = 0
    max_orders_env = os.environ.get("ML_ORDERS_MAX", "")
    max_orders = int(max_orders_env) if max_orders_env.isdigit() else 200
    results = []
    paging_total = 0
    while True:
        data = _request(
            "GET",
            "/orders/search",
            access_token=access_token,
            params={
                "seller": user_id,
                "order.date_created.from": date_from,
                "sort": "date_desc",
                "limit": limit,
                "offset": offset,
            },
        )
        batch = data.get("results") or []
        paging_total = int(data.get("paging", {}).get("total", 0) or 0)
        results.extend(batch)
        if len(batch) < limit or len(results) >= max_orders:
            break
        offset += limit

    total_amount = Decimal("0.00")
    total_items = 0
    item_sales: dict[str, dict[str, object]] = {}
    for order in results:
        total_amount += Decimal(str(order.get("total_amount", 0) or 0))
        order_created = _parse_ml_datetime(order.get("date_created"))
        for item in order.get("order_items") or []:
            total_items += int(item.get("quantity", 0) or 0)
            item_data = item.get("item") or {}
            item_id = str(item_data.get("id") or "")
            if not item_id:
                continue
            entry = item_sales.setdefault(item_id, {"units": 0, "last_sold_at": None})
            entry["units"] = int(entry["units"]) + int(item.get("quantity", 0) or 0)
            if order_created and (entry["last_sold_at"] is None or order_created > entry["last_sold_at"]):
                entry["last_sold_at"] = order_created

    return {
        "orders": len(results),
        "orders_total": paging_total,
        "orders_sampled": len(results),
        "total_amount": f"{total_amount:.2f}",
        "items_sold": total_items,
        "window_days": days,
        "item_sales": item_sales,
        "max_orders": max_orders,
    }


def get_recent_order_ids(user_id: str, access_token: str, days: int = 30, date_from_str: str | None = None, date_to_str: str | None = None) -> list[str]:
    if date_from_str:
        date_from = date_from_str
    else:
        date_from = (timezone.now() - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%S.000-00:00")
    limit = 50
    offset = 0
    max_orders_env = os.environ.get("ML_ORDERS_MAX", "")
    max_orders = int(max_orders_env) if max_orders_env.isdigit() else 1000
    order_ids: list[str] = []
    while True:
        params = {
            "seller": user_id,
            "order.date_created.from": date_from,
            "sort": "date_desc",
            "limit": limit,
            "offset": offset,
        }
        if date_to_str:
            params["order.date_created.to"] = date_to_str
        data = _request("GET", "/orders/search", access_token=access_token, params=params)
        batch = data.get("results") or []
        for order in batch:
            order_id = str(order.get("id") or "")
            if order_id:
                order_ids.append(order_id)
        if len(batch) < limit or len(order_ids) >= max_orders:
            break
        offset += limit
    return order_ids[:max_orders]


def sync_recent_orders(connection: MercadoLibreConnection, user, days: int = 30, date_from_str: str | None = None, date_to_str: str | None = None) -> dict:
    access_token = get_valid_access_token(connection)
    if not access_token:
        return {"total": 0, "created": 0, "updated": 0, "reasons": {"missing_access_token": 1}}
    try:
        order_ids = _call_with_refresh(
            connection,
            get_recent_order_ids,
            connection.ml_user_id,
            access_token=access_token,
            days=days,
            date_from_str=date_from_str,
            date_to_str=date_to_str,
        )
    except HTTPError as exc:
        if exc.code == 401:
            return {"total": 0, "created": 0, "updated": 0, "reasons": {"unauthorized": 1}}
        raise
    created = 0
    updated = 0
    reasons: dict[str, int] = {}
    no_match_ids: list[str] = []
    for order_id in order_ids:
        ok, reason = sync_order(connection, order_id, user)
        if ok and reason == "ok":
            created += 1
        elif ok and reason == "updated":
            updated += 1
        else:
            reasons[reason] = reasons.get(reason, 0) + 1
            if reason == "no_matches":
                no_match_ids.append(str(order_id))
    return {"total": len(order_ids), "created": created, "updated": updated, "reasons": reasons, "no_match_ids": no_match_ids}


def _normalize(text: str) -> str:
    if not text:
        return ""
    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    cleaned = []
    for ch in text.lower():
        cleaned.append(ch if ch.isalnum() else " ")
    return " ".join("".join(cleaned).split())


def _tokenize(text: str) -> list[str]:
    return [tok for tok in _normalize(text).split() if len(tok) > 1]


def _build_product_index(products: list[Product]):
    index = []
    for product in products:
        name_tokens = _tokenize(product.name)
        group_tokens = _tokenize(product.group or "")
        index.append((product, name_tokens, group_tokens, _normalize(product.name)))
    return index


def _match_product(title: str, product_index) -> tuple[Product | None, str]:
    title_norm = _normalize(title)
    title_tokens = set(_tokenize(title))
    best_score = 0.0
    best = None
    for product, name_tokens, group_tokens, name_norm in product_index:
        if not name_tokens:
            continue
        if group_tokens and not title_tokens.intersection(group_tokens):
            continue
        if name_norm and name_norm in title_norm:
            return product, product.name
        overlap_name = title_tokens.intersection(name_tokens)
        score = len(overlap_name) / max(len(name_tokens), 1)
        if group_tokens:
            overlap_group = title_tokens.intersection(group_tokens)
            score += 0.25 if overlap_group else 0
        if score > best_score:
            best_score = score
            best = product
    if best_score >= 0.3:
        return best, best.name if best else ""
    return None, ""


def _extract_variation_values(order_item: dict, item_detail: dict | None = None) -> list[str]:
    values: list[str] = []
    for source in (order_item.get("variation_attributes"), (order_item.get("item") or {}).get("variation_attributes")):
        for attr in source or []:
            value = attr.get("value_name") or attr.get("name") or attr.get("value_id")
            if value:
                values.append(str(value))
    variation_id = order_item.get("variation_id") or (order_item.get("item") or {}).get("variation_id")
    if item_detail and variation_id:
        for variation in item_detail.get("variations") or []:
            if str(variation.get("id") or "") != str(variation_id):
                continue
            for attr in variation.get("attribute_combinations") or []:
                value = attr.get("value_name") or attr.get("name") or attr.get("value_id")
                if value:
                    values.append(str(value))
            for attr in variation.get("attributes") or []:
                value = attr.get("value_name") or attr.get("name") or attr.get("value_id")
                if value:
                    values.append(str(value))
            break
    return values


def _resolve_variant_for_order_item(
    product: Product,
    order_item: dict,
    item_id: str,
    access_token: str,
    connection: MercadoLibreConnection | None = None,
) -> ProductVariant | None:
    if not ProductVariant.objects.filter(product=product).exists():
        return None
    values = _extract_variation_values(order_item)
    if not values:
        variation_id = order_item.get("variation_id") or (order_item.get("item") or {}).get("variation_id")
        if variation_id and item_id:
            try:
                if connection:
                    item_detail = _call_with_refresh(
                        connection, get_item, item_id, access_token=access_token
                    )
                else:
                    item_detail = get_item(item_id, access_token)
            except Exception:
                item_detail = None
            values = _extract_variation_values(order_item, item_detail=item_detail)
    if not values:
        return None
    value_norms = [_normalize(val) for val in values if val]
    best = None
    best_score = 0
    for variant in ProductVariant.objects.filter(product=product):
        name_norm = _normalize(variant.name)
        if not name_norm:
            continue
        for val_norm in value_norms:
            if not val_norm:
                continue
            score = 0
            if name_norm == val_norm:
                score = 3
            elif name_norm in val_norm or val_norm in name_norm:
                score = 1
            if score > best_score:
                best_score = score
                best = variant
    return best


def sync_items_and_stock(connection: MercadoLibreConnection, user, *, ignore_env_limit: bool = False) -> SyncResult:
    access_token = get_valid_access_token(connection)
    if not access_token:
        return SyncResult(0, 0, 0, 0, {})

    if not connection.ml_user_id:
        try:
            profile = _call_with_refresh(connection, get_user_profile, access_token=access_token)
        except HTTPError as exc:
            if exc.code == 401:
                return SyncResult(0, 0, 0, 0, {"error": "unauthorized"})
            raise
        connection.ml_user_id = str(profile.get("id", "") or "")
        connection.nickname = profile.get("nickname", "") or ""
        connection.save(update_fields=["ml_user_id", "nickname"])

    max_items = None
    if not ignore_env_limit:
        max_items_env = os.environ.get("ML_SYNC_MAX_ITEMS", "")
        max_items = int(max_items_env) if max_items_env.isdigit() else None
    try:
        item_ids, truncated = _call_with_refresh(
            connection,
            get_item_ids,
            connection.ml_user_id,
            access_token=access_token,
            max_items=max_items,
        )
    except HTTPError as exc:
        if exc.code == 401:
            return SyncResult(0, 0, 0, 0, {"error": "unauthorized"})
        raise
    ml_wh = Warehouse.objects.filter(type=Warehouse.WarehouseType.MERCADOLIBRE).first()
    total = matched = unmatched = updated_stock = 0
    # Cache user-product stock per user_product_id so publications sharing the
    # same Full stock (catalog + traditional) don't trigger duplicate API calls.
    fulfillment_cache: dict[str, dict] = {}

    for item_id in item_ids:
        try:
            item = _call_with_refresh(connection, get_item, item_id, access_token=access_token)
        except HTTPError as exc:
            if exc.code == 401:
                return SyncResult(total, matched, unmatched, updated_stock, {"error": "unauthorized"})
            raise
        title = item.get("title", "") or ""
        status = item.get("status", "") or ""
        shipping = item.get("shipping") or {}
        logistic_type = item.get("logistic_type", "") or shipping.get("logistic_type", "") or ""
        permalink = item.get("permalink", "") or ""
        # For Full items the /items available_quantity is unreliable (Full/Flex
        # coexistence); the user-products stock endpoint (meli_facility) is the
        # source of truth. Non-Full items keep using available_quantity directly.
        available, user_product_id = resolve_authoritative_stock(
            connection, item, access_token, cache=fulfillment_cache
        )
        existing = MercadoLibreItem.objects.filter(item_id=item_id).first()
        product = existing.product if existing else None
        matched_name = existing.matched_name if existing else ""
        MercadoLibreItem.objects.update_or_create(
            item_id=item_id,
            defaults={
                "title": title,
                "available_quantity": available,
                "status": status,
                "logistic_type": logistic_type,
                "user_product_id": user_product_id,
                "permalink": permalink,
                "product": product,
                "matched_name": matched_name,
            },
        )
        total += 1
        if product:
            matched += 1
            if ml_wh:
                stock = Stock.objects.filter(product=product, warehouse=ml_wh).first()
                current_qty = stock.quantity if stock else Decimal("0.00")
                desired_qty = Decimal(str(available))
                diff = desired_qty - current_qty
                if diff != 0:
                    services.register_adjustment(
                        product=product,
                        warehouse=ml_wh,
                        quantity=diff,
                        user=user,
                        reference=f"Sync ML {item_id}",
                        allow_negative=True,
                    )
                    updated_stock += 1
        else:
            unmatched += 1

    try:
        metrics = _call_with_refresh(
            connection,
            get_orders_summary,
            connection.ml_user_id,
            access_token=access_token,
            days=30,
        )
    except HTTPError as exc:
        if exc.code == 401:
            return SyncResult(total, matched, unmatched, updated_stock, {"error": "unauthorized"})
        raise
    item_sales = metrics.pop("item_sales", {})
    for item_id, data in item_sales.items():
        MercadoLibreItem.objects.filter(item_id=item_id).update(
            last_sold_at=data.get("last_sold_at"),
            units_sold_30d=data.get("units", 0),
        )
    # Mark items that no longer exist in seller's account as closed (only on full scan)
    if not truncated:
        scanned_ids = set(item_ids)
        orphaned_count = MercadoLibreItem.objects.exclude(item_id__in=scanned_ids).update(
            available_quantity=0,
            status="closed",
        )
    else:
        orphaned_count = 0

    connection.last_sync_at = timezone.now()
    connection.last_metrics = json.dumps(metrics)
    connection.last_metrics_at = timezone.now()
    connection.save(update_fields=["last_sync_at", "last_metrics", "last_metrics_at"])

    if truncated:
        metrics = {**metrics, "truncated": True, "max_items": max_items}
    return SyncResult(total, matched, unmatched, updated_stock, metrics)


def sync_order(connection: MercadoLibreConnection, order_id: str, user) -> tuple[bool, str]:
    access_token = get_valid_access_token(connection)
    if not access_token:
        return False, "missing_access_token"

    try:
        order = _call_with_refresh(connection, get_order, order_id, access_token=access_token)
    except HTTPError as exc:
        if exc.code == 401:
            return False, "unauthorized"
        raise
    order_status = order.get("status", "") or ""
    if order_status in {"cancelled", "expired"}:
        return False, "ignored_status"

    order_date = _parse_ml_datetime(order.get("date_created"))

    # Delivery status and fraud risk from order tags (no extra API call needed)
    order_tags = {str(t).lower() for t in (order.get("tags") or [])}
    fraud_risk = "fraud_risk_detected" in order_tags
    if "delivered" in order_tags:
        delivery_status = Sale.DeliveryStatus.DELIVERED
    elif order_status == "paid" and "not_delivered" not in order_tags:
        delivery_status = Sale.DeliveryStatus.IN_TRANSIT
    else:
        delivery_status = Sale.DeliveryStatus.NOT_DELIVERED

    reference = f"ML ORDER {order_id}"
    existing_sale = Sale.objects.filter(reference=reference).first()

    ml_wh = Warehouse.objects.filter(type=Warehouse.WarehouseType.MERCADOLIBRE).first()
    if not ml_wh:
        return False, "missing_warehouse"

    matched_items = []
    for order_item in order.get("order_items") or []:
        item = order_item.get("item") or {}
        item_id = str(item.get("id") or "")
        quantity = Decimal(str(order_item.get("quantity", 0) or 0))
        unit_price = Decimal(str(order_item.get("unit_price", 0) or 0))
        if quantity <= 0:
            continue
        product = None
        if item_id:
            ml_item = MercadoLibreItem.objects.select_related("product").filter(item_id=item_id).first()
            if ml_item and ml_item.product:
                product = ml_item.product
        if not product and item_id:
            try:
                item_detail = _call_with_refresh(connection, get_item, item_id, access_token=access_token)
            except HTTPError:
                item_detail = {}
            title = item_detail.get("title", "") or ""
            status = item_detail.get("status", "") or ""
            shipping = item_detail.get("shipping") or {}
            logistic_type = item_detail.get("logistic_type", "") or shipping.get("logistic_type", "") or ""
            permalink = item_detail.get("permalink", "") or ""
            available, user_product_id = resolve_authoritative_stock(connection, item_detail, access_token)
            MercadoLibreItem.objects.update_or_create(
                item_id=item_id,
                defaults={
                    "title": title,
                    "available_quantity": available,
                    "status": status,
                    "logistic_type": logistic_type,
                    "user_product_id": user_product_id,
                    "permalink": permalink,
                },
            )
        if not product:
            continue
        vat_percent = product.vat_percent or Decimal("0.00")
        variant = _resolve_variant_for_order_item(product, order_item, item_id, access_token, connection=connection)
        matched_items.append((product, quantity, unit_price, vat_percent, variant))

    if not matched_items:
        return False, "no_matches"

    total_amount = Decimal(str(order.get("total_amount", 0) or 0))

    # fee_details in the order object is the authoritative source for ML fees
    fee_total = Decimal("0.00")
    tax_total = Decimal("0.00")
    for fee in order.get("fee_details") or []:
        ftype = str(fee.get("type", "") or "").lower()
        amount = Decimal(str(fee.get("amount", 0) or 0)).copy_abs()
        if "tax" in ftype or "iva" in ftype or "iibb" in ftype or "impuesto" in ftype:
            tax_total += amount
        else:
            fee_total += amount

    # fallback 1: sale_fee in order_items is the total fee for that item (not per unit)
    if fee_total == Decimal("0.00"):
        for oi in order.get("order_items") or []:
            sf = Decimal(str(oi.get("sale_fee") or 0)).copy_abs()
            fee_total += sf
        if fee_total > Decimal("0.00"):
            # IIBB ≈ 3.5% of commission (standard ML Argentina rate)
            tax_total = (fee_total * Decimal("0.035")).quantize(Decimal("0.01"))

    # fallback 2: try payment-level data if still empty
    if fee_total == Decimal("0.00"):
        payments = order.get("payments") or []
        if not payments:
            try:
                payments_data = _call_with_refresh(connection, get_order_payments, order_id, access_token=access_token)
                if isinstance(payments_data, dict):
                    payments = payments_data.get("payments") or payments_data.get("results") or []
                elif isinstance(payments_data, list):
                    payments = payments_data
            except Exception:
                payments = []
        fee_total, tax_total = _sum_payment_details(payments)

    if existing_sale:
        existing_sale.ml_commission_total = fee_total.quantize(Decimal("0.01"))
        existing_sale.ml_tax_total = tax_total.quantize(Decimal("0.01"))
        existing_sale.ml_order_id = str(order_id)
        existing_sale.delivery_status = delivery_status
        existing_sale.ml_fraud_risk = fraud_risk
        existing_sale.save(update_fields=["ml_commission_total", "ml_tax_total", "ml_order_id", "delivery_status", "ml_fraud_risk"])
        for product, quantity, unit_price, vat_percent, variant in matched_items:
            target = (
                SaleItem.objects.filter(sale=existing_sale, product=product, quantity=quantity)
                .order_by("id")
                .first()
            )
            if target:
                to_update = []
                if variant and not target.variant_id:
                    target.variant = variant
                    to_update.append("variant")
                if not target.cost_unit or target.cost_unit <= Decimal("0.00"):
                    new_cost = product.last_purchase_cost()
                    if not new_cost or new_cost <= Decimal("0.00"):
                        new_cost = product.cost_with_vat()
                    if new_cost and new_cost > Decimal("0.00"):
                        target.cost_unit = new_cost
                        to_update.append("cost_unit")
                if to_update:
                    target.save(update_fields=to_update)
        return True, "updated"

    sale = Sale.objects.create(
        warehouse=ml_wh,
        audience=Customer.Audience.CONSUMER,
        total=total_amount,
        reference=reference,
        ml_order_id=str(order_id),
        ml_commission_total=fee_total.quantize(Decimal("0.01")),
        ml_tax_total=tax_total.quantize(Decimal("0.01")),
        delivery_status=delivery_status,
        ml_fraud_risk=fraud_risk,
        user=user,
    )
    if order_date:
        Sale.objects.filter(pk=sale.pk).update(created_at=order_date)
    for product, quantity, unit_price, vat_percent, variant in matched_items:
        line_total = (unit_price * quantity).quantize(Decimal("0.01"))
        cost_unit = product.last_purchase_cost()
        if not cost_unit or cost_unit <= Decimal("0.00"):
            cost_unit = product.cost_with_vat()
        SaleItem.objects.create(
            sale=sale,
            product=product,
            variant=variant,
            quantity=quantity,
            unit_price=unit_price,
            cost_unit=cost_unit,
            discount_percent=Decimal("0.00"),
            final_unit_price=unit_price,
            line_total=line_total,
            vat_percent=vat_percent,
        )

    return True, "ok"
