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


def get_user_profile(access_token: str) -> dict:
    return _request("GET", "/users/me", access_token=access_token)


def get_valid_access_token(connection: MercadoLibreConnection) -> str:
    if not connection.access_token:
        return ""
    if connection.expires_at and timezone.now() >= connection.expires_at - timedelta(minutes=2):
        refreshed = refresh_access_token(connection.refresh_token)
        connection.access_token = refreshed.get("access_token", connection.access_token)
        connection.refresh_token = refreshed.get("refresh_token", connection.refresh_token)
        expires_in = int(refreshed.get("expires_in", 0) or 0)
        if expires_in:
            connection.expires_at = timezone.now() + timedelta(seconds=expires_in)
        connection.save(update_fields=["access_token", "refresh_token", "expires_at"])
    return connection.access_token


def get_item_ids(user_id: str, access_token: str, max_items: int | None = None) -> tuple[list[str], bool]:
    item_ids: list[str] = []
    offset = 0
    limit = 50
    truncated = False
    while True:
        data = _request(
            "GET",
            f"/users/{user_id}/items/search",
            access_token=access_token,
            params={"search_type": "scan", "limit": limit, "offset": offset},
        )
        results = data.get("results") or []
        item_ids.extend(results)
        if max_items is not None and len(item_ids) >= max_items:
            item_ids = item_ids[:max_items]
            truncated = True
            break
        if len(results) < limit:
            break
        offset += limit
    return item_ids, truncated


def get_item(item_id: str, access_token: str) -> dict:
    return _request("GET", f"/items/{item_id}", access_token=access_token)


def get_order(order_id: str, access_token: str) -> dict:
    return _request("GET", f"/orders/{order_id}", access_token=access_token)


def get_order_payments(order_id: str, access_token: str):
    return _request("GET", f"/orders/{order_id}/payments", access_token=access_token)


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


def get_recent_order_ids(user_id: str, access_token: str, days: int = 30) -> list[str]:
    date_from = (timezone.now() - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%S.000-00:00")
    limit = 50
    offset = 0
    max_orders_env = os.environ.get("ML_ORDERS_MAX", "")
    max_orders = int(max_orders_env) if max_orders_env.isdigit() else 200
    order_ids: list[str] = []
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
        for order in batch:
            order_id = str(order.get("id") or "")
            if order_id:
                order_ids.append(order_id)
        if len(batch) < limit or len(order_ids) >= max_orders:
            break
        offset += limit
    return order_ids[:max_orders]


def sync_recent_orders(connection: MercadoLibreConnection, user, days: int = 30) -> dict:
    access_token = get_valid_access_token(connection)
    if not access_token:
        return {"total": 0, "created": 0, "reasons": {"missing_access_token": 1}}
    order_ids = get_recent_order_ids(connection.ml_user_id, access_token, days=days)
    created = 0
    reasons: dict[str, int] = {}
    for order_id in order_ids:
        ok, reason = sync_order(connection, order_id, user)
        if ok:
            created += 1
        else:
            reasons[reason] = reasons.get(reason, 0) + 1
    return {"total": len(order_ids), "created": created, "reasons": reasons}


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


def sync_items_and_stock(connection: MercadoLibreConnection, user) -> SyncResult:
    access_token = get_valid_access_token(connection)
    if not access_token:
        return SyncResult(0, 0, 0, 0, {})

    if not connection.ml_user_id:
        profile = get_user_profile(access_token)
        connection.ml_user_id = str(profile.get("id", "") or "")
        connection.nickname = profile.get("nickname", "") or ""
        connection.save(update_fields=["ml_user_id", "nickname"])

    max_items_env = os.environ.get("ML_SYNC_MAX_ITEMS", "")
    max_items = int(max_items_env) if max_items_env.isdigit() else None
    item_ids, truncated = get_item_ids(connection.ml_user_id, access_token, max_items=max_items)
    ml_wh = Warehouse.objects.filter(type=Warehouse.WarehouseType.MERCADOLIBRE).first()
    total = matched = unmatched = updated_stock = 0

    for item_id in item_ids:
        item = get_item(item_id, access_token)
        title = item.get("title", "") or ""
        available = int(item.get("available_quantity", 0) or 0)
        status = item.get("status", "") or ""
        permalink = item.get("permalink", "") or ""
        existing = MercadoLibreItem.objects.filter(item_id=item_id).first()
        product = existing.product if existing else None
        matched_name = existing.matched_name if existing else ""
        MercadoLibreItem.objects.update_or_create(
            item_id=item_id,
            defaults={
                "title": title,
                "available_quantity": available,
                "status": status,
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

    metrics = get_orders_summary(connection.ml_user_id, access_token, days=30)
    item_sales = metrics.pop("item_sales", {})
    for item_id, data in item_sales.items():
        MercadoLibreItem.objects.filter(item_id=item_id).update(
            last_sold_at=data.get("last_sold_at"),
            units_sold_30d=data.get("units", 0),
        )
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

    order = get_order(order_id, access_token)
    order_status = order.get("status", "") or ""
    if order_status in {"cancelled", "expired"}:
        return False, "ignored_status"

    reference = f"ML ORDER {order_id}"
    if Sale.objects.filter(reference=reference).exists():
        return False, "already_processed"

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
            item_detail = get_item(item_id, access_token)
            title = item_detail.get("title", "") or ""
            status = item_detail.get("status", "") or ""
            permalink = item_detail.get("permalink", "") or ""
            available = int(item_detail.get("available_quantity", 0) or 0)
            MercadoLibreItem.objects.update_or_create(
                item_id=item_id,
                defaults={
                    "title": title,
                    "available_quantity": available,
                    "status": status,
                    "permalink": permalink,
                },
            )
        if not product:
            continue
        vat_percent = product.vat_percent or Decimal("0.00")
        matched_items.append((product, quantity, unit_price, vat_percent))

    if not matched_items:
        return False, "no_matches"

    total_amount = Decimal(str(order.get("total_amount", 0) or 0))
    fee_total = Decimal("0.00")
    tax_total = Decimal("0.00")
    payments = order.get("payments") or []
    if not payments:
        try:
            payments_data = get_order_payments(order_id, access_token)
            if isinstance(payments_data, dict):
                payments = payments_data.get("payments") or payments_data.get("results") or []
            elif isinstance(payments_data, list):
                payments = payments_data
        except Exception:
            payments = []
    for payment in payments:
        fee_total += Decimal(str(payment.get("fee_amount", 0) or 0)).copy_abs()
        tax_total += Decimal(str(payment.get("taxes_amount", 0) or 0)).copy_abs()
    sale = Sale.objects.create(
        warehouse=ml_wh,
        audience=Customer.Audience.CONSUMER,
        total=total_amount,
        reference=reference,
        ml_order_id=str(order_id),
        ml_commission_total=fee_total.quantize(Decimal("0.01")),
        ml_tax_total=tax_total.quantize(Decimal("0.01")),
        user=user,
    )
    for product, quantity, unit_price, vat_percent in matched_items:
        line_total = (unit_price * quantity).quantize(Decimal("0.01"))
        SaleItem.objects.create(
            sale=sale,
            product=product,
            quantity=quantity,
            unit_price=unit_price,
            discount_percent=Decimal("0.00"),
            final_unit_price=unit_price,
            line_total=line_total,
            vat_percent=vat_percent,
        )
        services.register_exit(
            product=product,
            warehouse=ml_wh,
            quantity=quantity,
            user=user,
            reference=reference,
            sale=sale,
            sale_price=unit_price,
            vat_percent=vat_percent,
            allow_negative=True,
        )

    return True, "ok"
