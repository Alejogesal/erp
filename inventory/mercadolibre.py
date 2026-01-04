import json
import unicodedata
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal
from urllib.parse import urlencode
from urllib.request import Request, urlopen

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
    with urlopen(req, timeout=30) as resp:
        raw = resp.read()
    return json.loads(raw.decode("utf-8") or "{}")


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


def get_item_ids(user_id: str, access_token: str) -> list[str]:
    item_ids: list[str] = []
    offset = 0
    limit = 50
    while True:
        data = _request(
            "GET",
            f"/users/{user_id}/items/search",
            access_token=access_token,
            params={"search_type": "scan", "limit": limit, "offset": offset},
        )
        results = data.get("results") or []
        item_ids.extend(results)
        if len(results) < limit:
            break
        offset += limit
    return item_ids


def get_item(item_id: str, access_token: str) -> dict:
    return _request("GET", f"/items/{item_id}", access_token=access_token)


def get_order(order_id: str, access_token: str) -> dict:
    return _request("GET", f"/orders/{order_id}", access_token=access_token)


def get_orders_summary(user_id: str, access_token: str, days: int = 30) -> dict:
    date_from = (timezone.now() - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%S.000-00:00")
    data = _request(
        "GET",
        "/orders/search",
        access_token=access_token,
        params={"seller": user_id, "order.date_created.from": date_from, "sort": "date_desc"},
    )
    results = data.get("results") or []
    total_amount = Decimal("0.00")
    total_items = 0
    for order in results:
        total_amount += Decimal(str(order.get("total_amount", 0) or 0))
        for item in order.get("order_items") or []:
            total_items += int(item.get("quantity", 0) or 0)
    return {
        "orders": int(data.get("paging", {}).get("total", len(results))),
        "total_amount": f"{total_amount:.2f}",
        "items_sold": total_items,
        "window_days": days,
    }


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
        tokens = _tokenize(f"{product.name} {product.group or ''}")
        index.append((product, tokens, _normalize(product.name)))
    return index


def _match_product(title: str, product_index) -> tuple[Product | None, str]:
    title_norm = _normalize(title)
    title_tokens = set(_tokenize(title))
    best_score = 0.0
    best = None
    for product, tokens, name_norm in product_index:
        if not tokens:
            continue
        if name_norm and name_norm in title_norm:
            return product, product.name
        overlap = title_tokens.intersection(tokens)
        score = len(overlap) / max(len(tokens), 1)
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

    item_ids = get_item_ids(connection.ml_user_id, access_token)
    products = list(Product.objects.all())
    product_index = _build_product_index(products)
    ml_wh = Warehouse.objects.filter(type=Warehouse.WarehouseType.MERCADOLIBRE).first()
    total = matched = unmatched = updated_stock = 0

    for item_id in item_ids:
        item = get_item(item_id, access_token)
        title = item.get("title", "") or ""
        available = int(item.get("available_quantity", 0) or 0)
        status = item.get("status", "") or ""
        permalink = item.get("permalink", "") or ""
        product, matched_name = _match_product(title, product_index)
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
    connection.last_sync_at = timezone.now()
    connection.last_metrics = json.dumps(metrics)
    connection.last_metrics_at = timezone.now()
    connection.save(update_fields=["last_sync_at", "last_metrics", "last_metrics_at"])

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

    products = list(Product.objects.all())
    product_index = _build_product_index(products)
    matched_items = []
    for order_item in order.get("order_items") or []:
        item = order_item.get("item") or {}
        item_id = str(item.get("id") or "")
        quantity = Decimal(str(order_item.get("quantity", 0) or 0))
        unit_price = Decimal(str(order_item.get("unit_price", 0) or 0))
        if quantity <= 0:
            continue
        product = None
        matched_name = ""
        if item_id:
            ml_item = MercadoLibreItem.objects.select_related("product").filter(item_id=item_id).first()
            if ml_item and ml_item.product:
                product = ml_item.product
            elif ml_item and ml_item.title:
                product, matched_name = _match_product(ml_item.title, product_index)
        if not product and item_id:
            item_detail = get_item(item_id, access_token)
            title = item_detail.get("title", "") or ""
            status = item_detail.get("status", "") or ""
            permalink = item_detail.get("permalink", "") or ""
            available = int(item_detail.get("available_quantity", 0) or 0)
            product, matched_name = _match_product(title, product_index)
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
        if not product:
            continue
        vat_percent = product.vat_percent or Decimal("0.00")
        matched_items.append((product, quantity, unit_price, vat_percent))

    if not matched_items:
        return False, "no_matches"

    total_amount = Decimal(str(order.get("total_amount", 0) or 0))
    sale = Sale.objects.create(
        warehouse=ml_wh,
        audience=Customer.Audience.CONSUMER,
        total=total_amount,
        reference=reference,
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
