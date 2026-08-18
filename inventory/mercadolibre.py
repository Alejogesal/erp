import json
import logging
import threading
import time
import unicodedata
from dataclasses import dataclass
from datetime import timedelta, datetime
from decimal import Decimal
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError
import os

from django.conf import settings
from django.db import transaction
from django.utils import timezone

from . import services
from .models import (
    ML_SELLER_FULFILLED_TYPES,
    Customer,
    MercadoLibreConnection,
    MercadoLibreItem,
    Product,
    ProductVariant,
    Sale,
    SaleItem,
    Stock,
    StockMovement,
    Warehouse,
)

logger = logging.getLogger(__name__)

ML_BASE_URL = "https://api.mercadolibre.com"
ML_AUTH_URL = "https://auth.mercadolibre.com.ar/authorization"

# Antigüedad máxima de una venta ML para que se le descuente stock del depósito
# propio. Ver _apply_ml_stock_exit: protege contra el descuento retroactivo de
# ventas viejas cuando sync_ml_orders re-sincroniza su ventana de 90 días.
ML_STOCK_EXIT_MAX_AGE_DAYS = 3


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
        # offline_access is REQUIRED for ML to return a refresh_token; without it
        # the access token expires in ~6h and can't be renewed (sync freezes).
        "scope": "offline_access read write",
    }
    return f"{ML_AUTH_URL}?{urlencode(params)}"


def _request(
    method: str,
    path: str,
    access_token: str | None = None,
    params=None,
    data=None,
    extra_headers: dict | None = None,
    with_headers: bool = False,
):
    """Llamada a la API de ML.

    `with_headers=True` devuelve (payload, headers) en vez del payload solo: hace
    falta para el stock de user-products, donde el header `x-version` de la
    respuesta hay que reenviarlo en el PUT.
    """
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
    if extra_headers:
        headers.update(extra_headers)
    req = Request(url, data=body, headers=headers, method=method)
    with urlopen(req, timeout=30) as resp:
        raw = resp.read()
        resp_headers = dict(resp.headers)
    payload = json.loads(raw.decode("utf-8") or "{}")
    if with_headers:
        return payload, resp_headers
    return payload


def reconcile_enabled() -> bool:
    """¿El sync publica el stock del ERP en ML?

    Encendido salvo que se pida lo contrario. Vive acá para que el motor y el
    panel lean lo mismo: cuando cada uno tenía su default, el cartel decía
    "apagada" mientras el barrido empujaba igual.
    """
    return os.environ.get("ML_STOCK_RECONCILE", "1") != "0"


def sync_interval_minutes() -> int:
    """Cada cuánto se sincroniza con ML. Por defecto, 15 minutos."""
    raw = os.environ.get("ML_SYNC_EVERY_MINUTES", "")
    return int(raw) if raw.isdigit() and int(raw) > 0 else 15


def error_detail(exc: Exception) -> str:
    """Motivo legible de un error de la API de ML.

    El body de un 4xx trae el `message` de ML ("Item can not be modified",
    "invalid quantity"…), que es lo único que explica por qué no se publicó el
    stock. urlopen lo deja en el propio HTTPError y se pierde si no se lee.
    """
    if not isinstance(exc, HTTPError):
        return str(exc)
    detail = ""
    try:
        payload = json.loads(exc.read().decode("utf-8") or "{}")
        detail = payload.get("message") or payload.get("error") or ""
        causes = payload.get("cause") or []
        if causes:
            texts = [str(c.get("message") or c.get("code") or c) for c in causes if c]
            if texts:
                detail = f"{detail} ({'; '.join(texts)})" if detail else "; ".join(texts)
    except Exception:
        detail = ""
    return f"HTTP {exc.code}: {detail}" if detail else f"HTTP {exc.code}"


# El JSON nuevo de /shipments requiere este header. ML lo volvió obligatorio en
# octubre de 2025; sin él, `logistic` viene aplanado como `logistic_type` y los
# campos order_id/external_reference ya no se devuelven.
_SHIPMENT_HEADERS = {"x-format-new": "true"}


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


def token_status(connection: MercadoLibreConnection) -> dict:
    """Quick health check of a connection's OAuth token (no network calls).

    Returns flags the UI/sync can use to surface problems instead of failing
    silently:
      - has_token / has_refresh: whether each token string is present
      - expired: access token past its expiry
      - can_refresh: a refresh token exists to renew with
      - healthy: usable now or renewable
    """
    has_token = bool(connection.access_token)
    has_refresh = bool(connection.refresh_token)
    expired = bool(connection.expires_at and timezone.now() >= connection.expires_at)
    return {
        "has_token": has_token,
        "has_refresh": has_refresh,
        "expired": expired,
        "can_refresh": has_refresh,
        # Healthy = we have a non-expired token, OR we can refresh to get one.
        "healthy": (has_token and not expired) or has_refresh,
    }


def get_valid_access_token(connection: MercadoLibreConnection) -> str:
    # Refresh when the token is missing OR within 2 min of expiry, as long as we
    # have a refresh_token to do it with. If there's no refresh_token we can't
    # recover here — the caller must re-authorize.
    needs_refresh = (not connection.access_token) or (
        connection.expires_at and timezone.now() >= connection.expires_at - timedelta(minutes=2)
    )
    if needs_refresh:
        if not connection.refresh_token:
            logger.warning(
                "ML: la cuenta %s no tiene refresh token; hay que reautorizar para seguir sincronizando",
                connection.nickname or connection.ml_user_id,
            )
            return connection.access_token or ""
        refreshed = refresh_access_token(connection.refresh_token)
        new_token = (refreshed.get("access_token") or "").strip()
        if not new_token:
            # Sin esto la sincronización se corta en silencio: el barrido sale
            # sin token y nada explica por qué dejó de publicarse el stock.
            logger.error(
                "ML: no se pudo renovar el token de %s — %s. Reautorizá la cuenta.",
                connection.nickname or connection.ml_user_id,
                refreshed.get("error_description") or refreshed.get("error") or "sin detalle",
            )
            return connection.access_token or ""
        connection.access_token = new_token
        # ML rotates refresh tokens (single-use); persist the new one if present.
        connection.refresh_token = refreshed.get("refresh_token") or connection.refresh_token
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


def item_logistic_type(item: dict) -> str:
    """logistic_type de una publicación (viene suelto o dentro de shipping)."""
    shipping = (item or {}).get("shipping") or {}
    return str((item or {}).get("logistic_type") or shipping.get("logistic_type") or "")


def item_has_flex(item: dict) -> bool:
    """La publicación tiene Envíos Flex activo.

    ML lo marca con el tag `self_service_in` en shipping.tags. Combinado con
    logistic_type=fulfillment identifica las publicaciones en convivencia
    Full/Flex, que son las que necesitan manejo de stock por separado.
    """
    shipping = (item or {}).get("shipping") or {}
    tags = shipping.get("tags") or []
    return "self_service_in" in {str(t) for t in tags}


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


def _flex_stock_from_locations(data: dict) -> tuple[int, bool]:
    """Cantidad publicada en el depósito propio (selling_address) de un user_product.

    Es la contraparte de _full_stock_from_locations: en convivencia Full/Flex el
    mismo user_product tiene las dos ubicaciones y hay que compararlas por
    separado contra el depósito COMUN.
    """
    total = 0
    found = False
    for loc in (data or {}).get("locations") or []:
        if loc.get("type") == "selling_address":
            total += int(loc.get("quantity", 0) or 0)
            found = True
    return total, found


def resolve_stock_breakdown(
    connection: "MercadoLibreConnection",
    item: dict,
    access_token: str,
    cache: dict | None = None,
) -> tuple[int, str, int, int, bool]:
    """(available, user_product_id, en_full, en_deposito_propio, tiene_ubicación_propia).

    Separar las dos ubicaciones importa porque significan cosas distintas:
    `meli_facility` es mercadería que está físicamente en el depósito de ML, y
    `selling_address` es la que está en el tuyo. Mostrarlas juntas en un solo
    número hace que parezcan un desfasaje cosas que no lo son.

    El último valor dice si ML informó una ubicación `selling_address` para el
    user_product. Es la prueba de que la publicación vende desde el depósito
    propio, y es más confiable que el tag `self_service_in`: el tag puede no
    venir en /items aunque la convivencia exista, y sin él la publicación
    quedaba clasificada como Full puro y no recibía nunca el stock del ERP.

    Para las publicaciones que no son Full no hace falta llamar a la API: todo
    el stock publicado sale del depósito propio.
    """
    available, user_product_id = resolve_authoritative_stock(connection, item, access_token, cache=cache)
    logistic_type = item_logistic_type(item)
    if logistic_type != "fulfillment":
        return available, user_product_id, 0, available, False
    data = (cache or {}).get(user_product_id)
    if data is None and user_product_id:
        try:
            data = _call_with_refresh(
                connection, get_user_product_stock, user_product_id, access_token=access_token
            )
        except Exception:
            data = {}
        if cache is not None:
            cache[user_product_id] = data
    full_qty, _ = _full_stock_from_locations(data or {})
    flex_qty, own_location = _flex_stock_from_locations(data or {})
    return available, user_product_id, full_qty, flex_qty, own_location


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
    logistic_type = item_logistic_type(item)
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


def get_shipment(shipment_id: str, access_token: str) -> dict:
    """Detalle del envío. El JSON nuevo de /orders ya no trae datos de shipping,
    solo el id, así que el tipo logístico y el estado real salen de acá."""
    return _request(
        "GET", f"/shipments/{shipment_id}", access_token=access_token, extra_headers=_SHIPMENT_HEADERS
    )


def get_shipment_costs(shipment_id: str, access_token: str) -> dict:
    """Costos del envío: cuánto paga el comprador y cuánto el vendedor."""
    return _request(
        "GET", f"/shipments/{shipment_id}/costs", access_token=access_token, extra_headers=_SHIPMENT_HEADERS
    )


def extract_logistic_type(shipment: dict) -> str:
    """Tipo logístico de un shipment, tolerando los dos formatos de JSON.

    Formato nuevo (x-format-new): {"logistic": {"mode": "me2", "type": "self_service"}}
    Formato viejo:                {"logistic_type": "self_service"}
    """
    logistic = (shipment or {}).get("logistic") or {}
    if isinstance(logistic, dict) and logistic.get("type"):
        return str(logistic["type"])
    return str((shipment or {}).get("logistic_type") or "")


def seller_shipping_cost(costs: dict, seller_id: str | int | None) -> Decimal:
    """Costo de envío que efectivamente paga el vendedor.

    Sale de `senders[].cost` (el neto después de descuentos), no de
    `gross_amount`, que es el bruto del envío sin ningún descuento aplicado.
    `senders` es una lista pensada para carritos multi-vendedor: se filtra por
    user_id cuando se conoce, y si no se toma el primero.
    """
    senders = (costs or {}).get("senders") or []
    if not senders:
        return Decimal("0.00")
    chosen = None
    if seller_id:
        wanted = str(seller_id)
        chosen = next((s for s in senders if str(s.get("user_id") or "") == wanted), None)
    if chosen is None:
        chosen = senders[0]
    try:
        return Decimal(str(chosen.get("cost", 0) or 0)).quantize(Decimal("0.01"))
    except Exception:
        return Decimal("0.00")


# Estados de /shipments mapeados al estado de entrega del ERP. Los tags
# delivered/not_delivered de la orden ya no se agregan automáticamente (ML lo
# documenta explícitamente), así que el shipment es la fuente confiable.
_SHIPMENT_STATUS_TO_DELIVERY = {
    "delivered": "DELIVERED",
    "shipped": "IN_TRANSIT",
    "ready_to_ship": "IN_TRANSIT",
    "handling": "NOT_DELIVERED",
    "pending": "NOT_DELIVERED",
    "not_delivered": "NOT_DELIVERED",
    "cancelled": "NOT_DELIVERED",
}


def delivery_status_from_shipment(shipment: dict) -> str | None:
    """Estado de entrega del ERP a partir del status del shipment, o None."""
    status = str((shipment or {}).get("status") or "").lower()
    return _SHIPMENT_STATUS_TO_DELIVERY.get(status)


def push_selling_address_stock(user_product_id: str, quantity: int, access_token: str) -> bool:
    """Fijar el stock Flex (selling_address) de un user_product.

    Es el camino obligado para publicaciones en convivencia Full/Flex: ahí un
    PUT /items available_quantity no separa el stock propio del stock en Full.
    El PUT exige el header x-version que devuelve el GET; si otro proceso tocó
    la entidad en el medio, ML responde 409 y hay que releer la versión.
    """
    for _ in range(2):
        try:
            _, headers = _request(
                "GET",
                f"/user-products/{user_product_id}/stock",
                access_token=access_token,
                with_headers=True,
            )
        except HTTPError as exc:
            logger.warning(
                "ML: no se pudo leer el stock del user_product %s — %s",
                user_product_id, error_detail(exc),
            )
            return False
        version = headers.get("x-version") or headers.get("X-Version")
        if not version:
            logger.warning(
                "ML: el user_product %s no devolvió x-version; no se puede fijar el stock propio",
                user_product_id,
            )
            return False
        try:
            _request(
                "PUT",
                f"/user-products/{user_product_id}/stock/type/selling_address",
                access_token=access_token,
                data={"quantity": max(0, int(quantity))},
                extra_headers={"x-version": str(version)},
            )
            return True
        except HTTPError as exc:
            if exc.code == 409:
                # Versión desactualizada: releer y reintentar una vez.
                continue
            logger.warning(
                "ML: rechazó el stock %s del user_product %s — %s",
                quantity, user_product_id, error_detail(exc),
            )
            return False
    logger.warning(
        "ML: conflicto de versión (409) repetido al fijar el stock del user_product %s",
        user_product_id,
    )
    return False


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
    comun_wh = Warehouse.objects.filter(type=Warehouse.WarehouseType.COMUN).first()
    total = matched = unmatched = updated_stock = 0
    # Reconciliación ERP -> ML, al final del barrido (ver reconcile_stock_to_ml).
    # El push inmediato después de cada venta no alcanza: el stock propio también
    # cambia con compras, ajustes manuales y transferencias, y un push puede
    # fallar (token vencido, error de red) sin que nada lo corrija.
    # Viene ENCENDIDA: es lo que mantiene la publicación al día sola, sin que
    # nadie apriete nada. Antes estaba apagada porque el push mandaba el total
    # del producto a cada publicación y pisaba cantidades buenas con malas; ahora
    # cada una recibe el stock de SU variedad y las que no tienen variedad
    # elegida no se tocan. Se apaga con ML_STOCK_RECONCILE=0.
    reconcile = reconcile_enabled()
    stock_pushed = 0
    # Cache user-product stock per user_product_id so publications sharing the
    # same Full stock (catalog + traditional) don't trigger duplicate API calls.
    fulfillment_cache: dict[str, dict] = {}
    # product_id -> {user_product_id|item_id: cantidad en Full}. Ver el comentario
    # en el cuerpo del loop: sirve para deduplicar el stock Full compartido.
    full_by_product: dict[int, dict[str, int]] = {}
    products_seen: dict[int, Product] = {}

    for item_id in item_ids:
        try:
            item = _call_with_refresh(connection, get_item, item_id, access_token=access_token)
        except HTTPError as exc:
            if exc.code == 401:
                return SyncResult(total, matched, unmatched, updated_stock, {"error": "unauthorized"})
            raise
        title = item.get("title", "") or ""
        status = item.get("status", "") or ""
        logistic_type = item_logistic_type(item)
        permalink = item.get("permalink", "") or ""
        # For Full items the /items available_quantity is unreliable (Full/Flex
        # coexistence); the user-products stock endpoint (meli_facility) is the
        # source of truth. Non-Full items keep using available_quantity directly.
        available, user_product_id, full_qty, flex_qty, own_location = resolve_stock_breakdown(
            connection, item, access_token, cache=fulfillment_cache
        )
        # La convivencia Full/Flex se confirma de dos formas y alcanza con una:
        # el tag `self_service_in` de /items, o una ubicación `selling_address`
        # en el stock del user_product. Cuentas reales tienen la ubicación sin
        # el tag, y mirando solo el tag esas publicaciones quedaban como Full
        # puro: el ERP no les empujaba stock nunca y nada decía por qué.
        has_flex = item_has_flex(item) or own_location
        existing = MercadoLibreItem.objects.filter(item_id=item_id).first()
        product = existing.product if existing else None
        matched_name = existing.matched_name if existing else ""
        ml_item, _created = MercadoLibreItem.objects.update_or_create(
            item_id=item_id,
            defaults={
                "title": title,
                "available_quantity": available,
                "full_quantity": full_qty,
                "flex_quantity": flex_qty,
                "status": status,
                "logistic_type": logistic_type,
                "has_flex": has_flex,
                "user_product_id": user_product_id,
                "permalink": permalink,
                "product": product,
                "matched_name": matched_name,
            },
        )
        total += 1
        if product:
            matched += 1
            # El depósito ML refleja SOLO el stock en Full (meli_facility), que
            # es el único que administra MercadoLibre. El stock de Flex/Colecta/
            # Correo vive en el depósito COMUN, que es la fuente de verdad y se
            # empuja hacia ML — reflejarlo acá también lo contaría dos veces.
            # Se acumula por producto y se aplica al final del barrido: un mismo
            # producto puede tener varias publicaciones, y las de catálogo
            # comparten user_product_id (y por lo tanto el mismo stock Full),
            # así que se deduplica por esa clave antes de sumar.
            if logistic_type == "fulfillment":
                key = user_product_id or item_id
                full_by_product.setdefault(product.id, {})[key] = int(available)
            else:
                full_by_product.setdefault(product.id, {})
            products_seen[product.id] = product
        else:
            unmatched += 1

    # Con el barrido truncado (ML_SYNC_MAX_ITEMS) puede faltar la publicación Full
    # de un producto del que sí se vio la de Flex, y reconciliar ahí pondría en
    # cero stock Full real. Solo se reconcilia sobre un barrido completo.
    if ml_wh and not truncated:
        for product_id, per_location in full_by_product.items():
            product = products_seen[product_id]
            desired_qty = Decimal(str(sum(per_location.values())))
            stock = Stock.objects.filter(product=product, warehouse=ml_wh).first()
            current_qty = stock.quantity if stock else Decimal("0.00")
            diff = desired_qty - current_qty
            if diff != 0:
                services.register_adjustment(
                    product=product,
                    warehouse=ml_wh,
                    quantity=diff,
                    user=user,
                    reference="Sync ML stock Full",
                    allow_negative=True,
                )
                updated_stock += 1

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

    # Recién ahora, con la copia local ya al día (incluidas las que se acaban de
    # cerrar), se corrige lo que ML tenga distinto del ERP. Solo sobre las
    # publicaciones que se vieron en esta pasada: con el barrido truncado el
    # resto tiene datos viejos y empujarlos sería a ciegas.
    stock_report: dict = {}
    if reconcile:
        stock_report = reconcile_stock_to_ml(
            MercadoLibreItem.objects.filter(item_id__in=item_ids), access_token
        )
        stock_pushed = stock_report.get("pushed", 0)

    connection.last_sync_at = timezone.now()
    connection.last_metrics = json.dumps({**metrics, **stock_report})
    connection.last_metrics_at = timezone.now()
    connection.save(update_fields=["last_sync_at", "last_metrics", "last_metrics_at"])

    if truncated:
        metrics = {**metrics, "truncated": True, "max_items": max_items}
    metrics = {**metrics, "stock_pushed": stock_pushed, **stock_report}
    return SyncResult(total, matched, unmatched, updated_stock, metrics)


def run_full_sync(connection: MercadoLibreConnection, user) -> dict:
    """Una pasada completa: ventas recientes + stock de todas las publicaciones.

    Es lo mismo que hace el botón "Sincronizar todo", en una sola función, para
    que el disparador automático y el botón no puedan divergir.
    """
    try:
        orders = sync_recent_orders(connection, user, days=7)
    except Exception:
        logger.exception("ML: falló el sync de ventas")
        orders = {}
    result = sync_items_and_stock(connection, user, ignore_env_limit=True)
    return {"orders": orders, "stock": result}


def _claim_sync_slot(connection: MercadoLibreConnection, interval_minutes: int):
    """Reservar la próxima sincronización, o None si no toca / la tomó otro.

    Devuelve el last_sync_at anterior (para poder restaurarlo si el sync falla).
    Con varios workers de gunicorn la reserva tiene que ser atómica: gana el que
    logra actualizar la fila con el valor que leyó, los demás ven 0 filas
    afectadas y no arrancan un segundo sync en paralelo.
    """
    previous = connection.last_sync_at
    if previous and previous > timezone.now() - timedelta(minutes=interval_minutes):
        return None
    claimed = MercadoLibreConnection.objects.filter(
        pk=connection.pk, last_sync_at=previous
    ).update(last_sync_at=timezone.now())
    return (previous,) if claimed else None


def _run_sync_in_background(connection_pk: int, user_pk: int, previous_sync_at) -> None:
    from django.contrib.auth import get_user_model
    from django.db import connections as db_connections

    try:
        connection = MercadoLibreConnection.objects.filter(pk=connection_pk).first()
        user = get_user_model().objects.filter(pk=user_pk).first()
        if not connection or not user:
            return
        run_full_sync(connection, user)
    except Exception:
        logger.exception("ML: falló el sync automático")
        # Devolver la marca anterior: si no, el panel mostraría "sincronizado
        # recién" después de una corrida que no sincronizó nada.
        MercadoLibreConnection.objects.filter(pk=connection_pk).update(
            last_sync_at=previous_sync_at
        )
    finally:
        # El hilo tiene su propia conexión a la base: sin esto queda abierta.
        db_connections.close_all()


# Momento (reloj monotónico) a partir del cual vale la pena volver a mirar si
# toca sincronizar. Evita una consulta por request sin depender de un caché.
_next_sync_check = 0.0
_sync_check_lock = threading.Lock()


def maybe_start_background_sync(user) -> bool:
    """Disparar el sync si pasó el intervalo. Devuelve True si arrancó uno.

    El loop del contenedor puede no estar corriendo (start command distinto,
    servicio dormido, contenedor reiniciado) y entonces el stock queda viejo
    hasta que alguien aprieta el botón. Con esto, usar el ERP alcanza para que
    se sincronice solo: la primera visita después del intervalo lo dispara en
    segundo plano, sin demorar la respuesta.
    """
    global _next_sync_check
    if not getattr(settings, "ML_AUTO_SYNC", True):
        return False
    now = time.monotonic()
    with _sync_check_lock:
        if now < _next_sync_check:
            return False
        # Aunque no toque sincronizar, no volver a consultar por un minuto.
        _next_sync_check = now + 60
    try:
        connection = _default_connection()
        if not connection or not connection.access_token:
            return False
        claim = _claim_sync_slot(connection, sync_interval_minutes())
        if claim is None:
            return False
        thread = threading.Thread(
            target=_run_sync_in_background,
            args=(connection.pk, user.pk, claim[0]),
            name="ml-auto-sync",
            daemon=True,
        )
        thread.start()
        return True
    except Exception:
        logger.exception("ML: no se pudo lanzar el sync automático")
        return False


def _default_connection() -> "MercadoLibreConnection | None":
    from django.contrib.auth import get_user_model

    User = get_user_model()
    return (
        MercadoLibreConnection.objects.filter(
            user__in=User.objects.filter(is_superuser=True).order_by("id")[:1]
        ).first()
        or MercadoLibreConnection.objects.first()
    )


def products_with_variants(product_ids) -> set[int]:
    """IDs (de los recibidos) que tienen al menos una variedad cargada."""
    return set(
        ProductVariant.objects.filter(product_id__in=list(product_ids))
        .values_list("product_id", flat=True)
        .distinct()
    )


def ml_item_publishable_stock(ml_item, comun_qty: int, variant_products: set[int] | None = None) -> int | None:
    """Unidades que corresponden a UNA publicación desde el depósito propio.

    Un producto con variedades reparte su stock entre ellas y el COMUN es solo
    la suma: publicar esa suma en cada publicación muestra unidades que para esa
    variedad no existen. Entonces:

      - publicación enlazada a una variedad → el stock de esa variedad;
      - producto con variedades y publicación sin enlazar → None (no se toca la
        publicación: no hay forma de saber cuántas unidades son suyas);
      - producto sin variedades → el COMUN de siempre.
    """
    if ml_item.variant_id:
        variant = ProductVariant.objects.filter(
            id=ml_item.variant_id, product_id=ml_item.product_id
        ).first()
        if variant:
            return max(0, int(variant.quantity))
        return None
    if variant_products is None:
        variant_products = products_with_variants([ml_item.product_id])
    if ml_item.product_id in variant_products:
        return None
    return comun_qty


# Acciones posibles sobre una publicación (ver stock_alignment_rows).
ALIGN_SKIP = "skip"
ALIGN_ITEM = "item"
ALIGN_SELLING_ADDRESS = "selling_address"


def stock_alignment_rows(ml_items, comun_wh=None) -> list[dict]:
    """Qué stock le corresponde a cada publicación y por qué.

    Devuelve una fila por publicación con el número del ERP, el que ML tiene
    publicado y la acción que corresponde. Separar la decisión del push permite
    mostrarla antes de tocar nada (dry-run) y explicar cada salteo, que es lo
    que faltaba cuando el stock "no se sincronizaba" sin decir por qué.
    """
    ml_items = list(ml_items)
    comun_wh = comun_wh or Warehouse.objects.filter(type=Warehouse.WarehouseType.COMUN).first()
    product_ids = {item.product_id for item in ml_items if item.product_id}
    variant_products = products_with_variants(product_ids)
    comun_by_product: dict[int, int] = {}
    if comun_wh:
        comun_by_product = {
            stock.product_id: max(0, int(stock.quantity))
            for stock in Stock.objects.filter(product_id__in=product_ids, warehouse=comun_wh)
        }

    rows: list[dict] = []
    seen_user_products: set[tuple[str, int | None]] = set()
    for item in ml_items:
        row = {
            "item": item,
            "item_id": item.item_id,
            "title": item.title,
            "ml_qty": item.flex_quantity,
            "erp_qty": None,
            "action": ALIGN_SKIP,
            "reason": "",
            "result": "",
            "error": "",
        }
        rows.append(row)
        if not comun_wh:
            row["reason"] = "no hay depósito COMUN configurado"
            continue
        if not item.product_id:
            row["reason"] = "sin producto matcheado"
            continue
        if item.status == "closed":
            row["reason"] = "publicación cerrada"
            continue
        qty = ml_item_publishable_stock(item, comun_by_product.get(item.product_id, 0), variant_products)
        if qty is None:
            row["reason"] = "falta elegir la variedad"
            continue
        row["erp_qty"] = qty
        is_full = item.logistic_type == "fulfillment"
        if is_full and item.has_flex and item.user_product_id:
            key = (item.user_product_id, item.variant_id)
            if key in seen_user_products:
                row["reason"] = "stock compartido con otra publicación, se empuja una sola vez"
                continue
            seen_user_products.add(key)
            row["action"] = ALIGN_SELLING_ADDRESS
        elif is_full:
            row["reason"] = "Full puro: el stock lo administra ML"
        else:
            row["action"] = ALIGN_ITEM
    return rows


def apply_stock_alignment(rows, access_token, only_diff: bool = False) -> tuple[int, int]:
    """Empujar a ML las filas accionables. Devuelve (empujadas, fallidas).

    Cada fila queda con `result`/`error` cargados: un fallo de una publicación
    no frena al resto, pero deja de ser invisible.
    """
    pushed = failed = 0
    for row in rows:
        if row["action"] == ALIGN_SKIP:
            continue
        item = row["item"]
        qty = row["erp_qty"]
        if only_diff and qty == row["ml_qty"]:
            row["action"] = ALIGN_SKIP
            row["reason"] = "ya coincide con lo publicado"
            continue
        try:
            if row["action"] == ALIGN_SELLING_ADDRESS:
                if not push_selling_address_stock(item.user_product_id, qty, access_token):
                    raise RuntimeError(
                        "MercadoLibre rechazó el stock del depósito propio (detalle en los logs)"
                    )
                MercadoLibreItem.objects.filter(
                    user_product_id=item.user_product_id, variant_id=item.variant_id
                ).update(flex_quantity=qty)
            else:
                push_item_stock_and_price(item.item_id, qty, None, access_token)
                MercadoLibreItem.objects.filter(item_id=item.item_id).update(
                    available_quantity=qty, flex_quantity=qty
                )
            row["result"] = "ok"
            pushed += 1
        except Exception as exc:
            row["error"] = error_detail(exc)
            failed += 1
            logger.warning(
                "ML: no se pudo publicar stock %s en %s — %s", qty, item.item_id, row["error"]
            )
    return pushed, failed


def reconcile_stock_to_ml(ml_items, access_token) -> dict:
    """Dejar publicado en ML el stock del ERP y contar qué pasó.

    Corre en cada sync automático: es lo que mantiene las publicaciones al día
    sin que nadie apriete nada. Solo toca las que difieren, así una cuenta ya
    alineada no gasta una sola llamada a la API.

    El resumen que devuelve se guarda con las métricas de la conexión para que
    el panel pueda mostrar, sin botones de por medio, cuántas se corrigieron y
    cuáles rechazó ML.
    """
    rows = stock_alignment_rows(ml_items)
    pushed, failed = apply_stock_alignment(rows, access_token, only_diff=True)
    report: dict = {
        "stock_pushed": pushed,
        "stock_failed": failed,
        "stock_unlinked_variant": sum(
            1 for row in rows if row["reason"] == "falta elegir la variedad"
        ),
    }
    errors = [
        {"item_id": row["item_id"], "title": row["title"][:60], "error": row["error"]}
        for row in rows
        if row["error"]
    ]
    if errors:
        report["stock_errors"] = errors[:20]
    return report


def push_comun_stock_to_ml(products, connection=None) -> int:
    """Empujar el stock del depósito propio a las publicaciones de esos productos.

    COMUN es la fuente de verdad para todo lo que no sea Full: da igual si la
    venta entró por MercadoLibre (Flex/Colecta/Correo) o por fuera, el stock
    publicado se recalcula desde el mismo número. Como se manda el valor
    absoluto y no un delta, repetir el push es inofensivo. En los productos con
    variedades el número sale de la variedad enlazada a cada publicación.

    Qué se hace con cada publicación lo decide stock_alignment_rows; acá solo se
    juntan las publicaciones de los productos recibidos.

    Devuelve la cantidad de publicaciones actualizadas.
    """
    connection = connection or _default_connection()
    if not connection or not connection.access_token:
        return 0
    access_token = get_valid_access_token(connection)
    if not access_token:
        return 0
    product_ids = {product.id for product in products}
    if not product_ids:
        return 0
    ml_items = list(MercadoLibreItem.objects.filter(product_id__in=product_ids))
    if not ml_items:
        return 0
    rows = stock_alignment_rows(ml_items)
    pushed, _failed = apply_stock_alignment(rows, access_token)
    return pushed


def _resolve_shipment_info(connection, order: dict, access_token: str) -> dict:
    """Datos de envío de una orden: tipo logístico, estado y costo del vendedor.

    El JSON nuevo de /orders solo trae `shipping.id`, así que todo lo demás sale
    de /shipments. El id puede venir nulo un rato (el envío tarda en crearse) o
    directamente no existir en ventas "a convenir": en ese caso se devuelve lo
    que se pueda y el llamador cae al logistic_type de la publicación.
    """
    info = {"shipment_id": "", "logistic_type": "", "delivery_status": None, "shipping_cost": None}
    shipping = order.get("shipping") or {}
    shipment_id = str(shipping.get("id") or "")
    if not shipment_id:
        return info
    info["shipment_id"] = shipment_id
    try:
        shipment = _call_with_refresh(connection, get_shipment, shipment_id, access_token=access_token)
    except Exception:
        return info
    info["logistic_type"] = extract_logistic_type(shipment)
    info["delivery_status"] = delivery_status_from_shipment(shipment)
    seller_id = (order.get("seller") or {}).get("id") or connection.ml_user_id
    try:
        costs = _call_with_refresh(connection, get_shipment_costs, shipment_id, access_token=access_token)
        info["shipping_cost"] = seller_shipping_cost(costs, seller_id)
    except Exception:
        pass
    return info


def _logistic_type_from_items(matched_items) -> str:
    """Tipo logístico deducido de las publicaciones, para cuando no hay shipment."""
    for _product, _qty, _price, _vat, _variant, item_id in matched_items:
        if not item_id:
            continue
        ml_item = MercadoLibreItem.objects.filter(item_id=item_id).first()
        if ml_item and ml_item.logistic_type:
            return ml_item.logistic_type
    return ""


def _apply_ml_stock_exit(sale: Sale, matched_items, user) -> None:
    """Descontar del depósito COMUN el stock de una venta ML no-Full.

    Idempotente: si la venta ya tiene movimientos de salida asociados no hace
    nada, así un re-sync de la misma orden (que ocurre en cada notificación de
    ML) nunca descuenta dos veces.

    Solo se descuentan ventas RECIENTES. sync_ml_orders re-sincroniza hasta 90
    días hacia atrás, y las ventas anteriores a esta funcionalidad ya están
    reflejadas en el stock: descontarlas ahora sería contarlas dos veces (fue
    exactamente lo que pasó en producción, con productos que quedaron en
    negativo). La ventana igual cubre el caso legítimo de una venta importada
    antes de que ML creara el envío, que se resuelve en horas.
    """
    if StockMovement.objects.filter(sale=sale, movement_type=StockMovement.MovementType.EXIT).exists():
        return
    if sale.created_at and sale.created_at < timezone.now() - timedelta(days=ML_STOCK_EXIT_MAX_AGE_DAYS):
        return
    comun_wh = Warehouse.objects.filter(type=Warehouse.WarehouseType.COMUN).first()
    if not comun_wh:
        return
    variant_products = []
    for product, quantity, unit_price, vat_percent, variant, _item_id in matched_items:
        components = (
            [(kc.component, quantity * kc.quantity) for kc in product.kit_components.select_related("component")]
            if product.is_kit
            else [(product, quantity)]
        )
        for target_product, qty in components:
            services.register_exit(
                product=target_product,
                warehouse=comun_wh,
                quantity=qty,
                user=user,
                reference=f"Venta ML {sale.ml_order_id or sale.id}",
                sale_price=unit_price,
                vat_percent=vat_percent,
                sale=sale,
                allow_negative=True,
            )
        # En un producto con variedades el COMUN es la suma de las variedades:
        # hay que bajar la variedad vendida, si no el próximo recálculo (o una
        # edición de variedades) revive las unidades ya vendidas — y la
        # publicación de esa variedad seguiría ofreciendo stock que no está.
        if variant and not product.is_kit:
            with transaction.atomic():
                locked = ProductVariant.objects.select_for_update().filter(id=variant.id, product=product).first()
                if locked:
                    locked.quantity = (locked.quantity - quantity).quantize(Decimal("0.01"))
                    locked.save(update_fields=["quantity"])
                    variant_products.append(product)
    for product in variant_products:
        # Deja el COMUN igual a la suma de variedades (ya descontadas): pisa el
        # descuento que hizo register_exit en vez de sumarse a él.
        services.sync_comun_from_variants(product)


def _revert_ml_stock_exit(sale: Sale, user) -> None:
    """Devolver al depósito COMUN el stock de una venta ML cancelada."""
    movements = list(
        StockMovement.objects.filter(sale=sale, movement_type=StockMovement.MovementType.EXIT).select_related(
            "product", "from_warehouse"
        )
    )
    if not movements:
        return
    for movement in movements:
        if not movement.from_warehouse:
            continue
        services.register_adjustment(
            product=movement.product,
            warehouse=movement.from_warehouse,
            quantity=movement.quantity,
            user=user,
            reference=f"Reversa venta ML cancelada {sale.ml_order_id or sale.id}",
            allow_negative=True,
        )
    # La salida bajó la variedad vendida (ver _apply_ml_stock_exit): la reversa
    # se la devuelve y recalcula el COMUN desde las variedades, que ya incluye
    # las unidades devueltas.
    reverted_variant_products = []
    for item in sale.items.select_related("variant", "product").filter(variant__isnull=False):
        if item.product.is_kit:
            continue
        with transaction.atomic():
            locked = (
                ProductVariant.objects.select_for_update()
                .filter(id=item.variant_id, product=item.product)
                .first()
            )
            if locked:
                locked.quantity = (locked.quantity + item.quantity).quantize(Decimal("0.01"))
                locked.save(update_fields=["quantity"])
                reverted_variant_products.append(item.product)
    for product in reverted_variant_products:
        services.sync_comun_from_variants(product)
    # Los movimientos de salida originales se dejan como están: el ajuste de
    # reversa ya deja el rastro completo en el historial. Además solo se llega
    # acá en la transición a cancelada (después la orden sale por
    # "ignored_status"), así que no hay riesgo de revertir dos veces.


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
        # La orden está cancelada/expirada en ML: si ya existe la venta importada,
        # se marca como cancelada (queda en el historial pero deja de contar en
        # ventas y ganancias). No se borra.
        existing = (
            Sale.objects.filter(ml_order_id=str(order_id)).first()
            or Sale.objects.filter(reference=f"ML ORDER {order_id}").first()
        )
        if existing and not existing.is_cancelled:
            existing.is_cancelled = True
            existing.cancelled_at = timezone.now()
            existing.save(update_fields=["is_cancelled", "cancelled_at"])
            # Si era una venta que había descontado del depósito propio
            # (Flex/Colecta/Correo), la mercadería nunca salió: se devuelve.
            if existing.is_seller_fulfilled_ml:
                _revert_ml_stock_exit(existing, user)
            return False, "cancelled_marked"
        return False, "ignored_status"

    order_date = _parse_ml_datetime(order.get("date_created"))

    order_tags = {str(t).lower() for t in (order.get("tags") or [])}
    fraud_risk = "fraud_risk_detected" in order_tags

    shipment_info = _resolve_shipment_info(connection, order, access_token)

    # El estado real de entrega sale del shipment. Los tags delivered /
    # not_delivered de la orden ya no se agregan solos, así que solo se usan
    # como respaldo cuando todavía no hay envío creado.
    if shipment_info["delivery_status"]:
        delivery_status = shipment_info["delivery_status"]
    elif "delivered" in order_tags:
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
        linked_variant = None
        if item_id:
            ml_item = MercadoLibreItem.objects.select_related("product", "variant").filter(item_id=item_id).first()
            if ml_item and ml_item.product:
                product = ml_item.product
                linked_variant = ml_item.variant
        if not product and item_id:
            try:
                item_detail = _call_with_refresh(connection, get_item, item_id, access_token=access_token)
            except HTTPError:
                item_detail = {}
            title = item_detail.get("title", "") or ""
            status = item_detail.get("status", "") or ""
            logistic_type = item_logistic_type(item_detail)
            permalink = item_detail.get("permalink", "") or ""
            available, user_product_id = resolve_authoritative_stock(connection, item_detail, access_token)
            MercadoLibreItem.objects.update_or_create(
                item_id=item_id,
                defaults={
                    "title": title,
                    "available_quantity": available,
                    "status": status,
                    "logistic_type": logistic_type,
                    "has_flex": item_has_flex(item_detail),
                    "user_product_id": user_product_id,
                    "permalink": permalink,
                },
            )
        if not product:
            continue
        vat_percent = product.vat_percent or Decimal("0.00")
        # Cuando cada variedad es una publicación aparte (lo habitual acá) la
        # orden no trae variation_attributes: la variedad es la enlazada a la
        # publicación. Las variaciones propias de ML, si las hay, mandan.
        variant = (
            _resolve_variant_for_order_item(product, order_item, item_id, access_token, connection=connection)
            or linked_variant
        )
        matched_items.append((product, quantity, unit_price, vat_percent, variant, item_id))

    if not matched_items:
        return False, "no_matches"

    # Si el envío todavía no existe (ML lo crea con demora) se cae al tipo
    # logístico de la publicación, que para Full/Flex puros es el mismo.
    logistic_type = shipment_info["logistic_type"] or _logistic_type_from_items(matched_items)
    seller_fulfilled = logistic_type in ML_SELLER_FULFILLED_TYPES
    shipping_cost = shipment_info["shipping_cost"]

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

    # fallback 1: payment-level data (marketplace_fee + taxes_amount). This is
    # authoritative for the WHOLE order (todas las unidades) e incluye los
    # impuestos realmente retenidos por ML, así que se prioriza sobre sale_fee.
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

    # fallback 2 (última opción, estimada): sale_fee en order_items es la comisión
    # POR UNIDAD, así que hay que multiplicarla por la cantidad de cada línea.
    # Antes se sumaba sin multiplicar y las ventas de N unidades quedaban con la
    # comisión (y el impuesto) de 1 sola unidad.
    if fee_total == Decimal("0.00"):
        for oi in order.get("order_items") or []:
            sf = Decimal(str(oi.get("sale_fee") or 0)).copy_abs()
            qty = Decimal(str(oi.get("quantity") or 1))
            fee_total += sf * qty
        if fee_total > Decimal("0.00"):
            # IIBB ≈ 3.5% of commission (standard ML Argentina rate)
            tax_total = (fee_total * Decimal("0.035")).quantize(Decimal("0.01"))

    if existing_sale:
        existing_sale.ml_commission_total = fee_total.quantize(Decimal("0.01"))
        existing_sale.ml_tax_total = tax_total.quantize(Decimal("0.01"))
        existing_sale.ml_order_id = str(order_id)
        existing_sale.delivery_status = delivery_status
        existing_sale.ml_fraud_risk = fraud_risk
        update_fields = [
            "ml_commission_total",
            "ml_tax_total",
            "ml_order_id",
            "delivery_status",
            "ml_fraud_risk",
        ]
        if logistic_type:
            existing_sale.ml_logistic_type = logistic_type
            update_fields.append("ml_logistic_type")
        if shipment_info["shipment_id"]:
            existing_sale.ml_shipment_id = shipment_info["shipment_id"]
            update_fields.append("ml_shipment_id")
        if shipping_cost is not None:
            existing_sale.shipping_cost = shipping_cost
            update_fields.append("shipping_cost")
        existing_sale.save(update_fields=update_fields)
        # Ventas que salen del depósito propio: puede ser una venta importada
        # antes de conocerse el tipo logístico, o un re-sync. _apply_ml_stock_exit
        # es idempotente, así que descuenta solo la primera vez.
        if seller_fulfilled and not existing_sale.is_cancelled:
            _apply_ml_stock_exit(existing_sale, matched_items, user)
        for product, quantity, unit_price, vat_percent, variant, _item_id in matched_items:
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
        ml_logistic_type=logistic_type,
        ml_shipment_id=shipment_info["shipment_id"],
        ml_commission_total=fee_total.quantize(Decimal("0.01")),
        ml_tax_total=tax_total.quantize(Decimal("0.01")),
        shipping_cost=shipping_cost if shipping_cost is not None else Decimal("0.00"),
        delivery_status=delivery_status,
        ml_fraud_risk=fraud_risk,
        user=user,
    )
    if order_date:
        Sale.objects.filter(pk=sale.pk).update(created_at=order_date)
    for product, quantity, unit_price, vat_percent, variant, _item_id in matched_items:
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

    # Full lo despacha MercadoLibre desde su depósito: el stock del ERP no se
    # toca y se refleja desde meli_facility en el sync de items. En cambio
    # Flex/Colecta/Places/Correo salen del depósito propio, así que se descuenta
    # de COMUN igual que una venta mostrador.
    if seller_fulfilled:
        _apply_ml_stock_exit(sale, matched_items, user)
        # Reflejar el COMUN ya descontado en las publicaciones. ML normalmente ya
        # bajó su propio available_quantity al concretarse la venta, así que esto
        # suele ser un no-op; sirve para corregir desvíos (ventas por fuera de
        # ML, ajustes manuales) sin esperar al sync completo de items.
        try:
            push_comun_stock_to_ml([p for p, *_ in matched_items], connection=connection)
        except Exception:
            pass

    return True, "ok"
