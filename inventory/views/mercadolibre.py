"""MercadoLibre views."""
import json
import secrets
from datetime import timedelta

from django.conf import settings
from django.contrib import messages
from django.contrib.auth import get_user_model
from django.core.paginator import Paginator
from django.db.utils import OperationalError
from django.http import HttpResponse
from django.shortcuts import redirect, render
from django.utils import timezone
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from urllib.error import HTTPError

from .. import mercadolibre as ml
from .. import services
from ..models import (
    MercadoLibreConnection,
    MercadoLibreItem,
    MercadoLibreNotification,
    Product,
    Stock,
    Warehouse,
)
from django.contrib.auth.decorators import login_required


@login_required
@require_http_methods(["GET"])
def mercadolibre_connect(request):
    if not settings.ML_CLIENT_ID or not settings.ML_CLIENT_SECRET or not settings.ML_REDIRECT_URI:
        messages.error(request, "Faltan credenciales de MercadoLibre en las variables de entorno.")
        return redirect("inventory_mercadolibre_dashboard")
    state = secrets.token_urlsafe(16)
    request.session["ml_state"] = state
    return redirect(ml.get_authorize_url(state))


@login_required
@require_http_methods(["GET"])
def mercadolibre_callback(request):
    code = request.GET.get("code")
    state = request.GET.get("state")
    error = request.GET.get("error")
    error_description = request.GET.get("error_description")
    if error:
        return render(
            request,
            "inventory/mercadolibre_callback.html",
            {
                "code": code,
                "state": state,
                "error": error,
                "error_description": error_description,
            },
        )
    if not code:
        messages.error(request, "No se recibió el código de autorización.")
        return redirect("inventory_mercadolibre_dashboard")
    expected_state = request.session.pop("ml_state", None)
    if expected_state and state != expected_state:
        messages.error(request, "State inválido en el callback de MercadoLibre.")
        return redirect("inventory_mercadolibre_dashboard")
    token_data = ml.exchange_code_for_token(code)
    if token_data.get("error"):
        messages.error(
            request,
            f"No se pudo conectar con MercadoLibre. {token_data.get('error_description', token_data.get('error'))}",
        )
        return redirect("inventory_mercadolibre_dashboard")
    access_token = token_data.get("access_token")
    refresh_token = token_data.get("refresh_token", "")
    expires_in = int(token_data.get("expires_in", 0) or 0)
    if not access_token:
        messages.error(request, "No se pudo completar la conexión con MercadoLibre.")
        return redirect("inventory_mercadolibre_dashboard")

    connection, _ = MercadoLibreConnection.objects.get_or_create(user=request.user)
    connection.access_token = access_token
    connection.refresh_token = refresh_token
    connection.expires_at = timezone.now() + timedelta(seconds=expires_in) if expires_in else None
    profile = ml.get_user_profile(access_token)
    connection.ml_user_id = str(profile.get("id", "") or "")
    connection.nickname = profile.get("nickname", "") or ""
    connection.save(update_fields=["access_token", "refresh_token", "expires_at", "ml_user_id", "nickname"])

    messages.success(request, "MercadoLibre conectado correctamente.")
    return redirect("inventory_mercadolibre_dashboard")


@login_required
@require_http_methods(["GET", "POST"])
def mercadolibre_dashboard(request):
    missing_credentials = not settings.ML_CLIENT_ID or not settings.ML_CLIENT_SECRET or not settings.ML_REDIRECT_URI
    try:
        connection = MercadoLibreConnection.objects.filter(user=request.user).first()
        items_qs = MercadoLibreItem.objects.select_related("product")
    except OperationalError:
        messages.error(request, "Faltan tablas de MercadoLibre. Ejecutá migrate y recargá.")
        connection = None
        items_qs = MercadoLibreItem.objects.none()
    metrics = {}
    if connection and connection.last_metrics:
        try:
            metrics = json.loads(connection.last_metrics)
        except json.JSONDecodeError:
            metrics = {}
    search_query = (request.GET.get("q") or "").strip()
    if search_query:
        items_qs = items_qs.filter(title__icontains=search_query)
    items_qs = items_qs.order_by("-available_quantity", "title")
    paginator = Paginator(items_qs, 50)
    page_number = request.GET.get("page")
    page_obj = paginator.get_page(page_number)
    items = page_obj.object_list

    if request.method == "POST":
        action = request.POST.get("action")
        if action == "sync":
            if not connection or not connection.access_token:
                messages.error(request, "Primero conectá la cuenta de MercadoLibre.")
            else:
                result = ml.sync_items_and_stock(connection, request.user)
                metrics = result.metrics
                if metrics.get("error") == "unauthorized":
                    messages.error(
                        request,
                        "MercadoLibre rechazó el token. Volvé a conectar la cuenta para actualizar el acceso.",
                    )
                else:
                    notice = (
                        f"Sync OK. Items: {result.total_items}, Matcheados: {result.matched}, "
                        f"Sin match: {result.unmatched}, Stock actualizado: {result.updated_stock}."
                    )
                    if metrics.get("truncated"):
                        notice += " (Sync limitado por configuración)"
                    messages.success(request, notice)
        elif action == "sync_full":
            if not connection or not connection.access_token:
                messages.error(request, "Primero conectá la cuenta de MercadoLibre.")
            else:
                result = ml.sync_items_and_stock(connection, request.user, ignore_env_limit=True)
                metrics = result.metrics
                if metrics.get("error") == "unauthorized":
                    messages.error(
                        request,
                        "MercadoLibre rechazó el token. Volvé a conectar la cuenta para actualizar el acceso.",
                    )
                else:
                    notice = (
                        f"Sync completo OK. Items: {result.total_items}, Matcheados: {result.matched}, "
                        f"Sin match: {result.unmatched}, Stock actualizado: {result.updated_stock}."
                    )
                    messages.success(request, notice)
        elif action == "sync_orders":
            if not connection or not connection.access_token:
                messages.error(request, "Primero conectá la cuenta de MercadoLibre.")
            else:
                import os
                days_env = os.environ.get("ML_ORDERS_DAYS", "")
                days = int(days_env) if days_env.isdigit() else 30
                result = ml.sync_recent_orders(connection, request.user, days=days)
                if result.get("reasons", {}).get("unauthorized"):
                    messages.error(
                        request,
                        "MercadoLibre rechazó el token. Volvé a conectar la cuenta para actualizar el acceso.",
                    )
                else:
                    reason_parts = []
                    for key, count in result.get("reasons", {}).items():
                        reason_parts.append(f"{key}: {count}")
                    reason_text = f" ({', '.join(reason_parts)})" if reason_parts else ""
                    messages.success(
                        request,
                        "Sync ventas OK. Revisadas: "
                        f"{result['total']}, nuevas: {result['created']}, "
                        f"actualizadas: {result.get('updated', 0)}.{reason_text}",
                    )
        elif action == "sync_item":
            item_id = (request.POST.get("ml_item_id") or "").strip()
            if not item_id:
                messages.error(request, "Ingresá el ID de la publicación.")
            elif not connection or not connection.access_token:
                messages.error(request, "Primero conectá la cuenta de MercadoLibre.")
            else:
                if item_id.isdigit():
                    site_id = (getattr(settings, "ML_SITE_ID", "") or "MLA").upper()
                    item_id = f"{site_id}{item_id}"
                try:
                    access_token = ml.get_valid_access_token(connection)
                    if not access_token:
                        messages.error(
                            request,
                            "No hay token válido de MercadoLibre. Volvé a conectar la cuenta.",
                        )
                        return redirect("inventory_mercadolibre_dashboard")
                    item = ml._call_with_refresh(connection, ml.get_item, item_id, access_token=access_token)
                    title = item.get("title", "") or ""
                    available = int(item.get("available_quantity", 0) or 0)
                    status = item.get("status", "") or ""
                    shipping = item.get("shipping") or {}
                    logistic_type = item.get("logistic_type", "") or shipping.get("logistic_type", "") or ""
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
                            "logistic_type": logistic_type,
                            "permalink": permalink,
                            "product": product,
                            "matched_name": matched_name,
                        },
                    )
                    messages.success(request, "Publicación sincronizada.")
                except HTTPError as exc:
                    if exc.code == 401:
                        messages.error(
                            request,
                            "MercadoLibre rechazó el token. Volvé a conectar la cuenta para actualizar el acceso.",
                        )
                    else:
                        messages.error(request, f"No se pudo sincronizar: HTTP {exc.code}")
                except Exception as exc:
                    messages.error(request, f"No se pudo sincronizar: {exc}")
        elif action == "debug_order":
            order_id = (request.POST.get("debug_order_id") or "").strip()
            if not order_id or not connection or not connection.access_token:
                messages.error(request, "Ingresá un ID de orden.")
            else:
                try:
                    access_token = ml.get_valid_access_token(connection)
                    order = ml._call_with_refresh(connection, ml.get_order, order_id, access_token=access_token)
                    payments_in_order = order.get("payments") or []
                    try:
                        payments_api = ml._call_with_refresh(connection, ml.get_order_payments, order_id, access_token=access_token)
                    except Exception as e:
                        payments_api = {"error": str(e)}
                    debug_info = {
                        "order_status": order.get("status"),
                        "total_amount": order.get("total_amount"),
                        "fee_details": order.get("fee_details"),
                        "payments_in_order_count": len(payments_in_order),
                        "payments_in_order_sample": payments_in_order[:1],
                        "payments_api_raw": payments_api if isinstance(payments_api, list) else payments_api,
                        "payments_api_count": len(payments_api) if isinstance(payments_api, list) else "N/A",
                    }
                    messages.info(request, f"DEBUG ORDEN {order_id}: {json.dumps(debug_info, default=str)}")
                except Exception as exc:
                    messages.error(request, f"Error debug: {exc}")
        elif action == "link_item":
            item_id = request.POST.get("item_id")
            product_id = request.POST.get("product_id")
            ml_item = MercadoLibreItem.objects.filter(id=item_id).first()
            if not ml_item:
                messages.error(request, "No se encontró la publicación.")
            else:
                if product_id:
                    product = Product.objects.filter(id=product_id).first()
                    if not product:
                        messages.error(request, "Producto no encontrado.")
                    else:
                        ml_item.product = product
                        ml_item.matched_name = product.name
                        ml_item.save(update_fields=["product", "matched_name"])
                        messages.success(request, "Match actualizado.")
                else:
                    ml_item.product = None
                    ml_item.matched_name = ""
                    ml_item.save(update_fields=["product", "matched_name"])
                    messages.success(request, "Match eliminado.")

    products = Product.objects.order_by("name")
    recent_cutoff = timezone.now() - timedelta(days=30)
    return render(
        request,
        "inventory/mercadolibre_dashboard.html",
        {
            "connection": connection,
            "items": items,
            "metrics": metrics,
            "missing_credentials": missing_credentials,
            "page_obj": page_obj,
            "search_query": search_query,
            "recent_cutoff": recent_cutoff,
            "products": products,
        },
    )


@login_required
@require_http_methods(["GET"])
def mercadolibre_order_sheet(request):
    import math

    items_qs = (
        MercadoLibreItem.objects.select_related("product")
        .filter(product__isnull=False)
    )

    product_map = {}  # product_id -> dict
    for item in items_qs:
        product = item.product
        pid = product.id
        if pid not in product_map:
            product_map[pid] = {
                "name": product.name,
                "group": product.group or "",
                "stock": 0,
                "units_30d": 0,
            }
        product_map[pid]["stock"] += item.available_quantity
        product_map[pid]["units_30d"] += item.units_sold_30d

    rows = []
    for data in product_map.values():
        rec = math.ceil(data["units_30d"] / 2) if data["units_30d"] > 0 else 0
        rows.append({"name": data["name"], "group": data["group"], "stock": data["stock"], "recommendation": rec})

    rows.sort(key=lambda r: (r["group"].lower(), r["name"].lower()))

    return render(request, "inventory/mercadolibre_order_sheet.html", {"rows": rows})


@login_required
@require_http_methods(["GET", "POST"])
def ml_stock_push(request):
    from decimal import Decimal as _D

    comun_wh = Warehouse.objects.filter(type=Warehouse.WarehouseType.COMUN).first()

    # Build rows: one per unique product matched in ML
    product_ids_seen = set()
    product_rows = []
    for item in (
        MercadoLibreItem.objects.select_related("product")
        .filter(product__isnull=False)
        .order_by("product__group", "product__name")
    ):
        if item.product_id in product_ids_seen:
            continue
        product_ids_seen.add(item.product_id)
        stock_obj = Stock.objects.filter(product=item.product, warehouse=comun_wh).first() if comun_wh else None
        product_rows.append({
            "product": item.product,
            "comun_stock": stock_obj.quantity if stock_obj else _D("0"),
        })

    if request.method == "POST":
        updated = 0
        for row in product_rows:
            product = row["product"]
            raw = request.POST.get(f"qty_{product.id}", "").strip()
            if not raw:
                continue
            try:
                new_qty = _D(raw.replace(",", "."))
            except Exception:
                continue
            old_qty = row["comun_stock"]
            diff = new_qty - old_qty
            if diff == 0:
                continue
            if comun_wh:
                services.register_adjustment(
                    product=product,
                    warehouse=comun_wh,
                    quantity=diff,
                    user=request.user,
                    reference="Ajuste stock Comun",
                    allow_negative=True,
                )
            updated += 1
        if updated:
            messages.success(request, f"Stock actualizado para {updated} producto(s).")
        return redirect("inventory_ml_stock_push")

    return render(request, "inventory/ml_stock_push.html", {"rows": product_rows})


@csrf_exempt
@require_http_methods(["GET", "POST"])
def mercadolibre_webhook(request):
    if request.method == "GET":
        return HttpResponse("OK")
    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except json.JSONDecodeError:
        return HttpResponse("Invalid JSON", status=400)
    notification = MercadoLibreNotification.objects.create(
        topic=payload.get("topic", "") or "",
        resource=payload.get("resource", "") or "",
        ml_user_id=str(payload.get("user_id", "") or ""),
        application_id=str(payload.get("application_id", "") or ""),
        raw_payload=request.body.decode("utf-8"),
    )
    try:
        if notification.topic == "orders":
            resource = notification.resource or ""
            parts = resource.strip("/").split("/")
            order_id = ""
            if "orders" in parts:
                idx = parts.index("orders")
                if idx + 1 < len(parts):
                    order_id = parts[idx + 1]
            if order_id:
                connection = MercadoLibreConnection.objects.filter(ml_user_id=notification.ml_user_id).first()
                if connection:
                    User = get_user_model()
                    sync_user = (
                        User.objects.filter(is_superuser=True).order_by("id").first()
                        or User.objects.order_by("id").first()
                    )
                    if sync_user:
                        ml.sync_order(connection, order_id, sync_user)
    except Exception:
        pass
    return HttpResponse(f"OK:{notification.id}")
