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
    Sale,
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
    connection.last_sync_at = timezone.now()
    profile = ml.get_user_profile(access_token)
    connection.ml_user_id = str(profile.get("id", "") or "")
    connection.nickname = profile.get("nickname", "") or ""
    connection.save(update_fields=["access_token", "refresh_token", "expires_at", "last_sync_at", "ml_user_id", "nickname"])

    messages.success(request, "MercadoLibre conectado correctamente. Podés recuperar órdenes históricas desde Herramientas avanzadas.")
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
                from datetime import date as date_type
                date_str = (request.POST.get("sync_date") or "").strip()
                date_from_str = None
                date_to_str = None
                if date_str:
                    try:
                        d = date_type.fromisoformat(date_str)
                        date_from_str = f"{d.isoformat()}T00:00:00.000-03:00"
                        date_to_str = f"{d.isoformat()}T23:59:59.999-03:00"
                    except ValueError:
                        messages.error(request, "Fecha inválida. Usá el formato AAAA-MM-DD.")
                        date_str = None
                days_env = os.environ.get("ML_ORDERS_DAYS", "")
                days = int(days_env) if days_env.isdigit() else 30
                result = ml.sync_recent_orders(connection, request.user, days=days, date_from_str=date_from_str, date_to_str=date_to_str)
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
                    no_match_ids = result.get("no_match_ids", [])
                    no_match_text = ""
                    if no_match_ids:
                        no_match_text = f" — Sin match (vincular producto): {', '.join(no_match_ids)}"
                    from inventory.models import Sale as SaleModel
                    if date_str and date_from_str:
                        total_in_db = SaleModel.objects.filter(
                            reference__startswith="ML ORDER",
                            created_at__date=d,
                        ).count()
                        date_label = d.strftime("%d/%m/%Y")
                        messages.success(
                            request,
                            f"Sync ventas {date_label} OK. Revisadas: {result['total']}, "
                            f"nuevas: {result['created']}, actualizadas: {result.get('updated', 0)}.{reason_text} "
                            f"— Total en ERP ese día: {total_in_db}.{no_match_text}",
                        )
                    else:
                        messages.success(
                            request,
                            "Sync ventas OK. Revisadas: "
                            f"{result['total']}, nuevas: {result['created']}, "
                            f"actualizadas: {result.get('updated', 0)}.{reason_text}{no_match_text}",
                        )
        elif action == "push_price_ml":
            ml_item_id = (request.POST.get("ml_item_id") or "").strip()
            price_str = (request.POST.get("price") or "").strip()
            qty_str = (request.POST.get("quantity") or "").strip()
            if not ml_item_id:
                messages.error(request, "ID de publicación requerido.")
            elif not connection or not connection.access_token:
                messages.error(request, "No hay conexión ML.")
            else:
                from decimal import Decimal as _Dec
                try:
                    price = _Dec(price_str) if price_str else None
                    qty = int(qty_str) if qty_str else 0
                    access_token_push = ml.get_valid_access_token(connection)
                    ml._call_with_refresh(
                        connection, ml.push_item_stock_and_price,
                        ml_item_id, qty, price,
                        access_token=access_token_push,
                    )
                    messages.success(request, f"Publicación {ml_item_id} actualizada en ML.")
                except Exception as exc:
                    messages.error(request, f"Error al actualizar: {exc}")
        elif action == "recover_orders":
            if not connection or not connection.access_token:
                messages.error(request, "Primero conectá la cuenta de MercadoLibre.")
            else:
                from datetime import date as date_type, timedelta as td
                from_str = (request.POST.get("recover_from") or "").strip()
                to_str = (request.POST.get("recover_to") or "").strip()
                try:
                    d_from = date_type.fromisoformat(from_str)
                    d_to = date_type.fromisoformat(to_str)
                except ValueError:
                    messages.error(request, "Fechas inválidas.")
                    d_from = d_to = None
                if d_from and d_to and d_from <= d_to:
                    total_created = total_updated = total_reviewed = 0
                    current = d_from
                    while current <= d_to:
                        date_from_str = f"{current.isoformat()}T00:00:00.000-03:00"
                        date_to_str = f"{current.isoformat()}T23:59:59.999-03:00"
                        result = ml.sync_recent_orders(
                            connection, request.user, days=1,
                            date_from_str=date_from_str, date_to_str=date_to_str,
                        )
                        total_created += result.get("created", 0)
                        total_updated += result.get("updated", 0)
                        total_reviewed += result.get("total", 0)
                        current += td(days=1)
                    messages.success(
                        request,
                        f"Recuperación completada ({d_from} → {d_to}): "
                        f"{total_reviewed} revisadas, {total_created} nuevas, {total_updated} actualizadas."
                    )
                elif d_from and d_to:
                    messages.error(request, "La fecha de inicio debe ser anterior a la de fin.")
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
        elif action == "delete_ml_item":
            ml_item_db_id = request.POST.get("ml_item_db_id")
            deleted, _ = MercadoLibreItem.objects.filter(id=ml_item_db_id).delete()
            if deleted:
                messages.success(request, "Publicación eliminada del ERP.")
            else:
                messages.error(request, "No se encontró la publicación.")
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
        elif action == "delete_duplicate_sales":
            ids_to_delete = request.POST.getlist("delete_ids")
            if ids_to_delete:
                deleted_count, _ = Sale.objects.filter(pk__in=ids_to_delete).delete()
                messages.success(request, f"Se eliminaron {deleted_count} ventas duplicadas.")
            else:
                messages.warning(request, "No se seleccionó ninguna venta para eliminar.")
        elif action == "debug_item":
            item_id = (request.POST.get("debug_item_id") or "").strip()
            if not item_id or not connection:
                messages.error(request, "Ingresá el ID del item.")
            else:
                try:
                    import json as _json
                    access_token_dbg = ml.get_valid_access_token(connection)
                    item_data = ml._call_with_refresh(connection, ml.get_item, item_id, access_token=access_token_dbg)
                    inv_id = item_data.get("inventory_id", "") or ""
                    logistic = item_data.get("logistic_type", "") or (item_data.get("shipping") or {}).get("logistic_type", "")
                    avail = item_data.get("available_quantity", "?")
                    full_stock = None
                    if inv_id:
                        full_stock = ml.get_fulfillment_stock(inv_id, access_token_dbg)
                    messages.info(request,
                        f"Item {item_id} | logistic_type: {logistic!r} | "
                        f"inventory_id: {inv_id!r} | available_quantity: {avail} | "
                        f"fulfillment_stock: {full_stock}"
                    )
                except Exception as exc:
                    messages.error(request, f"Error debug item: {exc}")
        elif action == "debug_order":
            order_id = (request.POST.get("debug_order_id") or "").strip()
            if not order_id or not connection:
                messages.error(request, "Ingresá un ID de orden.")
            else:
                import json as _json
                try:
                    access_token = ml.get_valid_access_token(connection)
                    order = ml._call_with_refresh(connection, ml.get_order, order_id, access_token=access_token)
                    fee_details = order.get("fee_details") or []
                    payments = order.get("payments") or []
                    order_items = order.get("order_items") or []
                    item_fees = [(oi.get("item", {}).get("id"), oi.get("sale_fee"), oi.get("quantity")) for oi in order_items]
                    messages.info(request,
                        f"Orden {order_id} | status: {order.get('status')} | "
                        f"total_amount: {order.get('total_amount')} | "
                        f"fee_details: {_json.dumps(fee_details)} | "
                        f"payments_count: {len(payments)} | "
                        f"item_fees (id, sale_fee, qty): {item_fees}"
                    )
                except Exception as exc:
                    messages.error(request, f"Error debug: {exc}")
        elif action == "resync_commissions":
            if not connection:
                messages.error(request, "No hay conexión ML.")
            else:
                from decimal import Decimal as _Dec
                sales_to_fix = Sale.objects.filter(
                    ml_order_id__gt="",
                    ml_commission_total=_Dec("0.00"),
                )
                updated = 0
                skipped = 0
                for sale in sales_to_fix:
                    try:
                        ok, reason = ml.sync_order(connection, sale.ml_order_id, request.user)
                        if ok:
                            updated += 1
                        else:
                            skipped += 1
                    except Exception:
                        skipped += 1
                messages.success(request, f"Comisiones recalculadas: {updated} ventas actualizadas, {skipped} sin datos.")
        elif action == "recost_ml_sales":
            from decimal import Decimal as _Dec
            from ..models import SaleItem as _SaleItem
            items_to_fix = _SaleItem.objects.filter(
                sale__warehouse__type=Warehouse.WarehouseType.MERCADOLIBRE,
            ).select_related("product")
            fixed = 0
            for item in items_to_fix:
                new_cost = item.product.cost_with_vat()
                if not new_cost or new_cost <= _Dec("0.00"):
                    new_cost = item.product.last_purchase_cost()
                if new_cost and new_cost > _Dec("0.00"):
                    item.cost_unit = new_cost
                    item.save(update_fields=["cost_unit"])
                    fixed += 1
            messages.success(request, f"Costos actualizados: {fixed} items de ventas ML corregidos.")

    # Detect duplicate ML sales (same ml_order_id appearing more than once)
    from django.db.models import Count
    duplicate_order_ids = (
        Sale.objects.filter(ml_order_id__gt="")
        .values("ml_order_id")
        .annotate(cnt=Count("id"))
        .filter(cnt__gt=1)
        .values_list("ml_order_id", flat=True)
    )
    duplicate_sales = []
    if duplicate_order_ids:
        dupes_qs = (
            Sale.objects.filter(ml_order_id__in=duplicate_order_ids)
            .order_by("ml_order_id", "created_at")
        )
        current_id = None
        group = []
        for sale in dupes_qs:
            if sale.ml_order_id != current_id:
                if group:
                    duplicate_sales.append(group)
                group = [sale]
                current_id = sale.ml_order_id
            else:
                group.append(sale)
        if group:
            duplicate_sales.append(group)

    fraud_sales = Sale.objects.filter(ml_fraud_risk=True).order_by("-created_at")[:20]

    import math as _math

    def _stock_thresholds(units_sold_30d, manual_min=None):
        """
        Returns (min_stock, buffer) for semaphore calculation.
        min_stock = units_sold_30d / 4  (auto) or manual override
        buffer = extra units above min before turning green:
          >100 sold → buffer 20 | >50 → 10 | >10 → 3 | else → 0
        """
        if units_sold_30d and units_sold_30d > 0:
            auto_min = _math.ceil(units_sold_30d / 4)
            if units_sold_30d > 100:
                buffer = 20
            elif units_sold_30d > 50:
                buffer = 10
            elif units_sold_30d > 10:
                buffer = 3
            else:
                buffer = 0
        else:
            auto_min = None
            buffer = 0
        effective_min = manual_min if manual_min is not None else auto_min
        return effective_min, buffer

    def _semaphore(available, units_sold_30d, status, manual_min=None):
        if status == "closed":
            return "gray", None, 0
        effective_min, buffer = _stock_thresholds(units_sold_30d, manual_min)
        if effective_min is None:
            return "gray", None, 0
        if available <= effective_min:
            return "red", effective_min, buffer
        elif available < effective_min + buffer:
            return "yellow", effective_min, buffer
        else:
            return "green", effective_min, buffer

    # ML low stock alerts: red or yellow publications
    ml_low_stock = []
    for ml_item in MercadoLibreItem.objects.select_related("product").filter(
        status__in=["active", "paused"],
    ):
        sold = ml_item.units_sold_30d or 0
        manual_min = ml_item.product.min_stock if ml_item.product else None
        color, eff_min, buf = _semaphore(ml_item.available_quantity, sold, ml_item.status, manual_min)
        if color in ("red", "yellow") and eff_min is not None:
            ml_low_stock.append({
                "item_id": ml_item.item_id,
                "title": ml_item.title,
                "permalink": ml_item.permalink,
                "status": ml_item.status,
                "logistic_type": ml_item.logistic_type,
                "available": ml_item.available_quantity,
                "min": eff_min,
                "buffer": buf,
                "diff": max(0, eff_min + buf - ml_item.available_quantity),
                "color": color,
                "sold_30d": sold,
            })
    ml_low_stock.sort(key=lambda x: (0 if x["color"] == "red" else 1, x["available"]))

    # Annotate ML items with ERP stock and calculated price
    from decimal import Decimal as _Dec
    comun_wh_for_items = Warehouse.objects.filter(type=Warehouse.WarehouseType.COMUN).first()
    if comun_wh_for_items:
        product_ids = [item.product_id for item in items if item.product_id]
        erp_stocks = {
            s.product_id: s.quantity
            for s in Stock.objects.filter(product_id__in=product_ids, warehouse=comun_wh_for_items)
        }
    else:
        erp_stocks = {}
    for item in items:
        if item.product:
            item.erp_stock = erp_stocks.get(item.product_id, _Dec("0"))
            cost = item.product.avg_cost or _Dec("0")
            margin = item.product.margin_consumer or _Dec("0")
            item.erp_price = (cost * (1 + margin / 100)).quantize(_Dec("0.01"))
            item.min_stock = item.product.min_stock
        else:
            item.erp_stock = None
            item.erp_price = None
            item.min_stock = None
        # Semaphore based on sales velocity
        sold = item.units_sold_30d or 0
        manual_min = item.product.min_stock if item.product else None
        color, eff_min, buf = _semaphore(item.available_quantity, sold, item.status, manual_min)
        item.semaphore = color
        item.calc_min = eff_min
        item.calc_buffer = buf

    products = Product.objects.order_by("name")
    recent_cutoff = timezone.now() - timedelta(days=30)
    sync_age_minutes = None
    if connection and connection.last_sync_at:
        delta = timezone.now() - connection.last_sync_at
        sync_age_minutes = int(delta.total_seconds() / 60)

    # Seller reputation from ML API
    reputation = {}
    if connection and connection.access_token and connection.ml_user_id:
        try:
            access_token_rep = ml.get_valid_access_token(connection)
            if access_token_rep:
                reputation = ml.get_seller_reputation(connection.ml_user_id, access_token_rep)
        except Exception:
            pass

    # DB-based metrics (accurate, from local data)
    from decimal import Decimal as _Dec
    from django.db.models import Sum, Count
    now = timezone.now()
    thirty_days_ago = now - timedelta(days=30)
    this_month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    ml_wh = Warehouse.objects.filter(type=Warehouse.WarehouseType.MERCADOLIBRE).first()
    db_metrics = {}
    if ml_wh:
        def _agg(qs):
            r = qs.aggregate(count=Count("id"), revenue=Sum("total"), commission=Sum("ml_commission_total"), taxes=Sum("ml_tax_total"))
            return {
                "count": r["count"] or 0,
                "revenue": r["revenue"] or _Dec("0.00"),
                "commission": r["commission"] or _Dec("0.00"),
                "taxes": r["taxes"] or _Dec("0.00"),
            }
        base = Sale.objects.filter(warehouse=ml_wh)
        db_metrics["month"] = _agg(base.filter(created_at__gte=this_month_start))
        db_metrics["days30"] = _agg(base.filter(created_at__gte=thirty_days_ago))
        _meses = ["Enero","Febrero","Marzo","Abril","Mayo","Junio","Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"]
        db_metrics["month_label"] = f"{_meses[now.month - 1]} {now.year}"

    return render(
        request,
        "inventory/mercadolibre_dashboard.html",
        {
            "connection": connection,
            "items": items,
            "missing_credentials": missing_credentials,
            "page_obj": page_obj,
            "search_query": search_query,
            "recent_cutoff": recent_cutoff,
            "products": products,
            "duplicate_sales": duplicate_sales,
            "sync_age_minutes": sync_age_minutes,
            "db_metrics": db_metrics,
            "reputation": reputation,
            "fraud_sales": fraud_sales,
            "ml_low_stock": ml_low_stock,
        },
    )


@login_required
@require_http_methods(["GET", "POST"])
def mercadolibre_messages(request, order_id):
    connection = MercadoLibreConnection.objects.filter(user=request.user).first()
    if not connection or not connection.access_token:
        messages.error(request, "Cuenta de MercadoLibre no conectada.")
        return redirect("inventory_mercadolibre_dashboard")

    sale = Sale.objects.filter(ml_order_id=str(order_id)).first()
    conversation = {}
    send_error = None

    if request.method == "POST":
        text = (request.POST.get("text") or "").strip()
        if not text:
            send_error = "El mensaje no puede estar vacío."
        elif len(text) > 350:
            send_error = "Máximo 350 caracteres."
        else:
            try:
                access_token = ml.get_valid_access_token(connection)
                ml.send_order_message(str(order_id), connection.ml_user_id, text, access_token)
                messages.success(request, "Mensaje enviado.")
            except Exception as exc:
                send_error = f"No se pudo enviar: {exc}"

    try:
        access_token = ml.get_valid_access_token(connection)
        conversation = ml._call_with_refresh(
            connection, ml.get_order_messages,
            str(order_id), connection.ml_user_id,
            access_token=access_token,
        )
    except Exception as exc:
        messages.error(request, f"No se pudieron cargar los mensajes: {exc}")

    return render(request, "inventory/mercadolibre_messages.html", {
        "order_id": order_id,
        "sale": sale,
        "conversation": conversation,
        "send_error": send_error,
        "seller_id": connection.ml_user_id,
    })


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
        # Only count stock from active publications
        if item.status == "active":
            product_map[pid]["stock"] += item.available_quantity
        product_map[pid]["units_30d"] += item.units_sold_30d

    rows = []
    for data in product_map.values():
        rec = math.ceil(data["units_30d"] / 2) if data["units_30d"] > 0 else 0
        rows.append({"name": data["name"], "group": data["group"], "stock": data["stock"], "recommendation": rec})

    rows.sort(key=lambda r: (r["group"].lower(), r["name"].lower()))

    return render(request, "inventory/mercadolibre_order_sheet.html", {"rows": rows})


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
        if notification.topic in {"orders_v2", "orders"}:
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
