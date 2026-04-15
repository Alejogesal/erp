"""AI agent view — natural language queries over the ERP database."""
import json
import os
import re
from decimal import Decimal

from django.contrib.auth.decorators import login_required
from django.db import connection
from django.http import JsonResponse
from django.shortcuts import render
from django.views.decorators.http import require_http_methods

try:
    import openai as _openai
except ImportError:
    _openai = None


_SCHEMA = """
Tablas principales (PostgreSQL, Django ORM):

inventory_product
  id, sku, name, group (marca/categoría), avg_cost (SIN IVA), vat_percent,
  price_consumer, price_barber, price_distributor,
  margin_consumer, margin_barber, margin_distributor,
  ml_commission_percent, is_kit, updated_at
  → Costo real con IVA = avg_cost * (1 + vat_percent/100)

inventory_warehouse
  id, name, type  →  valores posibles: 'MERCADOLIBRE' o 'COMUN'

inventory_stock
  id, product_id, warehouse_id, quantity

inventory_sale
  id, customer_id (nullable), warehouse_id, audience (CONSUMER/BARBER/DISTRIBUTOR),
  total, discount_total, delivery_status (NOT_DELIVERED/IN_TRANSIT/DELIVERED),
  reference, ml_order_id, ml_commission_total, ml_tax_total,
  created_at, updated_at, user_id
  → Ventas ML tienen ml_order_id con valor y warehouse type='MERCADOLIBRE'

inventory_saleitem
  id, sale_id, product_id, variant_id (nullable), quantity,
  unit_price (precio antes del descuento del ítem),
  cost_unit (costo CON IVA al momento de la venta),
  discount_percent, final_unit_price,
  line_total (= quantity * final_unit_price), vat_percent
  → Ganancia por ítem = (final_unit_price - cost_unit) * quantity

inventory_purchase
  id, supplier_id (nullable), warehouse_id, total, discount_percent,
  shipping_cost, reference, created_at, updated_at, user_id

inventory_purchaseitem
  id, purchase_id, product_id, variant_id (nullable),
  quantity, unit_cost, discount_percent, vat_percent

inventory_customer
  id, name, email, audience (CONSUMER/BARBER/DISTRIBUTOR), updated_at

inventory_customerpayment
  id, customer_id, sale_id (nullable), amount,
  method (CASH/TRANSFER/CARD/MERCADOPAGO/OTHER),
  kind (PAYMENT / REFUND / CREDIT_NOTE), paid_at, notes, created_at

inventory_supplier
  id, name, phone, created_at, updated_at

inventory_supplierpayment
  id, supplier_id, purchase_id (nullable), amount,
  method (CASH/TRANSFER/CARD/MERCADOPAGO/OTHER),
  kind (PAYMENT=reduce deuda / ADJUSTMENT=aumenta deuda),
  paid_at, notes, created_at
  → Saldo proveedor = SUM(purchase.total) - SUM(kind=PAYMENT) + SUM(kind=ADJUSTMENT)

inventory_taxexpense
  id, description, amount, paid_at, created_at

inventory_productvariant
  id, product_id, name, quantity

inventory_stockmovement
  id, product_id, sale_id, purchase_id,
  movement_type (ENTRY/EXIT/TRANSFER/ADJUSTMENT),
  from_warehouse_id, to_warehouse_id, quantity, unit_cost,
  sale_price, sale_net, ml_commission_percent, retention_percent,
  profit, vat_percent, user_id, reference, created_at

auth_user
  id, username, first_name, last_name, email

FÓRMULAS EXACTAS (usá siempre estas):

COSTO POR ÍTEM:
  CASE WHEN si.cost_unit > 0 THEN si.cost_unit ELSE p.avg_cost * (1 + p.vat_percent/100) END

GANANCIA POR VENTA (la misma lógica que el dashboard):
  - Ventas COMUN: ganancia = si.line_total - si.quantity * costo_item
  - Ventas MERCADOLIBRE: ganancia = (s.total - s.ml_commission_total - s.ml_tax_total) * si.line_total / items_total_de_esa_venta - si.quantity * costo_item
  - Ganancia neta del período = ganancia_bruta - SUM(inventory_taxexpense.amount del mismo período)

SQL MODELO para ganancia de un período (reemplazá [INICIO] y [FIN] con fechas 'YYYY-MM-DD'):
WITH item_costs AS (
  SELECT si.id, si.sale_id, si.line_total, si.quantity,
         CASE WHEN si.cost_unit > 0 THEN si.cost_unit ELSE p.avg_cost * (1 + p.vat_percent/100) END AS cost,
         w.type AS wh_type,
         s.total AS sale_total, s.ml_commission_total, s.ml_tax_total,
         s.created_at
  FROM inventory_saleitem si
  JOIN inventory_sale s ON s.id = si.sale_id
  JOIN inventory_warehouse w ON w.id = s.warehouse_id
  JOIN inventory_product p ON p.id = si.product_id
  WHERE s.created_at AT TIME ZONE 'America/Argentina/Buenos_Aires' >= '[INICIO]'
    AND s.created_at AT TIME ZONE 'America/Argentina/Buenos_Aires' < '[FIN]'
),
sale_totals AS (
  SELECT sale_id, SUM(line_total) AS items_total FROM inventory_saleitem GROUP BY sale_id
)
SELECT ROUND(SUM(
  CASE WHEN ic.wh_type = 'MERCADOLIBRE'
    THEN (ic.sale_total - ic.ml_commission_total - ic.ml_tax_total)
         * ic.line_total / NULLIF(st.items_total, 0) - ic.quantity * ic.cost
    ELSE ic.line_total - ic.quantity * ic.cost
  END
)::numeric, 2) AS ganancia_bruta
FROM item_costs ic
JOIN sale_totals st ON st.sale_id = ic.sale_id;

SALDO PROVEEDOR: SUM(purchase.total) - SUM(payments donde kind='PAYMENT') + SUM(payments donde kind='ADJUSTMENT')
VENTAS TOTALES DEL PERÍODO: SUM(inventory_saleitem.line_total) filtrando por sale__created_at
"""

_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "execute_sql",
            "description": (
                "Ejecuta una consulta SQL SELECT de solo lectura contra la base de datos "
                "PostgreSQL del ERP. Usá esto para obtener cualquier dato que necesites. "
                "Siempre incluí LIMIT (máximo 500) salvo que hagas agregaciones. "
                "Devuelve columnas y filas."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Sentencia PostgreSQL SELECT válida. No INSERT/UPDATE/DELETE/DROP.",
                    }
                },
                "required": ["query"],
            },
        },
    }
]

_SYSTEM_PROMPT_TEMPLATE = """Sos un asistente de análisis del ERP de una distribuidora de productos de peluquería en Argentina.
Tenés acceso completo a la base de datos a través de la herramienta execute_sql.
Respondé siempre en español, de forma clara y concisa.
Cuando presentes números monetarios usá formato argentino: $1.234,56.
Hoy es {today} (zona horaria Argentina). Cuando el usuario diga "enero" sin año, significa enero de {year}. Cuando diga "este mes" significa {month_name} de {year}. Cuando diga "este año" significa {year}. NUNCA asumas un año distinto a {year} salvo que el usuario lo especifique explícitamente.
Para fechas en zona horaria argentina usá: created_at AT TIME ZONE 'America/Argentina/Buenos_Aires'.
Si la consulta requiere varias queries, ejecutalas todas antes de responder.
Presentá los resultados en tablas markdown cuando tenga sentido.

REGLA CRÍTICA — NUNCA INVENTES DATOS:
- Siempre ejecutá execute_sql antes de dar cualquier número o cifra.
- Si la query devuelve 0 filas o NULL, decí exactamente eso: "No encontré datos para ese período."
- Si no estás seguro de cómo calcular algo, preguntale al usuario en lugar de asumir.
- JAMÁS respondas con un número que no hayas obtenido directamente de una query ejecutada en esta conversación.
- Si ejecutaste una query y el resultado está vacío, NO repitas la consulta con otros criterios sin avisarle al usuario.

""" + _SCHEMA


def _build_system_prompt() -> str:
    from django.utils import timezone as _tz
    import locale as _locale
    now = _tz.localtime(_tz.now())
    month_names = [
        "", "enero", "febrero", "marzo", "abril", "mayo", "junio",
        "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"
    ]
    return _SYSTEM_PROMPT_TEMPLATE.format(
        today=now.strftime("%d/%m/%Y"),
        year=now.year,
        month_name=month_names[now.month],
    )

_FORBIDDEN_RE = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|TRUNCATE|ALTER|CREATE|GRANT|REVOKE|EXECUTE|CALL|COPY)\b",
    re.IGNORECASE,
)


def _run_sql(query: str) -> dict:
    clean = query.strip().lstrip(";").strip()
    first_word = clean.split()[0].upper() if clean else ""
    if first_word not in ("SELECT", "WITH", "EXPLAIN"):
        return {"error": "Solo se permiten consultas SELECT o WITH."}
    if _FORBIDDEN_RE.search(clean):
        return {"error": "La consulta contiene operaciones no permitidas."}
    try:
        with connection.cursor() as cursor:
            cursor.execute(clean)
            columns = [col[0] for col in cursor.description] if cursor.description else []
            rows = cursor.fetchall()
            serializable = []
            for row in rows:
                serializable.append([
                    float(v) if isinstance(v, Decimal) else
                    v.isoformat() if hasattr(v, "isoformat") else
                    v
                    for v in row
                ])
            return {"columns": columns, "rows": serializable, "count": len(rows)}
    except Exception as exc:
        return {"error": str(exc)}


@login_required
@require_http_methods(["GET", "POST"])
def agent_view(request):
    if request.method == "GET":
        return render(request, "inventory/agent.html")

    try:
        body = json.loads(request.body)
    except (json.JSONDecodeError, ValueError):
        return JsonResponse({"error": "JSON inválido"}, status=400)

    messages_history = body.get("messages", [])
    if not messages_history:
        return JsonResponse({"error": "Sin mensajes"}, status=400)

    if _openai is None:
        return JsonResponse({"reply": "El paquete openai no está instalado en el servidor."})

    api_key = os.environ.get("OPENAI_API_KEY", "")
    if not api_key:
        return JsonResponse({"reply": "La variable OPENAI_API_KEY no está configurada."})

    client = _openai.OpenAI(api_key=api_key)
    openai_messages = [{"role": "system", "content": _build_system_prompt()}] + messages_history

    for _ in range(10):
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=openai_messages,
            tools=_TOOLS,
            tool_choice="auto",
        )
        msg = response.choices[0].message

        if msg.tool_calls:
            openai_messages.append(msg)
            for tool_call in msg.tool_calls:
                if tool_call.function.name == "execute_sql":
                    args = json.loads(tool_call.function.arguments)
                    result = _run_sql(args.get("query", ""))
                    openai_messages.append({
                        "role": "tool",
                        "tool_call_id": tool_call.id,
                        "content": json.dumps(result, ensure_ascii=False),
                    })
        else:
            return JsonResponse({"reply": msg.content or ""})

    return JsonResponse({"reply": "No pude completar la consulta en el número máximo de pasos. Intentá reformular."})
