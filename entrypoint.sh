#!/usr/bin/env sh
set -e

python manage.py migrate
python manage.py collectstatic --noinput

# Sync automático de MercadoLibre en background.
# Órdenes cada 5 min (descuenta ventas, es lo urgente). Stock completo cada 15
# min: ahora recorre TODAS las publicaciones, así que se espacia para no competir
# con la app. Ajustable con ML_STOCK_SYNC_EVERY (cada cuántos ciclos de 5 min).
(i=0; stock_every="${ML_STOCK_SYNC_EVERY:-3}"; while true; do
  python manage.py sync_ml_orders 2>&1 || true
  if [ "$((i % stock_every))" -eq 0 ]; then
    python manage.py sync_ml_stock 2>&1 || true
  fi
  i=$((i + 1))
  sleep 300
done) &

exec gunicorn erp.wsgi:application \
  --bind 0.0.0.0:8000 \
  --workers "${GUNICORN_WORKERS:-3}" \
  --threads "${GUNICORN_THREADS:-2}" \
  --timeout "${GUNICORN_TIMEOUT:-90}"
