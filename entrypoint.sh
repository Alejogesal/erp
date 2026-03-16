#!/usr/bin/env sh
set -e

python manage.py migrate
python manage.py collectstatic --noinput

# Sync automático de MercadoLibre en background (cada 5 minutos)
(while true; do
  python manage.py sync_ml_orders 2>&1 || true
  python manage.py sync_ml_stock  2>&1 || true
  sleep 300
done) &

exec gunicorn erp.wsgi:application --bind 0.0.0.0:8000
