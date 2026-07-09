#!/usr/bin/env bash
# Периодическая переиндексация каталога (cron). Подхватывает новые/ручные
# загрузки в бакет и не трогает записи с ручными правками (edited=1).
set -euo pipefail
set -a; . /etc/balchug/secrets.env; set +a
cd /opt/balchug_racing/server/api
exec /opt/balchug_racing/venv/bin/python indexer.py
