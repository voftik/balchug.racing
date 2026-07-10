#!/usr/bin/env bash
# =============================================================================
#  Balchug Racing — деплой с локальной машины на сервер.
#
#  Запуск из корня проекта:  bash server/deploy.sh [--full]
#    (без флагов) — rsync зеркала, рендер веб-файлов (подстановка stream key),
#                   обновление nginx.conf (reload) и systemd-сервисов каталога.
#    --full      — дополнительно полная пересборка nginx (server/install.sh).
#
#  Stream key НЕ хранится в репозитории: локально лежит в файле `.stream_key`
#  (в .gitignore), на сервере — в /etc/balchug/stream.key. В web/index.html
#  вместо ключа плейсхолдер __STREAM_KEY__, который рендерится при деплое.
# =============================================================================
set -euo pipefail

HOST="${BALCHUG_HOST:-balchug}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
KEY_FILE="$ROOT/.stream_key"

[ -f "$KEY_FILE" ] || { echo "ОШИБКА: нет $KEY_FILE — создайте файл со stream key"; exit 1; }

echo "==> rsync → $HOST:/opt/balchug_racing/"
rsync -az --delete \
  --exclude '.git' --exclude '.omc' --exclude '.claude' --exclude '.playwright-cli' --exclude '.DS_Store' \
  --exclude '.stream_key' --exclude 'CREDENTIALS.md' \
  --exclude '__pycache__' --exclude 'venv' \
  "$ROOT"/ "$HOST":/opt/balchug_racing/

echo "==> stream key → $HOST:/etc/balchug/stream.key"
ssh "$HOST" 'mkdir -p /etc/balchug && umask 077 && cat > /etc/balchug/stream.key' < "$KEY_FILE"

if [ "${1:-}" = "--full" ]; then
  echo "==> полная установка (install.sh, пересборка nginx)"
  ssh "$HOST" 'bash /opt/balchug_racing/server/install.sh'
fi

echo "==> обновление web/конфига/сервисов (без пересборки nginx)"
ssh "$HOST" 'bash -s' <<'REMOTE'
set -euo pipefail
KEY="$(tr -d '[:space:]' < /etc/balchug/stream.key)"

# Keep every deployed Python service on the dependencies declared by this
# revision before migrations or systemd restarts load new modules.
test -x /opt/balchug_racing/venv/bin/pip
/opt/balchug_racing/venv/bin/pip install --disable-pip-version-check --quiet \
  -r /opt/balchug_racing/requirements.txt

# веб-файлы + рендер stream key в плеере
# cp не удаляет устаревшие каталоги: очищаем снятые с публикации разделы явно,
# не затрагивая HLS, stat.xsl и ACME-челленджи в web-root.
rm -rf /var/www/balchug/smp-live /var/www/balchug/smp-races
cp -rf /opt/balchug_racing/web/. /var/www/balchug/
sed -i "s/__STREAM_KEY__/${KEY}/g" /var/www/balchug/index.html
chown -R www-data:www-data /var/www/balchug

# конфиг nginx (reload без даунтайма)
cp -f /opt/balchug_racing/server/nginx.conf /etc/nginx/nginx.conf
nginx -t && systemctl reload nginx

# systemd-сервисы каталога
cp -f /opt/balchug_racing/server/systemd/*.service /etc/systemd/system/
systemctl daemon-reload

# Миграции live timing выполняются до перезапуска прикладных сервисов. Это
# отдельная БД, поэтому catalog.db и архивная часть остаются нетронутыми.
install -d -o www-data -g www-data -m 0750 /var/lib/balchug
runuser -u www-data -- env \
  PYTHONPATH=/opt/balchug_racing/server \
  TIMING_DB=/var/lib/balchug/timing.db \
  /opt/balchug_racing/venv/bin/python -m timing.migrate

# Локальный Wireproxy обязателен, когда аннотатор ходит к OpenRouter через него.
# Не запускаем merger до успешной проверки конфигурации туннеля.
LLM_PROXY="$(sed -n 's/^LLM_PROXY=//p' /etc/balchug/secrets.env | tail -n 1)"
LLM_PROXY="${LLM_PROXY#\"}"
LLM_PROXY="${LLM_PROXY%\"}"
LLM_PROXY="${LLM_PROXY#\'}"
LLM_PROXY="${LLM_PROXY%\'}"
LLM_PROXY="${LLM_PROXY%/}"
case "$LLM_PROXY" in
  http://127.0.0.1:25345|http://localhost:25345|http://\[::1\]:25345)
    test -x /usr/local/bin/wireproxy
    test -f /etc/wireproxy/wireproxy.conf
    /usr/local/bin/wireproxy -n -c /etc/wireproxy/wireproxy.conf
    systemctl enable --now wireproxy
    systemctl restart wireproxy
    systemctl is-active --quiet wireproxy
    proxy_ready=0
    for attempt in $(seq 1 20); do
      if curl -fsS --max-time 5 --proxy "$LLM_PROXY" https://openrouter.ai/api/v1/models -o /dev/null 2>/dev/null; then
        proxy_ready=1
        break
      fi
      sleep 1
    done
    if [ "$proxy_ready" -ne 1 ]; then
      echo "Wireproxy did not become ready for OpenRouter within 20 seconds" >&2
      exit 1
    fi
    ;;
esac

systemctl restart balchug-api balchug-transcode
if systemctl list-unit-files balchug-timing-api.service >/dev/null 2>&1; then
  systemctl enable --now balchug-timing-api
  systemctl restart balchug-timing-api
fi
if [ -f /etc/systemd/system/balchug-timing-ingest.service ]; then
  systemctl enable --now balchug-timing-ingest
  systemctl restart balchug-timing-ingest
fi
if systemctl list-unit-files balchug-merger.service >/dev/null 2>&1; then
  systemctl enable --now balchug-merger >/dev/null 2>&1 || true
  systemctl restart balchug-merger || true
fi
echo "deploy ok: $(date '+%F %T')"
REMOTE
