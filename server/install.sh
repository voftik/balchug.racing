#!/usr/bin/env bash
# =============================================================================
#  Balchug Racing — установка/пересборка nginx + nginx-http-flv-module
#  Запуск на сервере: sudo bash /opt/balchug_racing/server/install.sh
#  Идемпотентен: можно запускать повторно (для обновления nginx/конфига).
# =============================================================================
set -euo pipefail

NGINX_VERSION="1.28.0"
FLV_MODULE_REF="master"
DOMAIN="${DOMAIN:-balchug.racing}"
ALT_DOMAIN="${ALT_DOMAIN:-www.balchug.racing}"
ACME_EMAIL="${ACME_EMAIL:-admin@balchug.racing}"   # email для Let's Encrypt (переопределяется env)

REPO_DIR="/opt/balchug_racing"          # зеркало проекта на сервере
WEBROOT="/var/www/balchug"
BUILD_DIR="/usr/local/src/nginx-build"

log() { echo -e "\n\033[1;36m==> $*\033[0m"; }

# ---------------------------------------------------------------------------
log "1/8  Системные пакеты"
export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y build-essential libpcre3-dev zlib1g-dev libssl-dev \
                   wget git curl ca-certificates certbot ffmpeg gpac rsync

# ---------------------------------------------------------------------------
log "2/8  Исходники nginx ${NGINX_VERSION} + nginx-http-flv-module (${FLV_MODULE_REF})"
mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"
[ -f "nginx-${NGINX_VERSION}.tar.gz" ] || \
    wget -q "https://nginx.org/download/nginx-${NGINX_VERSION}.tar.gz"
rm -rf "nginx-${NGINX_VERSION}"
tar -xzf "nginx-${NGINX_VERSION}.tar.gz"
if [ -d nginx-http-flv-module ]; then
    git -C nginx-http-flv-module fetch --depth 1 origin "$FLV_MODULE_REF" && \
    git -C nginx-http-flv-module reset --hard FETCH_HEAD
else
    git clone --depth 1 -b "$FLV_MODULE_REF" \
        https://github.com/winshining/nginx-http-flv-module.git
fi

# ---------------------------------------------------------------------------
log "3/8  Сборка nginx"
cd "nginx-${NGINX_VERSION}"
./configure \
    --prefix=/etc/nginx \
    --sbin-path=/usr/sbin/nginx \
    --modules-path=/usr/lib/nginx/modules \
    --conf-path=/etc/nginx/nginx.conf \
    --error-log-path=/var/log/nginx/error.log \
    --http-log-path=/var/log/nginx/access.log \
    --pid-path=/run/nginx.pid \
    --lock-path=/run/nginx.lock \
    --http-client-body-temp-path=/var/cache/nginx/client_temp \
    --http-proxy-temp-path=/var/cache/nginx/proxy_temp \
    --user=www-data --group=www-data \
    --with-http_ssl_module \
    --with-http_v2_module \
    --with-http_realip_module \
    --with-threads \
    --add-module="../nginx-http-flv-module"
make -j"$(nproc)"
make install

# ---------------------------------------------------------------------------
log "4/8  Каталоги, права, stat.xsl"
id -u www-data >/dev/null 2>&1 || useradd -r -s /usr/sbin/nologin www-data
mkdir -p /var/log/nginx /var/cache/nginx "$WEBROOT/hls"
# stat.xsl поставляется вместе с модулем
cp -f "$BUILD_DIR/nginx-http-flv-module/stat.xsl" "$WEBROOT/stat.xsl"
# Веб-файлы из зеркала проекта (плеер + страницы разделов + ассеты).
# Копируем всё дерево web/, не трогая сгенерированные hls/ и stat.xsl.
# cp не удаляет отсутствующие в исходниках каталоги.
rm -rf "$WEBROOT/smp-live" "$WEBROOT/smp-races"
cp -rf "$REPO_DIR"/web/. "$WEBROOT"/
# Рендер stream key в плеере (ключ хранится вне репозитория)
if [ -f /etc/balchug/stream.key ]; then
    KEY="$(tr -d '[:space:]' < /etc/balchug/stream.key)"
    sed -i "s/__STREAM_KEY__/${KEY}/g" "$WEBROOT/index.html"
fi
chown -R www-data:www-data "$WEBROOT" /var/cache/nginx /var/log/nginx

# ---------------------------------------------------------------------------
log "5/8  systemd unit"
cp -f "$REPO_DIR/server/nginx.service" /etc/systemd/system/nginx.service
systemctl daemon-reload
systemctl enable nginx

# ---------------------------------------------------------------------------
log "6/8  Сертификат Let's Encrypt (webroot, без простоя)"
# Bootstrap: временный HTTP-конфиг для прохождения ACME-челленджа
if [ ! -f "/etc/letsencrypt/live/${DOMAIN}/fullchain.pem" ]; then
    cat > /etc/nginx/nginx.conf <<BOOT
user www-data;
worker_processes auto;
pid /run/nginx.pid;
events { worker_connections 1024; }
http {
    server {
        listen 80;
        server_name ${DOMAIN} ${ALT_DOMAIN};
        location /.well-known/acme-challenge/ { root ${WEBROOT}; }
        location / { return 200 'bootstrap'; }
    }
}
BOOT
    systemctl restart nginx
    certbot certonly --webroot -w "$WEBROOT" \
        -d "$DOMAIN" -d "$ALT_DOMAIN" \
        --agree-tos -m "$ACME_EMAIL" -n
fi

# ---------------------------------------------------------------------------
log "7/8  Боевой конфиг nginx + deploy-hook для продления"
cp -f "$REPO_DIR/server/nginx.conf" /etc/nginx/nginx.conf
mkdir -p /etc/letsencrypt/renewal-hooks/deploy
cp -f "$REPO_DIR/server/certbot-deploy-hook.sh" \
      /etc/letsencrypt/renewal-hooks/deploy/reload-nginx.sh
chmod +x /etc/letsencrypt/renewal-hooks/deploy/reload-nginx.sh

# ---------------------------------------------------------------------------
log "8/8  Проверка конфига и запуск"
nginx -t
systemctl restart nginx
systemctl --no-pager status nginx | head -n 5

echo
log "Готово. nginx -V (модуль flv):"
nginx -V 2>&1 | tr ' ' '\n' | grep -i flv || true
echo
echo "RTMP ingest : rtmp://${DOMAIN}/live"
echo "Плеер       : https://${DOMAIN}/"
echo "Статистика  : https://${DOMAIN}/stat"
