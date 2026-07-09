#!/usr/bin/env bash
# Reload nginx после успешного обновления сертификата Let's Encrypt.
# Устанавливается в /etc/letsencrypt/renewal-hooks/deploy/ (через install.sh).
set -euo pipefail
/usr/sbin/nginx -t && /usr/sbin/nginx -s reload
echo "[certbot-deploy-hook] nginx reloaded"
